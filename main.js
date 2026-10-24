// Simple web proxy. No dependencies: needs Node 18+ (built-in fetch).
// Usage: node main.js, then open http://localhost:3000
// Proxied URLs look like:  /p/https://example.com/some/page
//
// Optional environment variables:
//   PORT          port to listen on (default 3000)
//   CACHE_MB      in-memory cache size in megabytes (default 64, 0 disables)
//   TIMEOUT_MS    how long to wait for a target site (default 20000)

const http = require("http");
const zlib = require("zlib");
const { Readable, pipeline } = require("stream");
const { promisify } = require("util");

const brotliAsync = promisify(zlib.brotliCompress);
const gzipAsync = promisify(zlib.gzip);

const PORT = process.env.PORT || 3000;
const PREFIX = "/p/";
const CLIENT_SCRIPT_PATH = "/__proxy.js";
const CLIENT_SCRIPT_URL = CLIENT_SCRIPT_PATH + "?v=2";

const UPSTREAM_TIMEOUT_MS = Number(process.env.TIMEOUT_MS) || 20000;
const MAX_REQUEST_BODY = 10 * 1024 * 1024; // 10 MB uploads
const CACHE_MAX_BYTES = (process.env.CACHE_MB !== undefined ? Number(process.env.CACHE_MB) : 64) * 1024 * 1024;
const CACHE_MAX_ENTRY = 5 * 1024 * 1024; // don't cache single files above 5 MB
const CACHE_MAX_TTL_S = 24 * 60 * 60; // never keep anything longer than a day
const COMPRESS_MIN_BYTES = 1024;

const COMPRESSIBLE = /^(text\/|application\/(javascript|x-javascript|json|ld\+json|manifest\+json|xml|xhtml\+xml|rss\+xml|atom\+xml|wasm)|image\/svg\+xml)/;
const STATIC_TYPE = /^(image\/|font\/|text\/css|text\/javascript|application\/(javascript|x-javascript|font|wasm))/;

// Response headers that would break the page when served from our origin.
const DROP_RESPONSE_HEADERS = new Set([
  "content-security-policy",
  "content-security-policy-report-only",
  "x-frame-options",
  "strict-transport-security",
  "content-encoding", // fetch already decompressed the body
  "content-length", // re-added below when still accurate
  "transfer-encoding",
  "connection",
  "keep-alive",
  "set-cookie", // handled separately
  "location", // handled separately
  "cross-origin-opener-policy",
  "cross-origin-embedder-policy",
  "cross-origin-resource-policy",
  "alt-svc",
]);

// Request headers worth passing through to the target site.
const FORWARD_REQUEST_HEADERS = [
  "accept",
  "accept-language",
  "content-type",
  "cookie",
  "range",
  "user-agent",
  "if-none-match",
  "if-modified-since",
];

// ---------- In-memory LRU cache ----------

class LruCache {
  constructor(maxBytes) {
    this.maxBytes = maxBytes;
    this.bytes = 0;
    this.map = new Map(); // insertion order = least recently used first
  }
  get(key) {
    const e = this.map.get(key);
    if (!e) return null;
    if (e.expires <= Date.now()) {
      this.delete(key);
      return null;
    }
    this.map.delete(key); // move to most recently used
    this.map.set(key, e);
    return e;
  }
  set(key, entry) {
    if (this.maxBytes <= 0 || entry.body.length > CACHE_MAX_ENTRY) return null;
    this.delete(key);
    entry.key = key;
    entry.size = entry.body.length;
    entry.encoded = {}; // compressed copies, filled in lazily
    this.map.set(key, entry);
    this.bytes += entry.size;
    this.trim();
    return entry;
  }
  addVariant(entry, encoding, buf) {
    if (this.map.get(entry.key) !== entry) return;
    entry.encoded[encoding] = buf;
    entry.size += buf.length;
    this.bytes += buf.length;
    this.trim();
  }
  delete(key) {
    const e = this.map.get(key);
    if (!e) return;
    this.map.delete(key);
    this.bytes -= e.size;
  }
  trim() {
    while (this.bytes > this.maxBytes && this.map.size) this.delete(this.map.keys().next().value);
  }
}

const cache = new LruCache(CACHE_MAX_BYTES);
const varyIndex = new Map(); // url -> request header names the response varies on

function cacheKey(url, req) {
  const names = varyIndex.get(url) || [];
  return url + names.map((n) => "\n" + (req.headers[n] || "")).join("");
}

function rememberVary(url, varyHeader) {
  if (varyIndex.size > 20000) varyIndex.clear();
  const names = (varyHeader || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s && s !== "accept-encoding");
  varyIndex.set(url, names);
}

// How long (ms) a response may be cached, or 0 if it shouldn't be.
function freshnessMs(headers, type) {
  const cc = (headers.get("cache-control") || "").toLowerCase();
  if (/no-store|no-cache|private/.test(cc)) return 0;
  let seconds;
  const m = cc.match(/s-maxage=(\d+)/) || cc.match(/max-age=(\d+)/);
  if (m) {
    seconds = Number(m[1]);
  } else if (headers.get("expires")) {
    seconds = (Date.parse(headers.get("expires")) - Date.now()) / 1000;
  } else if (headers.get("last-modified")) {
    seconds = (Date.now() - Date.parse(headers.get("last-modified"))) / 10000; // 10% heuristic
  } else {
    seconds = STATIC_TYPE.test(type) ? 300 : 0;
  }
  if (!Number.isFinite(seconds) || seconds <= 0) return 0;
  return Math.min(seconds, CACHE_MAX_TTL_S) * 1000;
}

// ---------- Compression ----------

function pickEncoding(req, type, size) {
  if (!COMPRESSIBLE.test(type) || size < COMPRESS_MIN_BYTES) return null;
  const ae = req.headers["accept-encoding"] || "";
  if (/\bbr\b/.test(ae)) return "br";
  if (/\bgzip\b/.test(ae)) return "gzip";
  return null;
}

function compress(encoding, buf, quality) {
  if (encoding === "br") {
    return brotliAsync(buf, {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: quality,
        [zlib.constants.BROTLI_PARAM_SIZE_HINT]: buf.length,
      },
    });
  }
  return gzipAsync(buf, { level: 6 });
}

function compressStream(encoding) {
  if (encoding === "br") {
    return zlib.createBrotliCompress({ params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 } });
  }
  return zlib.createGzip({ level: 6 });
}

function addVaryEncoding(vary) {
  if (!vary) return "Accept-Encoding";
  if (/accept-encoding/i.test(vary)) return vary;
  return vary + ", Accept-Encoding";
}

// Send a fully buffered body, compressing it when the browser supports it.
async function sendBody(req, res, status, headers, body, type, entry) {
  const encoding = pickEncoding(req, type, body.length);
  if (COMPRESSIBLE.test(type)) headers["vary"] = addVaryEncoding(headers["vary"]);
  if (encoding) {
    let out = entry && entry.encoded[encoding];
    if (!out) {
      // Cached files are compressed once, so they can afford a higher quality.
      out = await compress(encoding, body, entry ? 8 : 5);
      if (entry) cache.addVariant(entry, encoding, out);
    }
    headers["content-encoding"] = encoding;
    body = out;
  }
  headers["content-length"] = body.length;
  res.writeHead(status, headers);
  res.end(req.method === "HEAD" ? undefined : body);
}

// ---------- URL helpers ----------

function proxify(url, base) {
  if (!url) return url;
  const trimmed = url.trim();
  if (/^(data:|blob:|javascript:|mailto:|tel:|about:|#)/i.test(trimmed)) return url;
  if (trimmed.startsWith(PREFIX)) return url;
  try {
    const abs = new URL(trimmed, base);
    if (abs.protocol !== "http:" && abs.protocol !== "https:") return url;
    return PREFIX + abs.href;
  } catch {
    return url;
  }
}

function normalizeTarget(input) {
  let s = input.trim();
  if (!/^https?:\/\//i.test(s)) {
    // Looks like a domain? Otherwise treat it as a search.
    if (/^[^\s]+\.[^\s]+$/.test(s)) s = "https://" + s;
    else s = "https://duckduckgo.com/html/?q=" + encodeURIComponent(s);
  }
  return new URL(s).href;
}

function decodeEntities(s) {
  return s.replace(/&amp;/g, "&").replace(/&#x2F;/gi, "/").replace(/&#47;/g, "/");
}

function escapeAttr(s) {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

// Decode text using the page's real charset (not every site is UTF-8).
function decodeText(buf, contentType, sniffHtml) {
  let charset = (contentType.match(/charset\s*=\s*["']?([\w-]+)/i) || [])[1];
  if (!charset && sniffHtml) {
    const head = buf.subarray(0, 2048).toString("latin1");
    const m = head.match(/<meta[^>]+charset\s*=\s*["']?([\w-]+)/i);
    if (m) charset = m[1];
  }
  try {
    return new TextDecoder(charset || "utf-8").decode(buf);
  } catch {
    return new TextDecoder("utf-8").decode(buf);
  }
}

// ---------- Content rewriting ----------

const CSS_URL = /url\(\s*(?:"([^"]*)"|'([^']*)'|([^)]*?))\s*\)|@import\s+(?:"([^"]*)"|'([^']*)')/gi;

function rewriteCss(css, base) {
  if (!/url\(|@import/i.test(css)) return css; // nothing to do
  return css.replace(CSS_URL, (m, d, s, u, id, is) => {
    if (id !== undefined || is !== undefined) {
      const q = id !== undefined ? '"' : "'";
      return `@import ${q}${proxify(id ?? is, base)}${q}`;
    }
    const q = d !== undefined ? '"' : s !== undefined ? "'" : "";
    return `url(${q}${proxify(d ?? s ?? u, base)}${q})`;
  });
}

function rewriteSrcset(list, base) {
  if (/data:/i.test(list)) return list; // commas inside data URIs make splitting unsafe
  return list
    .split(",")
    .map((part) => {
      const [u, ...rest] = part.trim().split(/\s+/);
      return [proxify(u, base), ...rest].join(" ");
    })
    .join(", ");
}

const URL_ATTRS = new Set(["href", "src", "action", "poster", "data", "formaction", "background"]);
const ATTR = /([^\s"'>\/=]+)(?:(\s*=\s*)(?:"([^"]*)"|'([^']*)'|([^\s"'>]+)))?/g;

function rewriteAttrs(attrs, base) {
  if (!attrs) return attrs;
  return attrs.replace(ATTR, (m, name, eq, dq, sq, uq) => {
    if (eq === undefined) return m;
    const n = name.toLowerCase();
    const raw = dq ?? sq ?? uq ?? "";
    let value;
    if (URL_ATTRS.has(n)) value = proxify(decodeEntities(raw), base);
    else if (n === "srcset" || n === "imagesrcset") value = rewriteSrcset(decodeEntities(raw), base);
    else if (n === "style") value = rewriteCss(raw, base);
    else if (n === "integrity") return ""; // rewritten files would fail integrity checks
    else return m;
    const q = sq !== undefined ? "'" : '"';
    return `${name}${eq}${q}${value}${q}`;
  });
}

// One pass over the document: comments, <script>/<style> blocks, and every other tag.
const HTML_TOKEN =
  /<!--[\s\S]*?-->|<(script|style)\b((?:"[^"]*"|'[^']*'|[^'">])*)>([\s\S]*?)(<\/\1\s*>)|<([a-zA-Z][\w:-]*)((?:"[^"]*"|'[^']*'|[^'">])*)>/gi;

function rewriteHtml(html, pageUrl) {
  // Respect a <base href> tag if the page has one (the tag itself is removed below).
  let base = pageUrl;
  const baseMatch = html.match(/<base\b[^>]*\bhref\s*=\s*["']?([^"'\s>]+)/i);
  if (baseMatch) {
    try {
      base = new URL(decodeEntities(baseMatch[1]), pageUrl).href;
    } catch {}
  }

  const inject = `<script src="${CLIENT_SCRIPT_URL}" data-base="${escapeAttr(base)}"></script>`;
  let injected = false;

  const out = html.replace(HTML_TOKEN, (m, block, blockAttrs, content, close, tag, attrs) => {
    if (block) {
      const isStyle = block.toLowerCase() === "style";
      return `<${block}${rewriteAttrs(blockAttrs, base)}>${isStyle ? rewriteCss(content, base) : content}${close}`;
    }
    if (!tag) return m; // comment

    const t = tag.toLowerCase();
    if (t === "base") return "";
    if (t === "meta") {
      if (/http-equiv\s*=\s*["']?content-security-policy/i.test(attrs)) return "";
      if (/http-equiv\s*=\s*["']?refresh/i.test(attrs)) {
        attrs = attrs.replace(
          /(content\s*=\s*["']?\s*\d+\s*;\s*url\s*=\s*)([^"'>\s]+)/i,
          (mm, pre, u) => pre + proxify(decodeEntities(u), base)
        );
      }
      return `<${tag}${attrs}>`;
    }

    let result = `<${tag}${rewriteAttrs(attrs, base)}>`;
    if (t === "head" && !injected) {
      injected = true;
      result += inject;
    }
    return result;
  });

  if (injected) return out;
  // No <head>: insert after the doctype so the page doesn't drop into quirks mode.
  const doctype = out.match(/^\s*<!doctype[^>]*>/i);
  return doctype ? doctype[0] + inject + out.slice(doctype[0].length) : inject + out;
}

// Script loaded by every proxied page. It routes dynamic requests through the proxy.
// Served as one cacheable file instead of being pasted into every page.
const CLIENT_JS = `(function(){
  var s=document.currentScript;
  var BASE=(s&&s.getAttribute("data-base"))||location.href, P=${JSON.stringify(PREFIX)};
  function px(u){
    try{
      if(u==null)return u; u=String(u);
      if(/^(data:|blob:|javascript:|about:|#)/i.test(u))return u;
      if(u.indexOf(location.origin+P)===0||u.indexOf(P)===0)return u;
      var a=new URL(u,BASE);
      if(a.origin===location.origin){a=new URL(a.pathname+a.search+a.hash,BASE);}
      if(!/^https?:$/.test(a.protocol))return u;
      return P+a.href;
    }catch(e){return u;}
  }
  var of=window.fetch;
  if(of)window.fetch=function(i,o){
    if(typeof i==="string"||i instanceof URL)i=px(i);
    else if(i&&i.url)i=new Request(px(i.url),i);
    return of.call(this,i,o);
  };
  var oo=XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open=function(m,u){arguments[1]=px(u);return oo.apply(this,arguments);};
  var ow=window.open;
  window.open=function(u){if(u)arguments[0]=px(u);return ow.apply(this,arguments);};
  if(navigator.sendBeacon){var ob=navigator.sendBeacon.bind(navigator);navigator.sendBeacon=function(u,d){return ob(px(u),d);};}
  ["pushState","replaceState"].forEach(function(k){
    var o=history[k];
    history[k]=function(st,t,u){
      if(u!=null){var p=px(u);arguments[2]=p;if(p.indexOf(P)===0)BASE=p.slice(P.length);}
      return o.apply(this,arguments);
    };
  });
  document.addEventListener("submit",function(e){var f=e.target;var a=f.getAttribute("action")||BASE;if(a.indexOf(P)!==0)f.setAttribute("action",px(a));},true);
  document.addEventListener("click",function(e){var el=e.target.closest&&e.target.closest("a[href]");if(!el)return;var h=el.getAttribute("href");if(h&&h.indexOf(P)!==0&&!/^(#|javascript:)/i.test(h))el.setAttribute("href",px(h));},true);
})();`;
const CLIENT_JS_BUF = Buffer.from(CLIENT_JS);
const clientScriptEntry = { body: CLIENT_JS_BUF, encoded: {}, key: null };

// ---------- Home page ----------

const HOME_PAGE = Buffer.from(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Web Proxy</title>
<style>
  :root{color-scheme:light dark}
  body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
       font-family:system-ui,sans-serif;background:#0f1115;color:#e8e8e8}
  .box{width:min(640px,92vw);text-align:center}
  h1{font-weight:600;margin-bottom:24px}
  form{display:flex;gap:8px}
  input{flex:1;min-width:0;padding:14px 16px;border-radius:10px;border:1px solid #333;background:#1a1d24;color:inherit;font-size:16px}
  button{padding:14px 20px;border:0;border-radius:10px;background:#4f7cff;color:#fff;font-size:16px;cursor:pointer}
  p{opacity:.6;font-size:14px}
</style></head>
<body><div class="box">
  <h1>Web Proxy</h1>
  <form action="/go" method="get">
    <input name="url" placeholder="Enter a URL or search term" autofocus required>
    <button type="submit">Go</button>
  </form>
  <p>Example: example.com or https://en.wikipedia.org</p>
</div></body></html>`);

// ---------- Server ----------

function readBody(req) {
  return new Promise((resolve, reject) => {
    const declared = Number(req.headers["content-length"]);
    if (declared > MAX_REQUEST_BODY) {
      req.resume(); // discard the upload
      return reject(Object.assign(new Error("Upload too large (limit 10 MB)."), { status: 413 }));
    }
    let size = 0;
    let chunks = [];
    req.on("data", (c) => {
      if (!chunks) return;
      size += c.length;
      if (size > MAX_REQUEST_BODY) {
        chunks = null;
        reject(Object.assign(new Error("Upload too large (limit 10 MB)."), { status: 413 }));
      } else {
        chunks.push(c);
      }
    });
    req.on("end", () => chunks && resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function sendError(res, status, message) {
  if (res.headersSent || res.destroyed) return res.end();
  res.writeHead(status, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
  res.end(`<!doctype html><body style="font-family:system-ui;padding:40px">
    <h2>Proxy error</h2><p>${String(message).replace(/</g, "&lt;")}</p><p><a href="/">Back home</a></p></body>`);
}

function redirect(res, location, status = 302) {
  res.writeHead(status, { location });
  res.end();
}

async function handleProxy(req, res, target) {
  let targetUrl;
  try {
    targetUrl = new URL(target);
  } catch {
    return sendError(res, 400, "Invalid URL: " + target);
  }
  if (targetUrl.protocol !== "http:" && targetUrl.protocol !== "https:") {
    return sendError(res, 400, "Only http and https URLs are supported.");
  }

  // Serve from cache when possible.
  const canUseCache = (req.method === "GET" || req.method === "HEAD") && !req.headers["range"];
  if (canUseCache) {
    const entry = cache.get(cacheKey(targetUrl.href, req));
    if (entry) {
      const headers = { ...entry.headers, "x-proxy-cache": "HIT" };
      const inm = req.headers["if-none-match"];
      if (inm && entry.headers["etag"] && inm.includes(entry.headers["etag"])) {
        res.writeHead(304, { etag: entry.headers["etag"], "cache-control": entry.headers["cache-control"] || "" });
        return res.end();
      }
      return sendBody(req, res, entry.status, headers, entry.body, entry.type, entry);
    }
  }

  // Build outgoing headers.
  const headers = {};
  for (const name of FORWARD_REQUEST_HEADERS) {
    if (req.headers[name]) headers[name] = req.headers[name];
  }
  // Translate the referer back to the real site.
  const ref = req.headers["referer"];
  if (ref) {
    const idx = ref.indexOf(PREFIX);
    if (idx !== -1) headers["referer"] = ref.slice(idx + PREFIX.length);
  }

  // Abort the upstream request on timeout, or when the browser goes away.
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, UPSTREAM_TIMEOUT_MS);
  res.on("close", () => {
    clearTimeout(timer);
    if (!res.writableFinished) controller.abort();
  });

  const init = { method: req.method, headers, redirect: "manual", signal: controller.signal };
  if (!["GET", "HEAD"].includes(req.method)) {
    try {
      init.body = await readBody(req);
    } catch (err) {
      clearTimeout(timer);
      return sendError(res, err.status || 400, err.message);
    }
  }

  let upstream;
  try {
    upstream = await fetch(targetUrl.href, init);
  } catch (err) {
    clearTimeout(timer);
    if (timedOut) return sendError(res, 504, `${targetUrl.host} took too long to respond.`);
    return sendError(res, 502, `Could not reach ${targetUrl.host}: ${err.cause?.code || err.message}`);
  }

  // Copy safe response headers.
  const outHeaders = {};
  upstream.headers.forEach((value, name) => {
    if (!DROP_RESPONSE_HEADERS.has(name)) outHeaders[name] = value;
  });

  // Rewrite redirects so they stay inside the proxy.
  const location = upstream.headers.get("location");
  if (location) outHeaders["location"] = proxify(location, targetUrl.href);

  // Cookies: drop Domain and Path so the browser stores them for our host.
  const cookies = upstream.headers.getSetCookie?.() || [];
  if (cookies.length) {
    outHeaders["set-cookie"] = cookies.map(
      (c) => c.replace(/;\s*domain=[^;]*/gi, "").replace(/;\s*path=[^;]*/gi, "") + "; Path=/"
    );
  }

  const type = (upstream.headers.get("content-type") || "").toLowerCase();
  const finalUrl = upstream.url || targetUrl.href;
  const status = upstream.status;

  if (req.method === "HEAD" || status === 204 || status === 304 || !upstream.body) {
    clearTimeout(timer);
    res.writeHead(status, outHeaders);
    return res.end();
  }

  const isHtml = type.includes("text/html");
  const isCss = type.includes("text/css");
  const partial = status === 206 || upstream.headers.has("content-range");
  const lengthHeader = upstream.headers.get("content-length");
  const declaredLength = lengthHeader == null ? null : Number(lengthHeader);
  const vary = upstream.headers.get("vary") || "";

  // Many sites attach tracking cookies even to images, so static files are still cached
  // (the cookies are sent on this response but never stored in the cache).
  const ttl =
    req.method === "GET" &&
    !req.headers["range"] &&
    status === 200 &&
    !partial &&
    !isHtml &&
    (!cookies.length || STATIC_TYPE.test(type)) &&
    !vary.includes("*")
      ? freshnessMs(upstream.headers, type)
      : 0;
  const cacheable = ttl > 0 && CACHE_MAX_BYTES > 0;

  // HTML and CSS get rewritten. Small cacheable files get buffered so they can be stored.
  if (isHtml || isCss || (cacheable && declaredLength != null && declaredLength <= CACHE_MAX_ENTRY)) {
    let body;
    try {
      body = Buffer.from(await upstream.arrayBuffer());
    } catch {
      return sendError(res, timedOut ? 504 : 502, `Failed to download the page from ${targetUrl.host}.`);
    } finally {
      clearTimeout(timer);
    }

    if (isHtml) {
      body = Buffer.from(rewriteHtml(decodeText(body, type, true), finalUrl));
      outHeaders["content-type"] = "text/html; charset=utf-8";
    } else if (isCss) {
      body = Buffer.from(rewriteCss(decodeText(body, type, false), finalUrl));
      outHeaders["content-type"] = "text/css; charset=utf-8";
    }

    let entry = null;
    if (cacheable) {
      rememberVary(targetUrl.href, vary);
      const { "set-cookie": _ignored, ...storedHeaders } = outHeaders;
      entry = cache.set(cacheKey(targetUrl.href, req), {
        status,
        headers: storedHeaders,
        body,
        type,
        expires: Date.now() + ttl,
      });
    }
    outHeaders["x-proxy-cache"] = entry ? "MISS" : "BYPASS";
    return sendBody(req, res, status, outHeaders, body, type, entry);
  }

  // Everything else (large images, video, downloads...) streams straight through.
  clearTimeout(timer);
  const encoding = partial ? null : pickEncoding(req, type, declaredLength ?? Infinity);
  if (COMPRESSIBLE.test(type)) outHeaders["vary"] = addVaryEncoding(outHeaders["vary"]);
  if (encoding) {
    outHeaders["content-encoding"] = encoding;
  } else if (lengthHeader && !upstream.headers.get("content-encoding")) {
    // Length is still accurate: keeps download progress bars and video seeking working.
    outHeaders["content-length"] = lengthHeader;
  }
  res.writeHead(status, outHeaders);

  const source = Readable.fromWeb(upstream.body);
  const stages = encoding ? [source, compressStream(encoding), res] : [source, res];
  pipeline(...stages, () => {}); // errors here just mean the browser or site hung up
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://localhost");

    if (url.pathname === "/") {
      return sendBody(req, res, 200, { "content-type": "text/html; charset=utf-8" }, HOME_PAGE, "text/html", null);
    }

    if (url.pathname === CLIENT_SCRIPT_PATH) {
      return sendBody(
        req,
        res,
        200,
        { "content-type": "text/javascript; charset=utf-8", "cache-control": "public, max-age=86400" },
        CLIENT_JS_BUF,
        "text/javascript",
        clientScriptEntry
      );
    }

    if (url.pathname === "/go") {
      const q = url.searchParams.get("url") || "";
      if (!q) return redirect(res, "/");
      try {
        return redirect(res, PREFIX + normalizeTarget(q));
      } catch {
        return sendError(res, 400, "Invalid URL.");
      }
    }

    if (req.url.startsWith(PREFIX)) {
      let target = req.url.slice(PREFIX.length);
      // Some clients collapse "https://" into "https:/".
      target = target.replace(/^(https?):\/(?!\/)/i, "$1://");
      return await handleProxy(req, res, target);
    }

    // A root-relative request that escaped rewriting, e.g. "/api/data".
    // Use the referer to work out which site it belongs to.
    const ref = req.headers["referer"];
    if (ref && ref.includes(PREFIX)) {
      try {
        const origin = new URL(ref.slice(ref.indexOf(PREFIX) + PREFIX.length)).origin;
        const target = origin + req.url;
        if (req.method === "GET" || req.method === "HEAD") return redirect(res, PREFIX + target, 307);
        return await handleProxy(req, res, target);
      } catch {}
    }

    sendError(res, 404, "Not found. Go to the home page to enter a URL.");
  } catch (err) {
    console.error(err);
    sendError(res, 500, err.message);
  }
});

// Keep the server alive if a single request misbehaves.
process.on("unhandledRejection", (err) => console.error("Unhandled rejection:", err));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Proxy running on http://localhost:${PORT} (cache ${CACHE_MAX_BYTES / 1048576} MB)`);
});
