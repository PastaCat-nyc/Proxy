// Simple web proxy. No dependencies: needs Node 18+ (built-in fetch).
// Usage: node main.js, then open http://localhost:3000
// Proxied URLs look like:  /p/https://example.com/some/page
//
// Optional environment variables:
//   PORT          port to listen on (default 3000)
//   CACHE_MB      in-memory cache size in megabytes (default 64, 0 disables)
//   TIMEOUT_MS    how long to wait for a target site (default 20000)

const http = require("http");
const fs = require("fs");
const path = require("path");
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

// ---------- Site pages (Proxy and Games tabs) ----------

const SITE_NAME = "QuickWire Proxy";

function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

const SITE_CSS = `
  :root{color-scheme:dark}
  *{box-sizing:border-box}
  body{margin:0;min-height:100vh;font-family:system-ui,sans-serif;background:#0f1115;color:#e8e8e8}
  a{color:inherit}
  nav{display:flex;gap:4px;align-items:center;padding:12px 16px;border-bottom:1px solid #22262f}
  nav .brand{font-weight:600;margin-right:12px}
  nav a.tab{text-decoration:none;padding:8px 14px;border-radius:8px;opacity:.7}
  nav a.tab:hover{background:#1a1d24;opacity:1}
  nav a.tab.active{background:#1f2430;opacity:1}
  main{padding:24px 16px}
  .center{min-height:calc(100vh - 140px);display:flex;align-items:center;justify-content:center}
  .box{width:min(640px,100%);text-align:center}
  h1{font-weight:600;margin:0 0 24px}
  form{display:flex;gap:8px}
  input{flex:1;min-width:0;padding:14px 16px;border-radius:10px;border:1px solid #333;background:#1a1d24;color:inherit;font-size:16px}
  button{padding:14px 20px;border:0;border-radius:10px;background:#4f7cff;color:#fff;font-size:16px;cursor:pointer}
  p.hint{opacity:.6;font-size:14px}
  .wrap{max-width:1100px;margin:0 auto}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:14px;margin-top:20px}
  .card{display:block;text-decoration:none;background:#171a21;border:1px solid #22262f;border-radius:12px;overflow:hidden;transition:transform .15s,border-color .15s}
  .card:hover{transform:translateY(-2px);border-color:#4f7cff}
  .thumb{aspect-ratio:4/3;background:#1f2430;display:flex;align-items:center;justify-content:center;font-size:42px;font-weight:700;color:#4f7cff}
  .thumb img{width:100%;height:100%;object-fit:cover}
  .card span{display:block;padding:10px 12px;font-size:15px}
  .empty{opacity:.7;line-height:1.6;background:#171a21;border:1px solid #22262f;border-radius:12px;padding:20px}
  code{background:#1f2430;padding:2px 6px;border-radius:6px}
`;

function renderPage(title, activeTab, body) {
  const tab = (href, label, key) =>
    `<a class="tab${activeTab === key ? " active" : ""}" href="${href}">${label}</a>`;
  return Buffer.from(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHtml(title)}</title><style>${SITE_CSS}</style></head>
<body>
<nav><span class="brand">${SITE_NAME}</span>${tab("/", "Proxy", "proxy")}${tab("/games", "Games", "games")}</nav>
${body}
</body></html>`);
}

const HOME_PAGE = renderPage(
  SITE_NAME,
  "proxy",
  `<main class="center"><div class="box">
  <h1>Web Proxy</h1>
  <form action="/go" method="get">
    <input name="url" placeholder="Enter a URL or search term" autofocus required>
    <button type="submit">Search</button>
  </form>
  <p class="hint">Example: example.com or https://en.wikipedia.org</p>
</div></main>`
);

// ---------- Games ----------
// Put games in the "games" folder next to this file. Two layouts work:
//   games/snake.html            a single HTML file (or a pasted embed/iframe snippet)
//   games/my-game/index.html    a folder with all of the game's files (most game ports)
// Optional thumbnails: games/snake.png, or games/my-game/thumb.png

const GAMES_DIR = path.join(__dirname, "games");
const THUMB_NAMES = ["thumb.png", "thumb.jpg", "thumb.webp", "cover.png", "cover.jpg", "icon.png"];

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".htm": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".txt": "text/plain; charset=utf-8",
  ".xml": "application/xml",
  ".wasm": "application/wasm",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
  ".wav": "audio/wav",
  ".m4a": "audio/mp4",
  ".mp4": "video/mp4",
  ".webm": "video/webm",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
  ".swf": "application/x-shockwave-flash",
};

function prettyName(id) {
  return id.replace(/[-_]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

async function exists(p) {
  try {
    await fs.promises.access(p);
    return true;
  } catch {
    return false;
  }
}

async function listGames() {
  let entries;
  try {
    entries = await fs.promises.readdir(GAMES_DIR, { withFileTypes: true });
  } catch {
    return [];
  }
  const games = [];
  for (const e of entries) {
    if (e.name.startsWith(".")) continue;
    if (e.isDirectory()) {
      const dir = path.join(GAMES_DIR, e.name);
      if (!(await exists(path.join(dir, "index.html")))) continue;
      let thumb = null;
      for (const t of THUMB_NAMES) if (!thumb && (await exists(path.join(dir, t)))) thumb = t;
      const base = `/games/${encodeURIComponent(e.name)}/`;
      games.push({ id: e.name, src: base, thumb: thumb && base + thumb });
    } else if (/\.html?$/i.test(e.name)) {
      const id = e.name.replace(/\.html?$/i, "");
      let thumb = null;
      for (const ext of [".png", ".jpg", ".webp"]) if (!thumb && (await exists(path.join(GAMES_DIR, id + ext)))) thumb = id + ext;
      games.push({
        id,
        src: `/games/${encodeURIComponent(e.name)}`,
        thumb: thumb && `/games/${encodeURIComponent(thumb)}`,
      });
    }
  }
  return games.sort((a, b) => a.id.localeCompare(b.id));
}

async function renderGamesPage() {
  const games = await listGames();
  const cards = games
    .map(
      (g) => `<a class="card" href="/play/${encodeURIComponent(g.id)}" data-name="${escapeHtml(prettyName(g.id).toLowerCase())}">
      <div class="thumb">${g.thumb ? `<img src="${escapeHtml(g.thumb)}" alt="" loading="lazy">` : escapeHtml(prettyName(g.id)[0] || "?")}</div>
      <span>${escapeHtml(prettyName(g.id))}</span></a>`
    )
    .join("");
  const body = games.length
    ? `<input id="q" placeholder="Search ${games.length} game${games.length === 1 ? "" : "s"}" autocomplete="off">
       <div class="grid" id="grid">${cards}</div>
       <script>document.getElementById("q").addEventListener("input",function(){var v=this.value.toLowerCase();
       document.querySelectorAll("#grid .card").forEach(function(c){c.style.display=c.dataset.name.indexOf(v)>-1?"":"none";});});</script>`
    : `<div class="empty">No games yet. Add an HTML file like <code>games/snake.html</code>, or a folder like
       <code>games/my-game/index.html</code>, next to main.js. Refresh this page and it will show up here.</div>`;
  return renderPage(`Games · ${SITE_NAME}`, "games", `<main><div class="wrap"><h1>Games</h1>${body}</div></main>`);
}

function renderPlayPage(game) {
  const name = escapeHtml(prettyName(game.id));
  return Buffer.from(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${name} · ${SITE_NAME}</title>
<style>
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;background:#000;color:#e8e8e8;font-family:system-ui,sans-serif}
  body{display:flex;flex-direction:column}
  header{display:flex;align-items:center;gap:12px;padding:8px 12px;background:#0f1115;border-bottom:1px solid #22262f}
  header a,header button{color:inherit;text-decoration:none;background:#1f2430;border:0;border-radius:8px;padding:8px 12px;font-size:14px;cursor:pointer}
  header strong{flex:1;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  iframe{flex:1;width:100%;border:0;background:#000}
</style></head>
<body>
<header><a href="/games">&larr; Games</a><strong>${name}</strong>
  <button onclick="location.reload()">Restart</button>
  <button onclick="var f=document.getElementById('game');(f.requestFullscreen||f.webkitRequestFullscreen).call(f)">Fullscreen</button></header>
<iframe id="game" src="${escapeHtml(game.src)}" allow="fullscreen; autoplay; gamepad; clipboard-write" allowfullscreen></iframe>
<script>document.getElementById("game").addEventListener("load",function(){try{this.contentWindow.focus();}catch(e){}});</script>
</body></html>`);
}

// Serve a file from the games folder (never anything outside it).
async function serveGameFile(req, res, url) {
  let rel;
  try {
    rel = decodeURIComponent(url.pathname.slice("/games/".length));
  } catch {
    return sendError(res, 400, "Bad file name.");
  }
  let file = path.resolve(GAMES_DIR, rel);
  if (!file.startsWith(GAMES_DIR + path.sep)) return sendError(res, 403, "Not allowed.");

  let stat;
  try {
    stat = await fs.promises.stat(file);
    if (stat.isDirectory()) {
      // Folders need a trailing slash so the game's relative file paths resolve.
      if (!url.pathname.endsWith("/")) return redirect(res, url.pathname + "/" + url.search, 301);
      file = path.join(file, "index.html");
      stat = await fs.promises.stat(file);
    }
  } catch {
    return sendError(res, 404, "Game file not found.");
  }

  // Answer "not modified" when the browser already has this version.
  const ims = Date.parse(req.headers["if-modified-since"] || "");
  if (ims && Math.floor(stat.mtimeMs / 1000) * 1000 <= ims) {
    res.writeHead(304);
    return res.end();
  }

  // Pre-compressed files (common in Unity WebGL builds), e.g. game.data.gz or game.wasm.br
  let ext = path.extname(file).toLowerCase();
  let preEncoding = null;
  if (ext === ".gz" || ext === ".br") {
    preEncoding = ext === ".gz" ? "gzip" : "br";
    ext = path.extname(file.slice(0, -3)).toLowerCase();
  }
  const type = MIME[ext] || "application/octet-stream";
  const headers = {
    "content-type": type,
    "cache-control": "public, max-age=300",
    "last-modified": stat.mtime.toUTCString(),
  };

  if (!preEncoding && stat.size <= CACHE_MAX_ENTRY) {
    // Small files: keep in memory and send compressed.
    const key = `file:${file}:${stat.mtimeMs}`;
    let entry = cache.get(key);
    if (!entry) {
      const body = await fs.promises.readFile(file);
      entry = cache.set(key, { status: 200, headers, body, type, expires: Infinity }) || { body, encoded: {} };
    }
    return sendBody(req, res, 200, { ...headers }, entry.body, type, entry.key ? entry : null);
  }

  // Large or pre-compressed files: stream from disk.
  if (preEncoding) headers["content-encoding"] = preEncoding;
  headers["content-length"] = stat.size;
  res.writeHead(200, headers);
  if (req.method === "HEAD") return res.end();
  pipeline(fs.createReadStream(file), res, () => {});
}

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

    if (url.pathname === "/games" || url.pathname === "/games/") {
      const page = await renderGamesPage();
      return sendBody(req, res, 200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" }, page, "text/html", null);
    }

    if (url.pathname.startsWith("/games/")) {
      return await serveGameFile(req, res, url);
    }

    if (url.pathname.startsWith("/play/")) {
      let id;
      try {
        id = decodeURIComponent(url.pathname.slice("/play/".length)).replace(/\/$/, "");
      } catch {
        return sendError(res, 400, "Bad game name.");
      }
      const game = (await listGames()).find((g) => g.id === id);
      if (!game) return sendError(res, 404, "That game isn't in the games folder.");
      return sendBody(req, res, 200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" }, renderPlayPage(game), "text/html", null);
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
