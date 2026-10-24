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
const CLIENT_SCRIPT_URL = CLIENT_SCRIPT_PATH + "?v=3";

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

// ---------- Accent colors (gold / silver / copper) ----------

const ACCENTS = {
  gold: { main: "#e2a93b", light: "#f7d98b", deep: "#a8772a", rgb: "226,169,59", grad: ["#f7d98b", "#e2a93b", "#b8862b"], btn: ["#f5d27a", "#d9a23a"], btnText: "#1d1505" },
  silver: { main: "#b9c2cf", light: "#eef2f7", deep: "#7d8794", rgb: "185,194,207", grad: ["#f1f4f8", "#b9c2cf", "#8a95a3"], btn: ["#eef1f5", "#aeb7c4"], btnText: "#14181e" },
  copper: { main: "#d27c4a", light: "#f4b48a", deep: "#8f4a24", rgb: "210,124,74", grad: ["#f6c09c", "#d27c4a", "#9c5229"], btn: ["#f1a679", "#c9683a"], btnText: "#1e0f06" },
};

function accentVars(a) {
  return `--gold:${a.main};--gold-light:${a.light};--gold-deep:${a.deep};--gold-rgb:${a.rgb};` +
    `--gold-grad:linear-gradient(135deg,${a.grad[0]} 0%,${a.grad[1]} 50%,${a.grad[2]} 100%);` +
    `--btn-grad:linear-gradient(180deg,${a.btn[0]},${a.btn[1]});--btn-text:${a.btnText};`;
}

// Runs before the page paints so the saved color never flashes gold first.
const ACCENT_BOOT = `<script>try{var a=localStorage.getItem("qw-accent");if(a)document.documentElement.setAttribute("data-accent",a)}catch(e){}</script>`;

// Safe way to put a string inside an inline <script>.
function jsString(s) {
  return JSON.stringify(String(s)).replace(/</g, "\\u003c");
}

// ---------- Floating toolbar shown on proxied pages ----------
// Lives in a closed shadow root so the visited site's CSS can't touch it.

const WIRE_ICON = `<svg viewBox="0 0 24 24"><path d="M2 12h5l3-7 4 14 3-7h5"/></svg>`;

const TOOLBAR_CSS = `
:host{all:initial}
*{box-sizing:border-box;font-family:system-ui,sans-serif}
.bar{position:fixed;left:50%;bottom:14px;transform:translateX(-50%);display:flex;align-items:center;gap:6px;padding:6px;
  width:min(560px,calc(100vw - 24px));background:rgba(12,14,18,.92);border:1px solid rgba(var(--ar),.55);border-radius:999px;
  box-shadow:0 10px 30px rgba(0,0,0,.45);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);color:#ece8df;font-size:14px;line-height:1}
.bar.off{display:none}
.b{flex:none;width:32px;height:32px;display:grid;place-items:center;border-radius:50%;border:1px solid #262a33;background:#1b1f27;
  color:#ece8df;cursor:pointer;padding:0;margin:0;transition:border-color .15s,color .15s}
.b:hover,.b:focus-visible{border-color:var(--a);color:var(--al);outline:none}
.b svg,.mini svg{width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2.2;stroke-linecap:round;stroke-linejoin:round}
.logo{border-color:rgba(var(--ar),.6);color:var(--a)}
form{flex:1;min-width:0;display:flex;margin:0}
input{flex:1;min-width:0;height:32px;padding:0 12px;margin:0;border-radius:999px;border:1px solid #262a33;background:#14171d;
  color:#ece8df;font-size:13px;outline:none}
input:focus{border-color:var(--a);box-shadow:0 0 0 3px rgba(var(--ar),.22)}
.mini{position:fixed;right:14px;bottom:14px;width:44px;height:44px;border-radius:50%;display:none;place-items:center;padding:0;
  background:rgba(12,14,18,.92);border:1px solid rgba(var(--ar),.7);box-shadow:0 8px 24px rgba(0,0,0,.45);cursor:pointer;color:var(--a)}
.mini svg{width:22px;height:22px;stroke-width:2.4}
.mini.on{display:grid}
.load{position:fixed;top:0;left:0;height:3px;width:0;background:linear-gradient(90deg,var(--al),var(--a));box-shadow:0 0 10px var(--a);
  opacity:0;transition:opacity .2s;pointer-events:none}
.load.on{opacity:1;animation:ld 8s cubic-bezier(.1,.7,.2,1) forwards}
@keyframes ld{0%{width:0}10%{width:35%}100%{width:92%}}
@media (max-width:480px){.sm-hide{display:none}}
`;

const TOOLBAR_HTML = `<div class="load"></div>
<div class="bar">
  <button class="b logo" data-a="home" title="QuickWire home">${WIRE_ICON}</button>
  <button class="b" data-a="back" title="Back"><svg viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"/></svg></button>
  <button class="b sm-hide" data-a="fwd" title="Forward"><svg viewBox="0 0 24 24"><path d="M9 18l6-6-6-6"/></svg></button>
  <button class="b" data-a="reload" title="Reload"><svg viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.6-6.4L21 8"/><path d="M21 3v5h-5"/></svg></button>
  <form><input spellcheck="false" autocomplete="off" aria-label="Address"></form>
  <button class="b" data-a="min" title="Hide toolbar"><svg viewBox="0 0 24 24"><path d="M6 9l6 6 6-6"/></svg></button>
</div>
<button class="mini" title="Show QuickWire toolbar">${WIRE_ICON}</button>`;

const TOOLBAR_ACCENTS = Object.fromEntries(
  Object.entries(ACCENTS).map(([k, v]) => [k, { a: v.main, al: v.light, ar: v.rgb }])
);

// Script loaded by every proxied page. It routes dynamic requests through the proxy,
// remembers the site in "Recent", and adds the QuickWire toolbar.
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

  // Everything below only runs in the main window, not in ads or embedded frames.
  var isTop=false; try{isTop=window.self===window.top;}catch(e){}
  if(!isTop)return;

  function realUrl(){var h=location.href,i=h.indexOf(P);return i>-1?h.slice(i+P.length):BASE;}

  // Remember this site for the "Recent" chips on the home page.
  try{
    var cur=new URL(realUrl());
    var list=JSON.parse(localStorage.getItem("qw-recent")||"[]").filter(function(r){return r&&r.host!==cur.hostname;});
    list.unshift({host:cur.hostname,url:cur.href});
    localStorage.setItem("qw-recent",JSON.stringify(list.slice(0,8)));
  }catch(e){}

  var ACC=${JSON.stringify(TOOLBAR_ACCENTS)}, CSS=${JSON.stringify(TOOLBAR_CSS)}, HTML=${JSON.stringify(TOOLBAR_HTML)};
  function build(){
    if(document.getElementById("qw-toolbar-host"))return;
    var acc=null,minimized=false;
    try{acc=ACC[localStorage.getItem("qw-accent")];minimized=localStorage.getItem("qw-bar")==="min";}catch(e){}
    acc=acc||ACC.gold;
    var host=document.createElement("qw-toolbar");
    host.id="qw-toolbar-host";
    host.setAttribute("style","all:initial;--a:"+acc.a+";--al:"+acc.al+";--ar:"+acc.ar);
    var root=host.attachShadow({mode:"closed"});
    root.innerHTML="<style>"+CSS+"</style>"+HTML;
    (document.body||document.documentElement).appendChild(host);

    var bar=root.querySelector(".bar"),mini=root.querySelector(".mini"),input=root.querySelector("input"),load=root.querySelector(".load");
    function setMin(m){bar.classList.toggle("off",m);mini.classList.toggle("on",m);try{localStorage.setItem("qw-bar",m?"min":"");}catch(e){}}
    setMin(minimized);
    input.value=realUrl();
    setInterval(function(){if(root.activeElement!==input){var u=realUrl();if(input.value!==u)input.value=u;}},1000);
    // Keep typing in the address box from triggering the site's keyboard shortcuts.
    ["keydown","keyup","keypress"].forEach(function(t){input.addEventListener(t,function(e){e.stopPropagation();});});
    input.addEventListener("focus",function(){input.select();});
    root.querySelector("form").addEventListener("submit",function(e){
      e.preventDefault();var v=input.value.trim();if(v)location.href="/go?url="+encodeURIComponent(v);
    });
    bar.addEventListener("click",function(e){
      var b=e.target.closest("[data-a]");if(!b)return;
      var a=b.getAttribute("data-a");
      if(a==="home")location.href="/";
      else if(a==="back")history.back();
      else if(a==="fwd")history.forward();
      else if(a==="reload")location.reload();
      else if(a==="min")setMin(true);
    });
    mini.addEventListener("click",function(){setMin(false);});
    window.addEventListener("beforeunload",function(){load.classList.remove("on");void load.offsetWidth;load.classList.add("on");});
    window.addEventListener("pageshow",function(){load.classList.remove("on");});
  }
  if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",build);else build();
})();`;
const CLIENT_JS_BUF = Buffer.from(CLIENT_JS);
const clientScriptEntry = { body: CLIENT_JS_BUF, encoded: {}, key: null };

// ---------- Site pages (Proxy and Games tabs) ----------

const SITE_NAME = "QuickWire Proxy";

function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

// Dark "wire" theme shared by every site page. The accent color is switchable.
const THEME_VARS = `
  :root{
    color-scheme:dark;
    --bg:#0c0e12; --panel:#14171d; --panel-2:#1b1f27; --line:#262a33; --text:#ece8df; --muted:#9a968c;
    ${accentVars(ACCENTS.gold)}
    --glow:0 0 0 3px rgba(var(--gold-rgb),.22);
  }
  :root[data-accent="silver"]{${accentVars(ACCENTS.silver)}}
  :root[data-accent="copper"]{${accentVars(ACCENTS.copper)}}`;

const SITE_CSS = `${THEME_VARS}
  *{box-sizing:border-box}
  [hidden]{display:none!important}
  body{margin:0;min-height:100vh;font-family:system-ui,sans-serif;background:var(--bg);color:var(--text);
       background-image:radial-gradient(ellipse at 50% 45%,rgba(var(--gold-rgb),.07),transparent 60%)}
  a{color:inherit}

  /* Circuit traces in the background */
  .wires{position:fixed;inset:0;width:100%;height:100%;z-index:0;pointer-events:none}
  .wires .trace{fill:none;stroke:var(--gold);stroke-width:1.5;opacity:.16}
  .wires .node{fill:var(--bg);stroke:var(--gold);stroke-width:1.5;opacity:.35}
  .wires .pulse{fill:none;stroke:var(--gold-light);stroke-width:2;stroke-linecap:round;
                stroke-dasharray:50 950;stroke-dashoffset:1000;opacity:.75;animation:current 6s linear infinite}
  .wires .pulse:nth-of-type(2n){animation-duration:8s;animation-delay:-3s}
  .wires .pulse:nth-of-type(3n){animation-duration:7s;animation-delay:-5s}
  @keyframes current{to{stroke-dashoffset:0}}

  /* Loading bar along the top edge */
  .loadbar{position:fixed;top:0;left:0;height:3px;width:0;z-index:50;background:var(--gold-grad);box-shadow:0 0 10px var(--gold);
           opacity:0;transition:opacity .2s;pointer-events:none}
  .loadbar.on{opacity:1;animation:load 8s cubic-bezier(.1,.7,.2,1) forwards}
  @keyframes load{0%{width:0}10%{width:35%}100%{width:92%}}

  nav,main{position:relative;z-index:1}
  nav{display:flex;gap:4px;align-items:center;padding:12px 16px;background:rgba(12,14,18,.75);backdrop-filter:blur(6px)}
  nav::after{content:"";position:absolute;left:0;right:0;bottom:0;height:1px;opacity:.55;
             background:linear-gradient(90deg,transparent,var(--gold) 20%,var(--gold) 80%,transparent)}
  .brand{display:flex;align-items:center;gap:8px;font-weight:700;margin-right:14px;text-decoration:none;letter-spacing:.2px}
  .gold{background:var(--gold-grad);-webkit-background-clip:text;background-clip:text;color:transparent}
  nav a.tab{text-decoration:none;padding:8px 14px;border-radius:8px;color:var(--muted);transition:color .15s,background .15s}
  nav a.tab:hover{color:var(--text);background:var(--panel)}
  nav a.tab.active{color:var(--gold-light);background:rgba(var(--gold-rgb),.1);box-shadow:inset 0 -2px 0 var(--gold)}

  /* Accent color picker */
  .accents{margin-left:auto;display:flex;gap:8px;align-items:center}
  .accents button{width:18px;height:18px;border-radius:50%;padding:0;border:0;cursor:pointer;transition:transform .15s}
  .accents button:hover{transform:scale(1.15)}
  .accents button[aria-pressed="true"]{outline:2px solid var(--text);outline-offset:2px}
  .accents .gold-sw{background:linear-gradient(135deg,${ACCENTS.gold.grad.join(",")})}
  .accents .silver-sw{background:linear-gradient(135deg,${ACCENTS.silver.grad.join(",")})}
  .accents .copper-sw{background:linear-gradient(135deg,${ACCENTS.copper.grad.join(",")})}
  @media (max-width:420px){nav .brand span{display:none}}

  main{padding:24px 16px}
  .center{min-height:calc(100vh - 140px);display:flex;align-items:center;justify-content:center}
  .box{width:min(680px,100%);text-align:center}
  h1{font-weight:700;margin:0 0 24px;letter-spacing:-.5px}
  .hero h1{font-size:clamp(34px,6vw,52px);margin-bottom:8px}
  .tagline{color:var(--muted);margin:0 0 28px;letter-spacing:3px;text-transform:uppercase;font-size:12px}

  form{display:flex;gap:8px;position:relative}
  input{flex:1;min-width:0;padding:14px 16px;border-radius:10px;border:1px solid var(--line);background:var(--panel);
        color:inherit;font-size:16px;transition:border-color .15s,box-shadow .15s}
  input::placeholder{color:#6f6c65}
  input:focus{outline:none;border-color:var(--gold);box-shadow:var(--glow)}

  .btn{display:inline-flex;align-items:center;justify-content:center;padding:14px 22px;border:0;border-radius:10px;background:var(--btn-grad);
       color:var(--btn-text);font:inherit;font-size:16px;font-weight:700;cursor:pointer;text-decoration:none;
       box-shadow:inset 0 0 0 1px var(--gold-deep),0 6px 20px rgba(var(--gold-rgb),.25);transition:filter .15s,box-shadow .15s,transform .05s}
  .btn:hover{filter:brightness(1.08);box-shadow:inset 0 0 0 1px var(--gold-deep),0 8px 28px rgba(var(--gold-rgb),.4)}
  .btn:active{transform:translateY(1px)}
  .btn:focus-visible{outline:none;box-shadow:var(--glow)}
  .btn.ghost{background:var(--panel-2);color:var(--text);box-shadow:inset 0 0 0 1px var(--line)}
  .btn.ghost:hover{color:var(--gold-light);box-shadow:inset 0 0 0 1px var(--gold)}

  /* Wires that plug into the search bar, with sparks that run in when you search */
  .plugged::before,.plugged::after{content:"";position:absolute;top:50%;width:90px;height:10px;margin-top:-5px;pointer-events:none;transition:filter .2s}
  .plugged::before{right:100%;margin-right:6px;
    background:radial-gradient(circle at calc(100% - 5px) 50%,var(--gold) 3.5px,transparent 4.5px),
               linear-gradient(90deg,transparent,var(--gold)) left center/calc(100% - 5px) 2px no-repeat}
  .plugged::after{left:100%;margin-left:6px;
    background:radial-gradient(circle at 5px 50%,var(--gold) 3.5px,transparent 4.5px),
               linear-gradient(90deg,var(--gold),transparent) right center/calc(100% - 5px) 2px no-repeat}
  .plugged:focus-within::before,.plugged:focus-within::after{filter:drop-shadow(0 0 4px var(--gold))}
  .spark{position:absolute;top:50%;width:16px;height:4px;margin-top:-2px;border-radius:2px;background:var(--gold-light);
         box-shadow:0 0 10px var(--gold-light);opacity:0;pointer-events:none}
  .spark.l{right:calc(100% + 12px)}
  .spark.r{left:calc(100% + 12px)}
  .sending .spark.l{animation:sparkL .5s linear infinite}
  .sending .spark.r{animation:sparkR .5s linear infinite}
  .sending::before,.sending::after{filter:drop-shadow(0 0 6px var(--gold-light))}
  @keyframes sparkL{0%{transform:translateX(-80px);opacity:0}25%{opacity:1}100%{transform:translateX(0);opacity:0}}
  @keyframes sparkR{0%{transform:translateX(80px);opacity:0}25%{opacity:1}100%{transform:translateX(0);opacity:0}}
  @media (max-width:860px){.plugged::before,.plugged::after,.spark{display:none}}

  p.hint{color:var(--muted);font-size:14px}

  /* Quick-launch tiles */
  .tiles{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-top:26px}
  .tile{display:flex;flex-direction:column;align-items:center;gap:8px;padding:14px 6px;border-radius:12px;background:rgba(20,23,29,.8);
        border:1px solid var(--line);text-decoration:none;font-size:13px;color:var(--muted);transition:border-color .15s,color .15s,transform .15s}
  .tile i{font-style:normal;width:38px;height:38px;border-radius:50%;display:grid;place-items:center;font-weight:800;font-size:17px;
          border:1px solid rgba(var(--gold-rgb),.45);color:var(--gold-light);background:var(--panel-2)}
  .tile:hover{border-color:var(--gold);color:var(--text);transform:translateY(-2px)}
  @media (max-width:640px){.tiles{grid-template-columns:repeat(3,1fr)}}

  /* Recent sites */
  .recent{display:flex;flex-wrap:wrap;gap:8px;justify-content:center;align-items:center;margin-top:18px}
  .recent .label{font-size:12px;letter-spacing:2px;text-transform:uppercase;color:var(--muted);margin-right:4px}
  .chip{display:inline-flex;align-items:center;border:1px solid var(--line);background:var(--panel);border-radius:999px;font-size:13px;transition:border-color .15s}
  .chip:hover{border-color:var(--gold)}
  .chip a{padding:6px 4px 6px 12px;text-decoration:none;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .chip button{background:none;border:0;color:var(--muted);padding:6px 10px 6px 6px;cursor:pointer;font-size:15px;line-height:1}
  .chip button:hover{color:var(--gold-light)}
  .recent .clear{background:none;border:0;color:var(--muted);font-size:12px;cursor:pointer;text-decoration:underline;padding:4px}
  .recent .clear:hover{color:var(--gold-light)}

  /* Games */
  .wrap{max-width:1100px;margin:0 auto}
  #q{width:100%;max-width:420px;flex:none}
  .sec h2{font-size:13px;letter-spacing:2px;text-transform:uppercase;color:var(--muted);font-weight:600;margin:30px 0 0;
          display:flex;align-items:center;gap:12px}
  .sec h2::after{content:"";flex:1;height:1px;background:linear-gradient(90deg,rgba(var(--gold-rgb),.5),transparent)}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:14px;margin-top:16px}
  .cell{position:relative}
  .card{display:block;text-decoration:none;background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden;
        transition:transform .15s,border-color .15s,box-shadow .15s}
  .card:hover{transform:translateY(-2px);border-color:var(--gold);box-shadow:0 8px 24px rgba(var(--gold-rgb),.15)}
  .thumb{aspect-ratio:4/3;background:var(--panel-2);display:flex;align-items:center;justify-content:center;font-size:42px;font-weight:800;
         border-bottom:1px solid var(--line)}
  .thumb b{background:var(--gold-grad);-webkit-background-clip:text;background-clip:text;color:transparent}
  .thumb img{width:100%;height:100%;object-fit:cover}
  .card span{display:block;padding:10px 12px;font-size:15px}
  .star{position:absolute;top:8px;right:8px;width:32px;height:32px;border-radius:50%;border:1px solid var(--line);
        background:rgba(12,14,18,.8);color:var(--muted);font-size:16px;line-height:1;cursor:pointer;display:grid;place-items:center;
        opacity:0;transition:opacity .15s,color .15s,border-color .15s}
  .cell:hover .star,.star:focus-visible,.cell.fav .star{opacity:1}
  .star:hover{color:var(--gold-light);border-color:var(--gold)}
  .cell.fav .star{color:var(--gold);border-color:var(--gold)}
  @media (hover:none){.star{opacity:1}}
  .empty{color:var(--muted);line-height:1.6;background:var(--panel);border:1px solid var(--line);border-left:3px solid var(--gold);border-radius:12px;padding:20px}
  code{background:var(--panel-2);color:var(--gold-light);padding:2px 6px;border-radius:6px}

  /* Error pages */
  .broken{display:block;width:min(320px,80vw);height:auto;margin:0 auto 14px}
  .broken .w{fill:none;stroke:var(--gold);stroke-width:3;stroke-linecap:round;stroke-linejoin:round}
  .broken .n{fill:var(--bg);stroke:var(--gold);stroke-width:2.5}
  .broken .sparks path{fill:none;stroke:var(--gold-light);stroke-width:2;stroke-linecap:round;animation:flicker 1.1s steps(2) infinite}
  .broken .sparks path:nth-child(2n){animation-delay:-.55s}
  @keyframes flicker{50%{opacity:0}}
  .err .code{font-size:clamp(56px,12vw,96px);font-weight:800;line-height:1;margin-bottom:8px;letter-spacing:-2px}
  .err h1{margin-bottom:10px}
  .actions{display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin-top:22px}
  .actions .btn{padding:11px 18px;font-size:15px}

  @media (prefers-reduced-motion:reduce){
    .wires .pulse{animation:none;opacity:0}
    .sending .spark,.broken .sparks path{animation:none}
    .loadbar.on{animation:none;width:60%}
  }
`;

// Wire-pulse logo, also used as the favicon. Its colors follow the accent.
const LOGO_SVG = `<svg width="24" height="24" viewBox="0 0 24 24" aria-hidden="true"><defs><linearGradient id="lg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" style="stop-color:var(--gold-light)"/><stop offset="1" style="stop-color:var(--gold-deep)"/></linearGradient></defs><path d="M2 12h5l3-7 4 14 3-7h5" fill="none" stroke="url(#lg)" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
const FAVICON = `<link rel="icon" href="data:image/svg+xml,${encodeURIComponent(
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="24" height="24" rx="6" fill="#0c0e12"/><path d="M2 12h5l3-7 4 14 3-7h5" fill="none" stroke="#e2a93b" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>`
)}">`;

// Background circuit board: traces run in from the edges and end at solder nodes.
const WIRE_PATHS = [
  ["M0 120 H180 L240 180 H380 L420 220 V300", 420, 300],
  ["M0 470 H90 L130 430 H240", 240, 430],
  ["M0 650 H140 L200 590 H330", 330, 590],
  ["M300 800 V710 L340 670 H470", 470, 670],
  ["M820 0 V90 L860 130 H990", 990, 130],
  ["M1200 170 H1010 L960 220 H850", 850, 220],
  ["M1200 390 H1110 L1080 360 H970", 970, 360],
  ["M1200 570 H1060 L1000 630 H900 L860 670 V740", 860, 740],
  ["M440 0 V60 L400 100 H300", 300, 100],
];
const WIRES_SVG = `<svg class="wires" viewBox="0 0 1200 800" preserveAspectRatio="xMidYMid slice" aria-hidden="true">
${WIRE_PATHS.map(([d]) => `<path class="trace" d="${d}"/>`).join("")}
${WIRE_PATHS.map(([d]) => `<path class="pulse" pathLength="1000" d="${d}"/>`).join("")}
${WIRE_PATHS.map(([, x, y]) => `<circle class="node" cx="${x}" cy="${y}" r="4.5"/>`).join("")}
</svg>`;

// Shared script for site pages: loading bar and the accent color picker.
const SITE_JS = `<script>(function(){
  var bar=document.getElementById("qwload");
  function load(){bar.classList.remove("on");void bar.offsetWidth;bar.classList.add("on");clearTimeout(bar._t);bar._t=setTimeout(function(){bar.classList.remove("on");},15000);}
  window.addEventListener("beforeunload",load);
  window.addEventListener("pageshow",function(){bar.classList.remove("on");document.querySelectorAll(".sending").forEach(function(f){f.classList.remove("sending");});});
  var picks=document.querySelectorAll("[data-set-accent]");
  function mark(){var cur=document.documentElement.getAttribute("data-accent")||"gold";picks.forEach(function(b){b.setAttribute("aria-pressed",String(b.getAttribute("data-set-accent")===cur));});}
  picks.forEach(function(b){b.addEventListener("click",function(){var a=b.getAttribute("data-set-accent");document.documentElement.setAttribute("data-accent",a);try{localStorage.setItem("qw-accent",a);}catch(e){}mark();});});
  mark();
})();</script>`;

function renderPage(title, activeTab, body, script = "") {
  const tab = (href, label, key) =>
    `<a class="tab${activeTab === key ? " active" : ""}" href="${href}">${label}</a>`;
  const swatch = (key, label) =>
    `<button type="button" class="${key}-sw" data-set-accent="${key}" title="${label} wires" aria-label="${label} theme"></button>`;
  return Buffer.from(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#0c0e12">${FAVICON}${ACCENT_BOOT}
<title>${escapeHtml(title)}</title><style>${SITE_CSS}</style></head>
<body>
<div class="loadbar" id="qwload"></div>
${WIRES_SVG}
<nav><a class="brand" href="/">${LOGO_SVG}<span>Quick<span class="gold">Wire</span></span></a>${tab("/", "Proxy", "proxy")}${tab("/games", "Games", "games")}
<div class="accents">${swatch("gold", "Gold")}${swatch("silver", "Silver")}${swatch("copper", "Copper")}</div></nav>
${body}
${SITE_JS}${script}
</body></html>`);
}

// Shortcut tiles under the search bar. Edit this list to change them.
const QUICK_LINKS = [
  { name: "Wikipedia", url: "https://en.wikipedia.org" },
  { name: "DuckDuckGo", url: "https://duckduckgo.com/html/" },
  { name: "Reddit", url: "https://old.reddit.com" },
  { name: "GitHub", url: "https://github.com" },
  { name: "Hacker News", url: "https://news.ycombinator.com" },
  { name: "Archive", url: "https://archive.org" },
];

const HOME_JS = `<script>(function(){
  var P=${JSON.stringify(PREFIX)};
  var form=document.querySelector("form.plugged");
  form.addEventListener("submit",function(){form.classList.add("sending");});

  var box=document.getElementById("recent"),list=[];
  try{list=JSON.parse(localStorage.getItem("qw-recent")||"[]").filter(function(r){return r&&/^https?:\\/\\//.test(r.url);});}catch(e){}
  function save(){try{localStorage.setItem("qw-recent",JSON.stringify(list));}catch(e){}}
  function render(){
    box.textContent="";
    box.hidden=!list.length;
    if(!list.length)return;
    var label=document.createElement("span");label.className="label";label.textContent="Recent";box.appendChild(label);
    list.forEach(function(item,i){
      var chip=document.createElement("span");chip.className="chip";
      var a=document.createElement("a");a.href=P+item.url;a.textContent=item.host;a.title=item.url;
      var x=document.createElement("button");x.type="button";x.textContent="\\u00d7";x.setAttribute("aria-label","Remove "+item.host);
      x.addEventListener("click",function(){list.splice(i,1);save();render();});
      chip.appendChild(a);chip.appendChild(x);box.appendChild(chip);
    });
    var clear=document.createElement("button");clear.type="button";clear.className="clear";clear.textContent="Clear";
    clear.addEventListener("click",function(){list=[];save();render();});
    box.appendChild(clear);
  }
  render();
})();</script>`;

const HOME_PAGE = renderPage(
  SITE_NAME,
  "proxy",
  `<main class="center"><div class="box hero">
  <h1>Quick<span class="gold">Wire</span> Proxy</h1>
  <p class="tagline">Browse through the wire</p>
  <form class="plugged" action="/go" method="get">
    <span class="spark l"></span><span class="spark r"></span>
    <input name="url" placeholder="Enter a URL or search term" autofocus required>
    <button class="btn" type="submit">Search</button>
  </form>
  <p class="hint">Example: example.com or https://en.wikipedia.org</p>
  <div class="tiles">${QUICK_LINKS.map(
    (l) => `<a class="tile" href="${escapeHtml(PREFIX + l.url)}"><i>${escapeHtml(l.name[0])}</i><span>${escapeHtml(l.name)}</span></a>`
  ).join("")}</div>
  <div class="recent" id="recent" hidden></div>
</div></main>`,
  HOME_JS
);

// ---------- Themed error pages ----------

const ERROR_TITLES = {
  400: "Crossed wires",
  403: "Access denied",
  404: "Nothing on this wire",
  413: "Too much to carry",
  500: "Short circuit",
  502: "Connection cut",
  504: "Signal timed out",
};

const BROKEN_WIRE_SVG = `<svg class="broken" viewBox="0 0 320 90" aria-hidden="true">
  <path class="w" d="M0 45 H70 L90 30 H128"/><path class="w" d="M320 45 H250 L230 60 H192"/>
  <circle class="n" cx="128" cy="30" r="5"/><circle class="n" cx="192" cy="60" r="5"/>
  <g class="sparks"><path d="M140 26 l10 -8"/><path d="M142 36 l12 2"/><path d="M180 64 l-12 6"/><path d="M178 54 l-10 -6"/><path d="M160 38 v12"/></g>
</svg>`;

function renderErrorPage(status, message) {
  const title = ERROR_TITLES[status] || "Something went wrong";
  const retry = status === 502 || status === 504 || status === 500;
  return renderPage(
    `${title} · ${SITE_NAME}`,
    "",
    `<main class="center"><div class="box err">
  ${BROKEN_WIRE_SVG}
  <div class="code gold">${status}</div>
  <h1>${escapeHtml(title)}</h1>
  <p class="hint">${escapeHtml(message)}</p>
  <div class="actions">
    <a class="btn" href="/">Home</a>
    <button class="btn ghost" type="button" onclick="history.back()">Go back</button>
    ${retry ? `<button class="btn ghost" type="button" onclick="location.reload()">Try again</button>` : ""}
  </div>
</div></main>`
  );
}

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
  const cells = games
    .map((g) => {
      const name = escapeHtml(prettyName(g.id));
      return `<div class="cell" data-id="${escapeHtml(g.id)}" data-name="${escapeHtml(prettyName(g.id).toLowerCase())}">
      <a class="card" href="/play/${encodeURIComponent(g.id)}">
      <div class="thumb">${g.thumb ? `<img src="${escapeHtml(g.thumb)}" alt="" loading="lazy">` : `<b>${escapeHtml(prettyName(g.id)[0] || "?")}</b>`}</div>
      <span>${name}</span></a>
      <button class="star" type="button" aria-pressed="false" aria-label="Favorite ${name}" title="Favorite">&#9733;</button></div>`;
    })
    .join("");
  const body = games.length
    ? `<input id="q" placeholder="Search ${games.length} game${games.length === 1 ? "" : "s"}" autocomplete="off">
       <section class="sec" id="favs" hidden><h2>Favorites</h2><div class="grid"></div></section>
       <section class="sec" id="played" hidden><h2>Recently played</h2><div class="grid"></div></section>
       <section class="sec" id="all"><h2>All games</h2><div class="grid" id="grid">${cells}</div></section>`
    : `<div class="empty">No games yet. Add an HTML file like <code>games/snake.html</code>, or a folder like
       <code>games/my-game/index.html</code>, next to main.js. Refresh this page and it will show up here.</div>`;
  return renderPage(
    `Games · ${SITE_NAME}`,
    "games",
    `<main><div class="wrap"><h1>Games</h1>${body}</div></main>`,
    games.length ? GAMES_JS : ""
  );
}

// Favorites (star button) and "Recently played", kept in the visitor's browser.
const GAMES_JS = `<script>(function(){
  function get(k){try{var v=JSON.parse(localStorage.getItem(k)||"[]");return Array.isArray(v)?v:[];}catch(e){return [];}}
  function put(k,v){try{localStorage.setItem(k,JSON.stringify(v));}catch(e){}}
  var favs=get("qw-fav"),played=get("qw-played"),q=document.getElementById("q");
  var cells=[].slice.call(document.querySelectorAll("#grid .cell")),byId={};
  cells.forEach(function(c){byId[c.getAttribute("data-id")]=c;});
  function fill(secId,ids){
    var sec=document.getElementById(secId),grid=sec.querySelector(".grid");
    grid.textContent="";
    ids.forEach(function(id){if(byId[id])grid.appendChild(byId[id].cloneNode(true));});
    sec.hidden=!grid.children.length||!!q.value;
  }
  function render(){
    cells.forEach(function(c){
      var on=favs.indexOf(c.getAttribute("data-id"))>-1;
      c.classList.toggle("fav",on);c.querySelector(".star").setAttribute("aria-pressed",String(on));
    });
    fill("favs",favs);fill("played",played.slice(0,6));
  }
  document.addEventListener("click",function(e){
    var star=e.target.closest(".star");if(!star)return;
    var id=star.closest(".cell").getAttribute("data-id"),i=favs.indexOf(id);
    if(i>-1)favs.splice(i,1);else favs.unshift(id);
    put("qw-fav",favs);render();
  });
  q.addEventListener("input",function(){
    var v=q.value.toLowerCase().trim();
    cells.forEach(function(c){c.hidden=c.getAttribute("data-name").indexOf(v)===-1;});
    render();
  });
  render();
})();</script>`;

function renderPlayPage(game) {
  const name = escapeHtml(prettyName(game.id));
  return Buffer.from(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${name} · ${SITE_NAME}</title>
<meta name="theme-color" content="#0c0e12">${FAVICON}${ACCENT_BOOT}
<style>${THEME_VARS}
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;background:#000;color:var(--text);font-family:system-ui,sans-serif}
  body{display:flex;flex-direction:column}
  header{position:relative;display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--bg)}
  header::after{content:"";position:absolute;left:0;right:0;bottom:0;height:1px;opacity:.6;
                background:linear-gradient(90deg,transparent,var(--gold) 20%,var(--gold) 80%,transparent)}
  header a,header button{color:var(--text);text-decoration:none;background:var(--panel-2);border:1px solid var(--line);border-radius:8px;
                         padding:7px 12px;font-size:14px;cursor:pointer;transition:border-color .15s,color .15s}
  header a:hover,header button:hover{border-color:var(--gold);color:var(--gold-light)}
  header button.primary{background:var(--btn-grad);color:var(--btn-text);font-weight:700;border-color:var(--gold-deep)}
  header button.primary:hover{color:var(--btn-text);filter:brightness(1.08)}
  header .logo{display:flex;border:0;background:none;padding:0}
  header strong{flex:1;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  iframe{flex:1;width:100%;border:0;background:#000}
</style></head>
<body>
<header><a class="logo" href="/" aria-label="Home">${LOGO_SVG}</a><a href="/games">&larr; Games</a><strong>${name}</strong>
  <button onclick="location.reload()">Restart</button>
  <button class="primary" onclick="var f=document.getElementById('game');(f.requestFullscreen||f.webkitRequestFullscreen).call(f)">Fullscreen</button></header>
<iframe id="game" src="${escapeHtml(game.src)}" allow="fullscreen; autoplay; gamepad; clipboard-write" allowfullscreen></iframe>
<script>
document.getElementById("game").addEventListener("load",function(){try{this.contentWindow.focus();}catch(e){}});
try{var id=${jsString(game.id)},l=JSON.parse(localStorage.getItem("qw-played")||"[]");
  if(!Array.isArray(l))l=[];l=l.filter(function(x){return x!==id;});l.unshift(id);
  localStorage.setItem("qw-played",JSON.stringify(l.slice(0,20)));}catch(e){}
</script>
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
    "cache-control": "no-cache", // always check for edits; unchanged files get a quick 304
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
  const page = renderErrorPage(status, String(message));
  res.writeHead(status, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "content-length": page.length });
  res.end(page);
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
