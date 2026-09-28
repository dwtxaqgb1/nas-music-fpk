#!/usr/bin/env python3
"""Bridge v2: fnmusic-ext <--> nas-music-download lxserver (port 5200).
Search via internal /api/music/search.
Playback: resolve url via /api/music/url, download to local cache, serve as file.
"""
import json, urllib.parse, urllib.request, sys, time, re, os, threading
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

_no_source_warned = False

NMD = "http://127.0.0.1:5200"
SELF = "http://127.0.0.1:8773"
CACHE_DIR = "/vol1/1000/音乐/fnmusic下载"
os.makedirs(CACHE_DIR, exist_ok=True)

_lock = threading.Lock()
_log_buf = []

def log(*a):
    msg = time.strftime("%H:%M:%S") + " " + " ".join(str(x) for x in a)
    print(msg, flush=True)
    _log_buf.append(msg)
    if len(_log_buf) > 200: _log_buf.pop(0)

def http_json(url, data=None, timeout=20):
    global _no_source_warned
    headers = {"Content-Type":"application/json"}
    if data is not None:
        body = json.dumps(data).encode()
    else:
        body = None
    req = urllib.request.Request(url, data=body, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        if ("未找到支持" in body or "自定义源" in body) and not _no_source_warned:
            _no_source_warned = True
            log("="*60)
            log("⚠️  未检测到可用音源！")
            log("   请打开 nmd 后台 (端口5200) 登录")
            log("   在「设置」中上传并启用音源JS（如玉宁熙.js）")
            log("="*60)
        raise

def resolve_url(song):
    """Call nmd /api/music/url, try flac -> 320k -> 128k, retry once on failure."""
    for attempt in range(2):
        for q in ["flac","320k","128k"]:
            try:
                r = http_json(f"{NMD}/api/music/url",
                    {"songInfo":song, "quality":q, "enableAutoSwitchApiSource":True}, timeout=15)
                url = r.get("url")
                if url:
                    return url, r.get("type","flac")
            except Exception as e:
                log(f"  resolve {q} attempt{attempt+1} failed: {e}")
        if attempt == 0:
            log("  retrying resolve...")
            time.sleep(1)
    return None, None

def safe_name(s):
    return re.sub(r'[\\/:*?"<>|]', '_', s).strip()

def download_to_cache(song):
    """Download direct URL to CACHE_DIR/<name - artist>.<ext>. Returns path."""
    direct_url, ext = resolve_url(song)
    if not direct_url:
        raise RuntimeError("no url from nmd")
    if ext not in ("flac","mp3"): ext = "flac"
    name = safe_name(song.get("name","unknown")) or "unknown"
    singer = safe_name(song.get("singer","")) or "unknown"
    fname = f"{name} - {singer}.{ext}"
    cache_path = os.path.join(CACHE_DIR, fname)
    # avoid overwrite
    if os.path.exists(cache_path):
        return cache_path, ext
    log(f"  downloading {fname} from {direct_url[:60]}...")
    req = urllib.request.Request(direct_url, headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        with open(cache_path+".part","wb") as f:
            while True:
                chunk = r.read(1<<16)
                if not chunk: break
                f.write(chunk)
    os.rename(cache_path+".part", cache_path)
    return cache_path, ext

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try: self.wfile.write(body)
        except: pass
    def _serve_file(self, path, ct):
        size = os.path.getsize(path)
        range_h = self.headers.get("Range")
        if range_h:
            rng = re.match(r"bytes=(\d+)-(\d*)", range_h)
            start = int(rng.group(1)) if rng else 0
            end = int(rng.group(2)) if rng and rng.group(2) else size-1
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        else:
            start, end = 0, size-1
            self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(end-start+1))
        self.send_header("Accept-Ranges","bytes")
        self.end_headers()
        with open(path,"rb") as f:
            f.seek(start)
            remaining = end-start+1
            while remaining > 0:
                chunk = f.read(min(1<<16, remaining))
                if not chunk: break
                try: self.wfile.write(chunk)
                except: break
                remaining -= len(chunk)

    def do_GET(self):
        t0 = time.time()
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        def one(k,d=""):
            v=qs.get(k,[d]); return v[0] if v else d
        try:
            if u.path == "/healthz":
                self._json(200, {"ok":True,"service":"lxbridge-v2","version":"2.0",
                    "sources":["kg","wy","mg","kw","tx"],
                    "user_source":{"configured":True,"initialized":True,
                        "source":{"name":"nmd-lxserver","version":"2.0",
                            "platforms":{s:{"name":s,"qualitys":["128k","320k","flac"]} for s in ["kg","kw","mg","tx","wy"]}},
                        "running":True}}); return

            if u.path == "/logs":
                self._json(200, {"ok":True,"logs":list(reversed(_log_buf[-100:]))}); return

            # /stream/<source>_<songmid> --- stream while saving
            m = re.match(r"^/stream/([a-z]+)_(.+)$", u.path)
            if m:
                src, songmid = m.group(1), m.group(2)
                key = f"{src}_{songmid}"
                # check if local file already exists
                cache_path = _file_map.get(key)
                if not cache_path or not os.path.exists(cache_path):
                    song = _song_cache.get(key) or _song_cache.get(f"lx:{src}:{songmid}")
                    if not song:
                        self.send_response(404); self.end_headers(); return
                    # resolve direct URL
                    direct_url, ext = resolve_url(song)
                    if not direct_url:
                        self.send_response(502); self.end_headers(); return
                    if ext not in ("flac","mp3"): ext = "flac"
                    name = safe_name(song.get("name","unknown")) or "unknown"
                    singer = safe_name(song.get("singer","")) or "unknown"
                    fname = f"{name} - {singer}.{ext}"
                    cache_path = os.path.join(CACHE_DIR, fname)
                    _file_map[key] = cache_path
                    # if file already fully downloaded, serve it
                    if os.path.exists(cache_path):
                        size = os.path.getsize(cache_path)
                        ct = "audio/flac" if ext=="flac" else "audio/mpeg"
                        self._serve_file(cache_path, ct)
                        return
                    # stream while saving
                    log(f"  streaming {fname} from {direct_url[:60]}...")
                    req = urllib.request.Request(direct_url, headers={"User-Agent":"Mozilla/5.0"})
                    remote = urllib.request.urlopen(req, timeout=30)
                    ct = remote.headers.get("Content-Type","audio/flac")
                    total = int(remote.headers.get("Content-Length",0))
                    range_h = self.headers.get("Range")
                    out = open(cache_path+".part","wb")
                    if range_h:
                        rng = re.match(r"bytes=(\d+)-(\d*)", range_h)
                        start = int(rng.group(1)) if rng else 0
                        self.send_response(206)
                        self.send_header("Content-Range", f"bytes {start}-{total-1}/{total}")
                    else:
                        start = 0
                        self.send_response(200)
                    self.send_header("Content-Type", ct)
                    self.send_header("Content-Length", str(total))
                    self.send_header("Accept-Ranges","bytes")
                    self.end_headers()
                    bytes_sent = 0
                    try:
                        while True:
                            chunk = remote.read(1<<16)
                            if not chunk: break
                            out.write(chunk)
                            try: self.wfile.write(chunk)
                            except: break
                            bytes_sent += len(chunk)
                    finally:
                        out.close()
                    # if fully downloaded, rename
                    if total == 0 or bytes_sent >= total:
                        os.rename(cache_path+".part", cache_path)
                    log(f"streamed {fname} {bytes_sent}B in {time.time()-t0:.2f}s")
                    return
                # local file exists, serve it
                ext = cache_path.rsplit(".",1)[-1]
                ct = "audio/flac" if ext=="flac" else "audio/mpeg"
                self._serve_file(cache_path, ct)
                return

            if u.path == "/api/v1/search":
                kw = one("keyword") or one("q") or ""
                limit = int(one("limit","20") or 20)
                log(f"search {kw!r}")
                # search cache
                ckey = ("search", kw, limit)
                if ckey in _search_cache:
                    exp, items = _search_cache[ckey]
                    if exp > time.time():
                        log(f"search cache hit -> {len(items)} items")
                        self._json(200, {"ok":True,"items":items}); return
                # parallel search across working sources only
                from concurrent.futures import ThreadPoolExecutor
                def search_src(src):
                    try:
                        d = http_json(f"{NMD}/api/music/search?name={urllib.parse.quote(kw)}&source={src}&type=song&limit={limit}&page=1", timeout=8)
                        return src, d if isinstance(d, list) else []
                    except Exception as e:
                        log(f"  {src} err: {e}")
                        return src, []
                items = []
                with ThreadPoolExecutor(max_workers=4) as ex:
                    for src, results in ex.map(search_src, ["kw","wy","mg","tx"]):
                        for s in results:
                            sm = s.get("songmid","")
                            sid = f"lx:{src}:{sm}"
                            song_info = {
                                "name":s.get("name",""),"singer":s.get("singer",""),
                                "source":src,"songmid":sm,"interval":s.get("interval","00:00"),
                                "img":s.get("img",""),"types":s.get("types",[])}
                            _song_cache[sid] = song_info
                            _song_cache[f"{src}_{sm}"] = song_info
                            iv = s.get("interval","0:0")
                            try:
                                parts = iv.split(":"); dur = int(parts[0])*60+int(parts[1])
                            except: dur = 0
                            items.append({"id":sid,"lx_source":src,
                                "title":s.get("name",""),"artist":s.get("singer",""),
                                "album":s.get("albumName",""),"duration_s":float(dur),
                                "ext":"flac","cover_url":s.get("img",""),
                                "file_size":0,"verified":True})
                _search_cache[ckey] = (time.time()+300, items)
                log(f"search -> {len(items)} items in {time.time()-t0:.2f}s")
                self._json(200, {"ok":True,"items":items}); return

            if u.path == "/api/v1/track/url":
                sid = one("id"); quality = one("quality","lossless")
                log(f"url {sid}")
                local = f"{SELF}/stream/{sid.split(':')[1]}_{sid.split(':')[2]}"
                self._json(200, {"ok":True,"data":{"id":sid,"url":local,"ext":"flac",
                    "file_size":0,"actual_tier":quality,"validation_status":"media_verified"}}); return

            if u.path == "/api/v1/track/lyric":
                sid = one("id")
                src, sm = sid.split(":")[1], sid.split(":")[2]
                try:
                    d = http_json(f"{NMD}/api/music/lyric?source={src}&songmid={sm}", timeout=10)
                    self._json(200, {"ok":True,"data":{"lyric":d.get("lyric","")}})
                except Exception as e:
                    self._json(200, {"ok":True,"data":{"lyric":""}})
                return

            if u.path == "/api/v1/track/info":
                sid = one("id")
                si = _song_cache.get(sid, {})
                iv = si.get("interval","0:0")
                try:
                    parts = iv.split(":"); dur = int(parts[0])*60+int(parts[1])
                except: dur = 0
                self._json(200, {"ok":True,"data":{
                    "id":sid,
                    "title":si.get("name",""),
                    "artist":si.get("singer",""),
                    "album":"",
                    "cover_url":si.get("img",""),
                    "duration_s":float(dur),
                    "ext":"flac"
                }}); return
            self._json(404, {"ok":False,"error":"nf"})
        except Exception as e:
            log("EXC", u.path, e)
            try: self._json(500, {"ok":False,"error":str(e)})
            except: pass

_song_cache = {}
_search_cache = {}
_file_map = {}

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv)>1 else 8773
    log(f"bridge v2 on :{port} -> nmd:{NMD}, download-then-play")
    ThreadedHTTPServer(("127.0.0.1", port), H).serve_forever()
