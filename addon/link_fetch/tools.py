"""Link Fetch: download client deliveries from share links and put them back together as one folder tree.

Everything is written inside data_dir() (the only folder an add-on may write in): each fetch gets its own folder
<data>/fetches/<id>/ with `raw/` (what was downloaded), `tree/` (the merged, cleaned result) and `job.json` (its
state, so a fetch survives a restart of the add-on and is taken up again where it stopped).

Google Drive splits a big folder into several zips (drive-download-...-001.zip, -002.zip...), each holding a PART of
the same folder tree. They are merged by relative path, so files of the same folder land together again.

A dropped connection is retried with a growing wait, and a half-downloaded file goes on from where it stopped (an
HTTP Range request). The add-on sends the events fetch.done and fetch.failed when a fetch ends.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from .toolkit import ToolError, VALUES, data_dir, prop, tool

MODULE = "fetch"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
JUNK = {".DS_Store", "Thumbs.db", "desktop.ini", "Icon\r"}
AUDIO = {".wav", ".aif", ".aiff", ".flac", ".mp3", ".m4a", ".caf", ".bwf"}
SESSION = {".ptx", ".ptf", ".als", ".logicx", ".rpp", ".cpr"}
CHUNK = 1 << 20
TRIES = 12                      # per file; the waits grow 2, 4, 8 ... up to 120 s
EVENTS = ["fetch.done", "fetch.failed", "fetch.resumed"]

_JOBS = {}
_LOCK = threading.Lock()
EMIT = None                     # set by __main__: EMIT(event, data) sends a provider.event to the hub


class Retry(Exception):
    """A failure worth trying again (network, 5xx, 429)."""


# ---- small helpers -------------------------------------------------------------------------------------------------
def _root() -> str:
    p = os.path.join(data_dir(), "fetches")
    os.makedirs(p, exist_ok=True)
    return p


def _safe(name: str) -> str:
    name = re.sub(r"[/\\:\x00]", "-", name).strip().strip(".") or "file"
    return name[:180]


def _emit(event: str, data: dict) -> None:
    try:
        if EMIT:
            EMIT(event, data)
    except Exception:  # noqa: BLE001 - an event that cannot be sent must not stop the fetch
        pass


def _persist(job: dict) -> None:
    path = os.path.join(job["folder"], "job.json")
    os.makedirs(job["folder"], exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({k: v for k, v in job.items() if k != "cancel"}, f)
    os.replace(tmp, path)


def _open(url: str, data: bytes = None, headers: dict = None, timeout: int = 60):
    h = {"User-Agent": UA}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h)
    host = urllib.parse.urlsplit(url).hostname
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code in (408, 429) or e.code >= 500:
            raise Retry("%s answered HTTP %s" % (host, e.code))
        if e.code == 416:
            raise
        raise ToolError("%s answered HTTP %s (%s)" % (host, e.code, e.reason), "http_error")
    except (urllib.error.URLError, OSError) as e:
        reason = str(getattr(e, "reason", e))
        if "not permitted" in reason or "nodename nor servname" in reason:
            raise ToolError("could not reach %s (%s) - is the internet allowed for this add-on, and is this host in "
                            "its list?" % (host, reason), "network")
        raise Retry("could not reach %s (%s)" % (host, reason))


def _retrying(job: dict, what: str, fn):
    """fn() again after a growing wait while it raises Retry; never gives up before TRIES."""
    wait = 2.0
    for n in range(1, TRIES + 1):
        if job.get("cancel"):
            raise ToolError("cancelled", "cancelled")
        try:
            return fn()
        except (Retry, ConnectionError, TimeoutError) as e:
            if n == TRIES:
                raise ToolError("%s: gave up after %d tries (%s)" % (what, TRIES, e), "network")
            job["retrying"] = "%s (try %d of %d: %s)" % (what, n + 1, TRIES, e)
            time.sleep(wait)
            wait = min(wait * 2, 120.0)
    return None


def _filename(resp, fallback: str) -> str:
    cd = resp.headers.get("Content-Disposition") or ""
    m = re.search(r"filename\*=UTF-8''([^;]+)", cd) or re.search(r'filename="?([^";]+)"?', cd)
    if m:
        return _safe(urllib.parse.unquote(m.group(1)))
    tail = os.path.basename(urllib.parse.urlsplit(resp.geturl()).path)
    return _safe(urllib.parse.unquote(tail) or fallback)


def _download(job: dict, url_fn, folder: str, name: str, fallback: str) -> str:
    """One file, resumable: a finished file is skipped, a .part goes on with a Range request, a dropped connection is
    retried. `name` empty: the server's own file name. Answers the file's path."""
    os.makedirs(folder, exist_ok=True)
    if name and os.path.exists(os.path.join(folder, name)):
        job["filesDone"] = job.get("filesDone", 0) + 1
        job["skipped"] = job.get("skipped", 0) + 1
        return os.path.join(folder, name)
    part = os.path.join(folder, (name or _safe(fallback)) + ".part")
    final = {"name": name}

    def attempt():
        have = os.path.getsize(part) if os.path.exists(part) else 0
        headers = {"Range": "bytes=%d-" % have} if have else {}
        try:
            resp = _open(url_fn(), headers=headers, timeout=120)
        except urllib.error.HTTPError:                # 416: the part is already whole
            return
        if (resp.headers.get("Content-Type") or "").startswith("text/html") and job["provider"] == "drive":
            raise ToolError("Google Drive answered a web page instead of %s (not shared publicly, or over its "
                            "download quota - try again later)" % (name or fallback), "not_public")
        if not final["name"]:
            final["name"] = _filename(resp, fallback)
        mode = "ab" if have and resp.status == 206 else "wb"
        if mode == "wb":
            job["bytes"] = job.get("bytes", 0) - have if have else job.get("bytes", 0)
        total = resp.headers.get("Content-Length")
        if total and not job.get("bytesTotalKnown"):
            job["currentSize"] = int(total) + (have if mode == "ab" else 0)
        try:
            with open(part, mode) as f:
                while True:
                    if job.get("cancel"):
                        raise ToolError("cancelled", "cancelled")
                    b = resp.read(CHUNK)
                    if not b:
                        break
                    f.write(b)
                    job["bytes"] = job.get("bytes", 0) + len(b)
        except (OSError, ConnectionError) as e:
            if isinstance(e, PermissionError):
                raise
            raise Retry("the connection dropped (%s)" % e)
        if total and os.path.getsize(part) < int(total) + (have if mode == "ab" else 0):
            raise Retry("the file came in short")

    _retrying(job, name or fallback, attempt)
    dest = os.path.join(folder, final["name"] or _safe(fallback))
    os.replace(part, dest)
    job["filesDone"] = job.get("filesDone", 0) + 1
    job["retrying"] = ""
    if job["filesDone"] % 5 == 0:
        _persist(job)
    return dest


# ---- providers -----------------------------------------------------------------------------------------------------
def _kind(url: str) -> str:
    host = (urllib.parse.urlsplit(url).hostname or "").lower()
    if host.endswith("dropbox.com") or host.endswith("dropboxusercontent.com"):
        return "dropbox"
    if host in ("we.tl",) or host.endswith("wetransfer.com"):
        return "wetransfer"
    if host.endswith("drive.google.com") or host.endswith("docs.google.com") or host.endswith("usercontent.google.com"):
        return "drive"
    if host.endswith("box.com") or host.endswith("box.net"):
        raise ToolError("Box links are not handled by this add-on", "unsupported")
    raise ToolError("not a Dropbox, WeTransfer or Google Drive link: %s" % host, "unsupported")


def _dropbox(job: dict, url: str, raw: str) -> list:
    p = urllib.parse.urlsplit(url)
    q = dict(urllib.parse.parse_qsl(p.query))
    q["dl"] = "1"
    u = urllib.parse.urlunsplit((p.scheme or "https", p.netloc, p.path, urllib.parse.urlencode(q), ""))
    job["filesTotal"] = 1
    return [_download(job, lambda: u, raw, "", "dropbox-download")]


def _wetransfer(job: dict, url: str, raw: str) -> list:
    if (urllib.parse.urlsplit(url).hostname or "").lower() == "we.tl":
        url = _retrying(job, "we.tl link", lambda: _open(url).geturl())
    parts = [x for x in urllib.parse.urlsplit(url).path.split("/") if x]
    if "downloads" not in parts:
        raise ToolError("this WeTransfer link has no /downloads/<id>/<hash> part: %s" % url, "bad_link")
    rest = parts[parts.index("downloads") + 1:]
    if len(rest) < 2:
        raise ToolError("this WeTransfer link is incomplete", "bad_link")
    tid = rest[0]
    body = {"security_hash": rest[-1], "intent": "entire_transfer"}
    if len(rest) == 3:
        body["recipient_id"] = rest[1]

    def direct():
        resp = _open("https://wetransfer.com/api/v4/transfers/%s/download" % tid, data=json.dumps(body).encode(),
                     headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"})
        link = json.loads(resp.read().decode("utf-8") or "{}").get("direct_link")
        if not link:
            raise ToolError("WeTransfer gave no download link (expired, or a password-protected transfer)", "expired")
        return link

    job["filesTotal"] = 1
    return [_download(job, direct, raw, "", "wetransfer-download")]   # a fresh signed link on every try


def _drive_id(url: str):
    p = urllib.parse.urlsplit(url)
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", p.path)
    if m:
        return "folder", m.group(1)
    m = re.search(r"/d/([A-Za-z0-9_-]+)", p.path)
    if m:
        return "file", m.group(1)
    q = dict(urllib.parse.parse_qsl(p.query))
    if q.get("id"):
        return "file", q["id"]
    raise ToolError("no Google Drive file or folder id in this link", "bad_link")


def _drive_url(fid: str) -> str:
    key = VALUES.get("googleApiKey") or ""
    if key:
        return "https://www.googleapis.com/drive/v3/files/%s?alt=media&supportsAllDrives=true&key=%s" % (fid, key)
    return "https://drive.usercontent.google.com/download?id=%s&export=download&confirm=t" % fid


def _drive_list(fid: str, key: str) -> list:
    items, token = [], ""
    while True:
        q = urllib.parse.urlencode({"q": "'%s' in parents and trashed = false" % fid, "key": key,
                                    "fields": "nextPageToken, files(id, name, mimeType, size)", "pageSize": 1000,
                                    "supportsAllDrives": "true", "includeItemsFromAllDrives": "true",
                                    "pageToken": token})
        data = json.loads(_open("https://www.googleapis.com/drive/v3/files?" + q).read().decode("utf-8"))
        items += data.get("files", [])
        token = data.get("nextPageToken") or ""
        if not token:
            return items


def _drive_list_public(fid: str) -> list:
    """A public folder's entries without an API key, from Drive's embedded folder view (one level)."""
    html = _open("https://drive.google.com/embeddedfolderview?id=%s" % fid).read().decode("utf-8", "replace")
    items = []
    for m in re.finditer(r'<div class="flip-entry" id="entry-([A-Za-z0-9_-]+)".*?<a href="([^"]+)".*?'
                         r'<div class="flip-entry-title">(.*?)</div>', html, re.S):
        eid, href, title = m.group(1), m.group(2), m.group(3)
        title = re.sub(r"<[^>]+>", "", title)
        for a, b in (("&amp;", "&"), ("&#39;", "'"), ("&quot;", '"'), ("&lt;", "<"), ("&gt;", ">")):
            title = title.replace(a, b)
        folder = "/folders/" in href
        items.append({"id": eid, "name": title.strip(),
                      "mimeType": "application/vnd.google-apps.folder" if folder else ""})
    if not items and "flip-entry" not in html:
        raise ToolError("Google Drive did not list this folder: it is not shared as 'anyone with the link', or "
                        "set a Google Drive API key in Add-ons > Link Fetch", "not_public")
    return items


def _drive(job: dict, url: str, raw: str) -> list:
    kind, fid = _drive_id(url)
    if kind == "file":
        job["filesTotal"] = 1
        return [_download(job, lambda: _drive_url(fid), raw, "", fid)]
    key = VALUES.get("googleApiKey") or ""
    todo = []

    def walk(folder_id: str, rel: str):
        listing = _retrying(job, "listing %s" % (rel or "the folder"),
                            lambda: _drive_list(folder_id, key) if key else _drive_list_public(folder_id))
        for it in listing:
            name = _safe(it["name"])
            if it.get("mimeType") == "application/vnd.google-apps.folder":
                walk(it["id"], os.path.join(rel, name))
            elif not it.get("mimeType", "").startswith("application/vnd.google-apps."):
                todo.append((it["id"], rel, name, int(it.get("size") or 0)))

    job["state"] = "listing"
    walk(fid, "")
    only = set(job.get("only") or [])
    if only:
        todo = [t for t in todo if t[2] in only or os.path.join(t[1], t[2]) in only]
        if not todo:
            raise ToolError("none of the files in `only` is in this folder; list_link shows its names", "not_found")
    job["filesTotal"] = len(todo)
    if all(t[3] for t in todo):
        job["bytesTotal"] = sum(t[3] for t in todo)
        job["bytesTotalKnown"] = True
    job["state"] = "downloading"
    job["filesDone"], job["skipped"], job["bytes"] = 0, 0, 0
    _persist(job)
    out = []
    for i, (file_id, rel, name, size) in enumerate(todo):
        job["current"] = os.path.join(rel, name)
        if os.path.exists(os.path.join(raw, rel, name)):
            job["bytes"] += os.path.getsize(os.path.join(raw, rel, name))
        out.append(_download(job, lambda f=file_id: _drive_url(f), os.path.join(raw, rel), name, name))
    return out


# ---- merge and clean -----------------------------------------------------------------------------------------------
def _junk(name: str) -> bool:
    return name in JUNK or name.startswith("._") or name == "__MACOSX" or name.endswith(".part")


def _merge_into(tree: str, src_files: list, base: str) -> dict:
    """Unzips every zip into `tree` by relative path (Drive's split parts merge back into one tree) and copies loose
    files keeping their path under `base`. A file met twice with the same size is kept once; a different one gets a
    numbered name."""
    stats = {"zips": 0, "files": 0, "duplicates": 0, "renamed": 0}
    os.makedirs(tree, exist_ok=True)

    def place(rel: str, size: int, writer):
        parts = [_safe(p) for p in rel.replace("\\", "/").split("/") if p and p not in (".", "..")]
        if not parts or any(_junk(p) for p in parts):
            return
        dest = os.path.join(tree, *parts)
        if os.path.exists(dest):
            if os.path.getsize(dest) == size:
                stats["duplicates"] += 1
                return
            b, e = os.path.splitext(dest)
            n = 2
            while os.path.exists("%s (%d)%s" % (b, n, e)):
                n += 1
            dest = "%s (%d)%s" % (b, n, e)
            stats["renamed"] += 1
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        writer(dest)
        stats["files"] += 1

    for f in src_files:
        if f.lower().endswith(".zip") and zipfile.is_zipfile(f):
            stats["zips"] += 1
            with zipfile.ZipFile(f) as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue
                    name = info.filename
                    if not (info.flag_bits & 0x800):
                        try:
                            name = name.encode("cp437").decode("utf-8")
                        except (UnicodeEncodeError, UnicodeDecodeError):
                            pass

                    def w(dest, info=info, z=z):
                        with z.open(info) as s, open(dest, "wb") as d:
                            shutil.copyfileobj(s, d, CHUNK)
                    place(name, info.file_size, w)
        else:
            rel = os.path.relpath(f, base) if base else os.path.basename(f)
            place(rel, os.path.getsize(f), lambda dest, f=f: shutil.copyfile(f, dest))
    _flatten(tree)
    return stats


def _flatten(tree: str) -> None:
    """A tree holding a single folder and nothing else is lifted one level (a zip's wrapper folder)."""
    while True:
        items = [x for x in os.listdir(tree) if not _junk(x)]
        if len(items) != 1 or not os.path.isdir(os.path.join(tree, items[0])):
            return
        inner = os.path.join(tree, items[0])
        tmp = tree + ".lift"
        os.replace(inner, tmp)
        shutil.rmtree(tree)
        os.replace(tmp, tree)


def _summary(tree: str) -> dict:
    sessions, audio, other, folders = [], 0, 0, {}
    size = 0
    for d, dirs, files in os.walk(tree):
        rel = os.path.relpath(d, tree)
        for fn in files:
            ext = os.path.splitext(fn)[1].lower()
            size += os.path.getsize(os.path.join(d, fn))
            if ext in SESSION:
                sessions.append(os.path.join(rel, fn) if rel != "." else fn)
            elif ext in AUDIO:
                audio += 1
                folders[rel] = folders.get(rel, 0) + 1
            else:
                other += 1
    return {"tree": tree, "sessions": sessions, "audioFiles": audio, "otherFiles": other, "bytes": size,
            "audioByFolder": [{"folder": k, "audio": v} for k, v in sorted(folders.items())][:40]}


# ---- jobs ----------------------------------------------------------------------------------------------------------
def _raw_files(raw: str) -> list:
    out = []
    for d, _, files in os.walk(raw):
        out += [os.path.join(d, f) for f in sorted(files) if not f.endswith(".part")]
    return out


def _run(job: dict):
    raw = os.path.join(job["folder"], "raw")
    try:
        {"dropbox": _dropbox, "wetransfer": _wetransfer, "drive": _drive}[job["provider"]](job, job["url"], raw)
        job["state"] = "merging"
        _persist(job)
        tree = os.path.join(job["folder"], "tree")
        if os.path.isdir(tree):
            shutil.rmtree(tree)
        job["stats"] = _merge_into(tree, _raw_files(raw), raw)
        job["result"] = _summary(tree)
        job["state"] = "done"
        job["error"] = ""
    except ToolError as e:
        job["state"], job["error"] = "failed", str(e)
    except Exception as e:  # noqa: BLE001 - reported to the caller as the job's error
        job["state"], job["error"] = "failed", "%s: %s" % (type(e).__name__, e)
    job["ended"] = time.time()
    _persist(job)
    _emit("fetch.done" if job["state"] == "done" else "fetch.failed",
          {"job": job["id"], "state": job["state"], "error": job.get("error", ""),
           "tree": (job.get("result") or {}).get("tree", ""), "files": job.get("filesDone", 0)})


def _start(job: dict) -> None:
    job.pop("cancel", None)
    with _LOCK:
        _JOBS[job["id"]] = job
    _persist(job)
    threading.Thread(target=_run, args=(job,), daemon=True, name="fetch-" + job["id"]).start()


def resume_unfinished() -> list:
    """At start-up: every fetch whose job.json says it was still running is taken up again."""
    taken = []
    for jid in sorted(os.listdir(_root())):
        path = os.path.join(_root(), jid, "job.json")
        try:
            with open(path, encoding="utf-8") as f:
                job = json.load(f)
        except (OSError, ValueError):
            continue
        if job.get("state") in ("listing", "downloading", "merging") and job.get("url"):
            job["resumes"] = job.get("resumes", 0) + 1
            job["state"] = "downloading"
            _start(job)
            taken.append(jid)
            _emit("fetch.resumed", {"job": jid})
    return taken


def _public(job: dict) -> dict:
    out = {k: v for k, v in job.items() if k not in ("cancel", "url", "bytesTotalKnown", "currentSize")}
    total, done = job.get("filesTotal") or 0, job.get("filesDone") or 0
    if job.get("bytesTotal"):
        out["percent"] = round(100.0 * min(job.get("bytes", 0), job["bytesTotal"]) / job["bytesTotal"], 1)
    elif total:
        out["percent"] = round(100.0 * done / total, 1)
    if job.get("state") == "done":
        out["percent"] = 100.0
    return out


def _load_job(jid: str) -> dict:
    job = _JOBS.get(jid)
    if job:
        return job
    try:
        with open(os.path.join(_root(), jid, "job.json"), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        raise ToolError("no fetch %s; list_fetches shows them" % jid, "not_found")


@tool("fetch_link", "Starts downloading a Dropbox, WeTransfer or Google Drive share link (a file or a whole folder) "
      "into the add-on's folder, then unzips and merges it into one folder tree. Answers a job id at once; "
      "fetch_status follows it, and the add-on sends fetch.done or fetch.failed when it ends.",
      props={"url": prop("string", "The share link.", minLength=8, maxLength=2000),
             "name": prop("string", "A name for this fetch, e.g. the song (default: from the link).", maxLength=120),
             "only": prop("array", "Google Drive folders: download only these files (names or paths as list_link "
                          "gives them), e.g. the ones still missing.")},
      required=("url",), writes=True, module=MODULE, timeout_ms=30000)
def fetch_link(a: dict) -> dict:
    url = a["url"].strip()
    provider = _kind(url)
    jid = time.strftime("%Y%m%d-%H%M%S") + "-" + _safe(a.get("name") or provider)[:40].replace(" ", "_")
    job = {"id": jid, "provider": provider, "url": url, "state": "downloading", "bytes": 0, "filesDone": 0,
           "started": time.time(), "folder": os.path.join(_root(), jid)}
    if a.get("only"):
        job["only"] = [str(x) for x in a["only"]][:500]
    _start(job)
    return _public(job)


@tool("resume_fetch", "Takes up a fetch that failed or was stopped, from where it stopped: finished files are kept, "
      "a half-downloaded one goes on.",
      props={"job": prop("string", "The fetch's id (list_fetches)."),
             "url": prop("string", "The share link again, for a fetch that kept no record of it.", maxLength=2000)},
      required=("job",), writes=True, module=MODULE,
      timeout_ms=30000)
def resume_fetch(a: dict) -> dict:
    folder = os.path.join(_root(), _safe(a["job"]))
    if a.get("url") and not os.path.exists(os.path.join(folder, "job.json")) and a["job"] not in _JOBS:
        if not os.path.isdir(folder):
            raise ToolError("no fetch folder %s; list_fetches shows them" % a["job"], "not_found")
        job = {"id": _safe(a["job"]), "provider": _kind(a["url"]), "url": a["url"].strip(), "state": "downloading",
               "bytes": 0, "filesDone": 0, "started": time.time(), "folder": folder, "resumes": 1}
        _start(job)
        return _public(job)
    job = _load_job(a["job"])
    if job.get("state") in ("listing", "downloading", "merging") and a["job"] in _JOBS:
        return _public(job)
    if not job.get("url"):
        raise ToolError("this fetch kept no link to resume from; start it again with fetch_link", "no_url")
    job["state"], job["error"] = "downloading", ""
    job["resumes"] = job.get("resumes", 0) + 1
    _start(job)
    return _public(job)


@tool("fetch_status", "Where a fetch stands: listing, downloading (files done of total, bytes, percent, the current "
      "file, a retry under way), merging, done (the merged tree: sessions, audio files per folder, size) or failed "
      "(why).",
      props={"job": prop("string", "The id fetch_link answered.")}, required=("job",), module=MODULE)
def fetch_status(a: dict) -> dict:
    return _public(_load_job(a["job"]))


@tool("merge_zips", "Merges zips the user already downloaded (e.g. Google Drive's split parts drive-download-...-001.zip, "
      "-002.zip) into ONE folder tree in the add-on's folder, files of the same folder put back together, Finder and "
      "Windows junk left out.",
      props={"files": prop("array", "Absolute paths of the zips (in Downloads, Desktop, Documents...)."),
             "name": prop("string", "A name for the result, e.g. the song.", maxLength=120)},
      required=("files",), writes=True, module=MODULE, timeout_ms=1800000)
def merge_zips(a: dict) -> dict:
    files = [os.path.expanduser(str(f)) for f in a["files"]]
    missing = [f for f in files if not os.path.isfile(f)]
    if missing:
        raise ToolError("not found: %s" % ", ".join(missing), "not_found")
    jid = time.strftime("%Y%m%d-%H%M%S") + "-" + _safe(a.get("name") or "merge")[:40].replace(" ", "_")
    tree = os.path.join(_root(), jid, "tree")
    try:
        stats = _merge_into(tree, sorted(files), "")
    except PermissionError:
        raise ToolError("this add-on may not read those files (only Downloads, Desktop, Documents, Music, Movies)",
                        "no_access")
    return {"id": jid, "stats": stats, **_summary(tree)}


@tool("list_fetches", "The fetches kept in the add-on's folder, newest first: id, state, percent, merged tree, size.",
      module=MODULE)
def list_fetches(a: dict) -> dict:
    out = []
    for jid in sorted(os.listdir(_root()), reverse=True):
        try:
            job = _public(_load_job(jid))
        except ToolError:
            job = {"id": jid, "state": "done" if os.path.isdir(os.path.join(_root(), jid, "tree")) else "unknown"}
        r = job.get("result") or {}
        out.append({"id": jid, "state": job.get("state"), "percent": job.get("percent"), "error": job.get("error", ""),
                    "tree": r.get("tree", ""), "audioFiles": r.get("audioFiles"), "bytes": r.get("bytes"),
                    "onDisk": _du(os.path.join(_root(), jid)), "folder": os.path.join(_root(), jid)})
    return {"fetches": out[:50]}


# ---- looking at a link without downloading ----
@tool("list_link", "Lists what a Google Drive folder link holds now (file names, folders, sizes) WITHOUT downloading "
      "anything. With `compareTo` (a folder on this Mac) each entry says whether it is already there: the file "
      "itself, or - for a zip - the folder it unzips into (same name without .zip).",
      props={"url": prop("string", "The share link.", minLength=8, maxLength=2000),
             "compareTo": prop("string", "Absolute path of a folder to check the entries against.", maxLength=1000)},
      required=("url",), module=MODULE, timeout_ms=120000)
def list_link(a: dict) -> dict:
    url = a["url"].strip()
    if _kind(url) != "drive":
        raise ToolError("list_link reads Google Drive folders only for now", "unsupported")
    kind, fid = _drive_id(url)
    if kind != "folder":
        raise ToolError("this is a single file link, not a folder", "bad_link")
    key = VALUES.get("googleApiKey") or ""
    job = {"provider": "drive"}
    entries = []

    def walk(folder_id: str, rel: str):
        listing = _retrying(job, "listing %s" % (rel or "the folder"),
                            lambda: _drive_list(folder_id, key) if key else _drive_list_public(folder_id))
        for it in listing:
            name = _safe(it["name"])
            if it.get("mimeType") == "application/vnd.google-apps.folder":
                walk(it["id"], os.path.join(rel, name))
            elif not it.get("mimeType", "").startswith("application/vnd.google-apps."):
                entries.append({"path": os.path.join(rel, name), "size": int(it.get("size") or 0) or None})

    walk(fid, "")
    base = os.path.expanduser(a.get("compareTo") or "")
    if base:
        if not os.path.isdir(base):
            raise ToolError("the folder %s is not there" % base, "not_found")
        for e in entries:
            there = os.path.join(base, e["path"])
            stem = there[:-4] if there.lower().endswith(".zip") else ""
            if os.path.isfile(there):
                e["here"] = "file"
            elif stem and (os.path.isdir(stem) or os.path.isdir(os.path.join(base, os.path.basename(stem)))):
                e["here"] = "unzipped"
            else:
                e["here"] = False
    missing = [e["path"] for e in entries if base and not e.get("here")]
    return {"files": len(entries), "entries": entries[:500], "compareTo": base or None,
            "missing": missing if base else None}


# ---- clearing what was already copied ------------------------------------------------------------------------------
def _du(folder: str) -> int:
    total = 0
    for d, _, files in os.walk(folder):
        for fn in files:
            try:
                total += os.path.getsize(os.path.join(d, fn))
            except OSError:
                pass
    return total


def _files_under(base: str) -> dict:
    """{relative path: size} of every real file under `base` (junk and .part left out)."""
    out = {}
    for d, _, files in os.walk(base):
        for fn in files:
            if _junk(fn):
                continue
            p = os.path.join(d, fn)
            out[os.path.relpath(p, base)] = os.path.getsize(p)
    return out


def _sha256(path: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(CHUNK)
            if not b:
                return h.hexdigest()
            h.update(b)


def _compare(src: str, dest: str, files: dict, deep: bool) -> list:
    """The problems found when `files` (relative to src) are looked for under dest: missing, other size, other content."""
    bad = []
    for rel, size in sorted(files.items()):
        there = os.path.join(dest, rel)
        if not os.path.isfile(there):
            bad.append({"file": rel, "problem": "missing"})
        elif os.path.getsize(there) != size:
            bad.append({"file": rel, "problem": "other size (%d here, %d there)" % (size, os.path.getsize(there))})
        elif deep and _sha256(os.path.join(src, rel)) != _sha256(there):
            bad.append({"file": rel, "problem": "other content"})
    return bad


@tool("clear_fetch", "Frees the disk: removes a fetch's downloaded copy from the add-on's own temporary folder, ONLY "
      "once every one of its files is found in the project folder it was copied to (same relative path and size; "
      "`deep` also compares the contents). Nothing outside the add-on's folder is ever touched; with any file "
      "missing or different nothing is removed and the answer lists them. `dryRun` only checks.",
      props={"job": prop("string", "The fetch's id (list_fetches)."),
             "copiedTo": prop("string", "Absolute path of the folder its files were copied into.", maxLength=1000),
             "deep": prop("boolean", "Also compare every file's content (SHA-256): slower, sure. Default false."),
             "dryRun": prop("boolean", "Only check and say what would be removed. Default false.")},
      required=("job", "copiedTo"), writes=True, module=MODULE, timeout_ms=1800000)
def clear_fetch(a: dict) -> dict:
    jid = _safe(a["job"])
    folder = os.path.realpath(os.path.join(_root(), jid))
    root = os.path.realpath(_root())
    if not folder.startswith(root + os.sep) or not os.path.isdir(folder):
        raise ToolError("no fetch %s in the add-on's folder; list_fetches shows them" % a["job"], "not_found")
    job = _JOBS.get(jid)
    if job and job.get("state") in ("listing", "downloading", "merging"):
        raise ToolError("fetch %s is still %s; wait for it to end" % (jid, job["state"]), "busy")
    dest = os.path.realpath(os.path.expanduser(a["copiedTo"]))
    if not os.path.isdir(dest):
        raise ToolError("the folder %s is not there" % a["copiedTo"], "not_found")
    if dest == folder or dest.startswith(folder + os.sep):
        raise ToolError("copiedTo is inside the fetch itself", "bad_folder")
    deep = bool(a.get("deep"))
    # What the fetch holds: its merged tree when the fetch finished (the raw zips are only what the tree came from),
    # else its raw downloads.
    done = (job or _load_job(jid)).get("state") == "done" if os.path.exists(os.path.join(folder, "job.json")) else True
    parts = ("tree",) if done and os.path.isdir(os.path.join(folder, "tree")) else ("raw",)
    sets = [(os.path.join(folder, part), part) for part in parts if os.path.isdir(os.path.join(folder, part))]
    if not sets:
        raise ToolError("fetch %s holds no downloaded files" % jid, "empty")
    checked, report = [], []
    for src, part in sets:
        files = _files_under(src)
        if not files:
            continue
        # The copy may hold the files themselves, or the fetch folder / its tree or raw folder wrapped in one more level.
        cands = [dest, os.path.join(dest, part)]
        for sub in sorted(os.listdir(dest)):
            p = os.path.join(dest, sub)
            if os.path.isdir(p):
                cands += [p, os.path.join(p, part)]
        best = None
        for c in cands:
            if not os.path.isdir(c):
                continue
            try:
                bad = _compare(src, c, files, deep)
            except PermissionError:
                raise ToolError("this add-on may not read %s" % c, "no_access")
            if best is None or len(bad) < len(best[1]):
                best = (c, bad)
            if not bad:
                break
        report.append({"part": part, "files": len(files), "bytes": sum(files.values()),
                       "foundIn": best[0] if best else "", "problems": (best[1] if best else [])[:20],
                       "problemCount": len(best[1]) if best else len(files)})
        checked.append(not best or bool(best[1]))
    ok = bool(report) and not any(checked)
    size = _du(folder)
    out = {"job": jid, "copiedTo": dest, "deep": deep, "verified": ok, "checked": report, "bytesOnDisk": size}
    if not ok:
        out["removed"] = False
        out["why"] = "some files are not in %s as they are here: nothing was removed" % dest
        return out
    if a.get("dryRun"):
        out["removed"] = False
        out["wouldFree"] = size
        return out
    shutil.rmtree(folder)
    with _LOCK:
        _JOBS.pop(jid, None)
    out["removed"] = True
    out["freed"] = size
    return out
