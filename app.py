import os
import json
import pickle
import shlex
import uuid
import subprocess
import threading
import ast
import time
import hashlib
import traceback
from typing import Optional

import pexpect
import requests
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, session, send_from_directory

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev")

# --- Paths and data directories -------------------------------------------------
APP_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("GOGREPO_DATA_DIR", "/data")  # mapped to ./data in Docker
os.makedirs(DATA_DIR, exist_ok=True)

# gogrepo runner configuration
GOGREPO = os.environ.get("GOGREPO_PATH", os.path.join(APP_DIR, "gogrepo.py"))
PY      = os.environ.get("PYTHON_BIN", "python3")

# primary files persisted in DATA_DIR
MANIFEST = os.path.join(DATA_DIR, "gog-manifest.dat")
COOKIES  = os.path.join(DATA_DIR, "gog-cookies.dat")

# --- On-disk cache next to cookies and manifest --------------------------------
CACHE_DIR = os.path.join(DATA_DIR, "Cache")
DESC_DIR  = os.path.join(CACHE_DIR, "desc")   # JSON descriptions
COVER_DIR = os.path.join(CACHE_DIR, "cover")  # binary images
os.makedirs(DESC_DIR, exist_ok=True)
os.makedirs(COVER_DIR, exist_ok=True)

# TTLs (tune as needed)
DAY_MS    = 24 * 60 * 60 * 1000
DESC_TTL  = 7 * DAY_MS    # 7 days for descriptions
COVER_TTL = 30 * DAY_MS   # 30 days for covers

# --- Cache utilities ------------------------------------------------------------
def _now_ms() -> int:
    # Current time in milliseconds
    return int(time.time() * 1000)

def _is_fresh(path: str, ttl_ms: int) -> bool:
    # True if file exists and its mtime is within the TTL window
    try:
        st = os.stat(path)
        return (_now_ms() - int(st.st_mtime * 1000)) < ttl_ms
    except FileNotFoundError:
        return False
    except Exception:
        return False

def _sha256(text: str) -> str:
    # Deterministic key based on SHA-256 of input text
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _desc_cache_path(product_id: str, locale: str) -> str:
    # JSON cache filename for description and images metadata
    key = _sha256(f"product:{product_id}|locale:{locale}")
    return os.path.join(DESC_DIR, f"{key}.json")

def _cover_cache_path_from_url(url: str) -> str:
    # Keep common extension from URL if present; default to .bin otherwise
    base_ext = ".bin"
    for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
        if url.lower().split("?")[0].endswith(ext):
            base_ext = ext
            break
    key = _sha256(url.strip())
    return os.path.join(COVER_DIR, f"{key}{base_ext}")

def _cache_get_json(path: str, ttl_ms: int):
    # Return parsed JSON from path if fresh, else None
    if _is_fresh(path, ttl_ms):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def _cache_put_json(path: str, data) -> None:
    # Persist JSON atomically
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass

def _cache_get_or_fetch_json(product_id: str, locale: str, fetcher):
    # Cache-aside for JSON: try cache, fetch on miss, then store
    path = _desc_cache_path(product_id, locale)
    cached = _cache_get_json(path, DESC_TTL)
    if cached is not None:
        return cached
    data = fetcher()
    _cache_put_json(path, data)
    return data

def _cache_cover_from_url(url: str) -> Optional[str]:
    # Cache-aside for binary cover: return file path if cached and fresh; otherwise fetch and store
    if not url:
        return None
    url = url.strip()
    path = _cover_cache_path_from_url(url)
    if _is_fresh(path, COVER_TTL):
        return path
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(r.content)
        os.replace(tmp, path)
        return path
    except Exception:
        # If a stale file exists, use it as a last resort
        if os.path.exists(path):
            return path
        return None

# --- Async jobs (update/download) with live output and cancel -------------------
class Job:
    # Represents a long-running process with streamed output
    def __init__(self):
        self.status = "running"          # running | finished | error | canceled
        self.output = ""                 # accumulated stdout/stderr
        self.rc: Optional[int] = None    # return code when finished
        self.lock = threading.Lock()
        self.proc: Optional[subprocess.Popen] = None

    def append(self, text: str):
        # Append text to job output in a thread-safe way
        with self.lock:
            self.output += text

    def finish(self, rc: int, status: Optional[str] = None):
        # Mark job as finished with rc and optional explicit status
        with self.lock:
            self.rc = rc
            if status:
                self.status = status
            else:
                self.status = "finished" if rc == 0 else "error"

jobs = {}  # job_id -> Job
_current_job_id = None
_current_job_lock = threading.Lock()

def _run_stream(job_id, args, cwd=None):
    # Spawn a subprocess, stream its output line by line, and update job state
    global _current_job_id
    job = jobs[job_id]
    try:
        # Echo exact command line to the UI for easier debugging
        job.append("$ " + " ".join(shlex.quote(a) for a in args) + "\n")

        # Ensure unbuffered Python to stream logs immediately
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        proc = subprocess.Popen(
            args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env
        )
        job.proc = proc
        for line in proc.stdout:
            job.append(line)
        rc = proc.wait()
        if job.status == "running":
            job.finish(rc)
    except Exception as e:
        job.append(f"\n[ERROR] {e}\n{traceback.format_exc()}\n")
        job.finish(1)
    finally:
        with _current_job_lock:
            if _current_job_id == job_id:
                _current_job_id = None

def start_job(args, cwd=None) -> str:
    # Register and start a background thread that runs the subprocess
    global _current_job_id
    job_id = str(uuid.uuid4())
    jobs[job_id] = Job()
    with _current_job_lock:
        _current_job_id = job_id
    t = threading.Thread(target=_run_stream, args=(job_id, args, cwd), daemon=True)
    t.start()
    return job_id

def cancel_job(job_id: Optional[str]) -> tuple[bool, str]:
    # Try to gracefully terminate the running process, then kill if needed
    job = jobs.get(job_id or "")
    if not job or job.status != "running" or not job.proc:
        return False, "No running job"
    try:
        job.append("\n[INFO] Cancel requested, terminating process...\n")
        job.proc.terminate()
        try:
            job.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            job.append("[INFO] Process did not terminate, killing...\n")
            job.proc.kill()
            job.proc.wait(timeout=5)
        job.finish(-9, status="canceled")
        return True, "Canceled"
    except Exception as e:
        job.append(f"[ERROR] Cancel failed: {e}\n")
        return False, str(e)

# --- Manifest parsing helpers ---------------------------------------------------
def _extract_games_from_obj(data):
    # Normalize different manifest shapes into list of games with title/long_title/product_id
    if isinstance(data, dict):
        if isinstance(data.get("products"), dict):
            source = list(data["products"].values())
        elif "games" in data:
            source = list(data["games"].values()) if isinstance(data["games"], dict) else data["games"]
        else:
            source = [v for v in data.values() if isinstance(v, dict)]
    elif isinstance(data, list):
        source = data
    else:
        source = []

    out, seen = [], set()
    for g in source:
        if not isinstance(g, dict):
            continue
        slug = (g.get("title") or g.get("slug") or "").strip()
        nice = (g.get("long_title") or slug).strip()
        pid  = g.get("product_id") or g.get("productId") or g.get("productid") or g.get("id")
        if slug and slug.lower() not in seen:
            seen.add(slug.lower())
            out.append({"title": slug, "long_title": nice, "product_id": pid})
    out.sort(key=lambda x: x["long_title"].lower())
    return out

def _load_manifest_raw():
    # Try pickle, then JSON, then Python literal as a last resort for older manifests
    try:
        with open(MANIFEST, "rb") as f:
            return pickle.load(f)
    except Exception:
        pass
    try:
        with open(MANIFEST, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        pass
    try:
        with open(MANIFEST, "r", encoding="utf-8", errors="ignore") as f:
            return ast.literal_eval(f.read())
    except Exception:
        return None

def load_manifest_games():
    # Public helper for the UI to list games from manifest
    raw = _load_manifest_raw()
    return _extract_games_from_obj(raw) if raw is not None else []

# --- Image and URL helpers ------------------------------------------------------
def _abs_url(u: str) -> str:
    # Normalize GOG/relative/ schemeless URLs into absolute https
    if not u:
        return ""
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://www.gog.com" + u
    if not u.lower().startswith("http"):
        return "https://" + u
    return u

def _pick_from_dict(d: dict, keys: list[str]) -> str:
    # Return first non-empty value for keys in a dict
    for k in keys:
        v = d.get(k)
        if v:
            return v
    return ""

def _extract_url_from_value(v) -> str:
    # Extract url-like value from either a string or a nested dict
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return _pick_from_dict(v, ["image_url", "url", "href", "src", "original"]) or ""
    return ""

def _get_image_from_images(images) -> str:
    # Heuristics to find a primary image URL among multiple shapes
    if isinstance(images, dict):
        for key in ["vertical", "boxArtImage", "box_art_image", "logo", "background", "square", "tileImage", "tile_image", "cover", "image"]:
            if key in images:
                url = _extract_url_from_value(images.get(key))
                if url:
                    return url
        url = _pick_from_dict(images, ["image_url", "url", "href", "src", "original"])
        if url:
            return url
        for v in images.values():
            url = _extract_url_from_value(v)
            if url:
                return url
    elif isinstance(images, list):
        for item in images:
            url = _extract_url_from_value(item)
            if url:
                return url
    return ""

def _find_game_raw_by_title(slug: str):
    # Find a game dict by title in the raw manifest data
    data = _load_manifest_raw()
    if data is None:
        return None
    if isinstance(data, dict):
        if isinstance(data.get("products"), dict):
            items = list(data["products"].values())
        elif "games" in data:
            items = list(data["games"].values()) if isinstance(data["games"], dict) else data["games"]
        else:
            items = [v for v in data.values() if isinstance(v, dict)]
    elif isinstance(data, list):
        items = data
    else:
        items = []
    for g in items:
        if isinstance(g, dict) and (g.get("title") or "").strip() == slug:
            return g
    return None

# --- UI -------------------------------------------------------------------------
login_children = {}  # token -> pexpect child waiting for 2FA

@app.route("/")
def index():
    # Render main page with status (cookies/manifest/2FA) and games list
    status = {
        "cookies": os.path.exists(COOKIES),
        "manifest": os.path.exists(MANIFEST),
        "need_2fa": session.pop("need_2fa", False),
        "login_token": session.get("login_token"),
    }
    games = load_manifest_games()
    return render_template("index.html", status=status, games=games)

# --- Two-step login with 2FA ----------------------------------------------------
@app.route("/login", methods=["POST"])
def login():
    # Handle both initial login and the second step (OTP submission)
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    otp      = (request.form.get("otp") or "").strip()
    token    = (request.form.get("login_token") or "").strip()

    if token:
        # Second step: deliver OTP to the spawned gogrepo login process
        child = login_children.get(token)
        if not child:
            flash("Login session expired — start again.", "error")
            return redirect(url_for("index"))
        try:
            child.sendline(otp)
            child.expect(pexpect.EOF, timeout=240)
            flash(child.before, "info")
        except Exception as e:
            flash(f"2FA login error: {e}", "error")
        finally:
            try:
                child.close(force=True)
            except Exception:
                pass
            login_children.pop(token, None)
            session.pop("login_token", None)
        return redirect(url_for("index"))

    # First step: start gogrepo login and wait for 2FA prompt
    cmd = f"{shlex.quote(PY)} {shlex.quote(GOGREPO)} login"
    try:
        child = pexpect.spawn(cmd, cwd=DATA_DIR, encoding="utf-8", timeout=300)
        child.expect(["[Uu]sername", "enter username", "Enter username"], timeout=120)
        child.sendline(username)
        child.expect(["[Pp]assword", "enter password", "Enter password"], timeout=120)
        child.sendline(password)
        idx = child.expect([
            "Enter the code from your authenticator",
            "Enter the security code",
            "Enter the code sent to your email",
            "2FA code",
            "Two-Factor Code",
            pexpect.EOF
        ], timeout=240)

        if idx == 5:
            # Logged in without 2FA prompt
            flash(child.before, "info")
            try:
                child.close(force=True)
            except Exception:
                pass
            return redirect(url_for("index"))

        # 2FA step required: keep the child open and store a token in session
        token = str(uuid.uuid4())
        login_children[token] = child
        session["need_2fa"] = True
        session["login_token"] = token
        flash("Enter 2FA code from email/app — login process is waiting for your code.", "info")
        return redirect(url_for("index"))

    except pexpect.TIMEOUT:
        flash("Timeout during login", "error")
    except Exception as e:
        flash(f"Login error: {e}", "error")
    return redirect(url_for("index"))

# --- UPDATE job endpoints -------------------------------------------------------
@app.route("/run_update", methods=["POST"])
def run_update():
    # Start gogrepo update with selected OS/lang options
    os_list = [v for v in request.form.getlist("os") if v.strip()]
    langs   = [v for v in (request.form.get("langs") or "").strip().split() if v.strip()]
    args = [PY, GOGREPO, "update"]
    if os_list:
        args += ["-os"] + os_list
    if langs:
        args += ["-lang"] + langs
    if request.form.get("skipknown"):
        args.append("-skipknown")
    if request.form.get("updateonly"):
        args.append("-updateonly")
    job_id = start_job(args, cwd=DATA_DIR)
    return jsonify({"job_id": job_id})

@app.route("/job_status/<job_id>")
def job_status(job_id):
    # Return current status and streamed output for a given job
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "unknown", "output": "", "rc": None})
    with job.lock:
        return jsonify({"status": job.status, "output": job.output, "rc": job.rc})

@app.route("/current_job")
def current_job():
    # Resume UI polling after reload: return a running job if any
    with _current_job_lock:
        jid = _current_job_id
    if not jid:
        for k, j in jobs.items():
            with j.lock:
                if j.status == "running":
                    jid = k
                    break
    if not jid or jid not in jobs:
        return jsonify({"job_id": None, "status": "idle", "output": "", "rc": None})
    j = jobs[jid]
    with j.lock:
        return jsonify({"job_id": jid, "status": j.status, "output": j.output, "rc": j.rc})

@app.route("/cancel_job", methods=["POST"])
def cancel_job_endpoint():
    # Cancel the active job (either provided or the last started)
    job_id = (request.form.get("job_id") or None)
    if not job_id:
        with _current_job_lock:
            job_id = _current_job_id
    ok, msg = cancel_job(job_id)
    return jsonify({"ok": ok, "message": msg, "job_id": job_id})

# --- DOWNLOAD job endpoints (with error handling) -------------------------------
@app.route("/download_selected", methods=["POST"])
def download_selected():
    # Start download for a single selected title; always return JSON
    try:
        title = (request.form.get("selected_title") or "").strip()
        if not title:
            return jsonify({"error": "Select a game from the list"}), 400
        args = [PY, GOGREPO, "download", "-id", title]
        if request.form.get("skipextras"):
            args.append("-skipextras")
        if request.form.get("skipgames"):
            args.append("-skipgames")
        job_id = start_job(args, cwd=DATA_DIR)
        return jsonify({"job_id": job_id})
    except Exception as e:
        app.logger.exception("download_selected failed")
        return jsonify({"error": str(e)}), 500

@app.route("/download_all", methods=["POST"])
def download_all():
    # Start download for the entire library; always return JSON
    try:
        args = [PY, GOGREPO, "download"]
        if request.form.get("skipextras"):
            args.append("-skipextras")
        if request.form.get("skipgames"):
            args.append("-skipgames")
        job_id = start_job(args, cwd=DATA_DIR)
        return jsonify({"job_id": job_id})
    except Exception as e:
        app.logger.exception("download_all failed")
        return jsonify({"error": str(e)}), 500

# --- Serve cached cover files ---------------------------------------------------
@app.route("/cache/cover/<path:name>")
def serve_cover(name: str):
    # Expose cached cover images via a local URL for the UI
    return send_from_directory(COVER_DIR, name)

# --- Product info (GOG API with cache + manifest fallback) ----------------------
def _fetch_product_details_raw(product_id, locale="en-US"):
    # Raw HTTP call to GOG products API with expanded description & images
    url = f"https://api.gog.com/products/{product_id}"
    params = {"expand": "description,images", "locale": locale}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_product_details(product_id, locale="en-US"):
    # Cached JSON fetcher (cache-aside)
    return _cache_get_or_fetch_json(
        str(product_id),
        locale,
        lambda: _fetch_product_details_raw(product_id, locale)
    )

@app.route("/game_info")
def game_info():
    # Return description (HTML) and a cover URL for the selected game
    pid   = (request.args.get("product_id") or "").strip()
    title = (request.args.get("title") or "").strip()
    info = {"title": title, "description_html": "", "cover_url": ""}

    # Prefer GOG API (with JSON cache) for both description and images
    if pid:
        try:
            data = fetch_product_details(pid)
            desc = (data.get("description", {}) or {}).get("full") or ""
            images = data.get("images", {}) or {}
            cover = _get_image_from_images(images) or _extract_url_from_value(data.get("image"))
            if desc:
                info["description_html"] = desc
            if cover:
                cover_abs = _abs_url(cover)
                cached_path = _cache_cover_from_url(cover_abs)
                if cached_path:
                    info["cover_url"] = url_for("serve_cover", name=os.path.basename(cached_path))
                else:
                    info["cover_url"] = cover_abs
        except Exception:
            # Silent fallback — UI still shows manifest-based data below
            pass

    # Fallback: try to get a cover from the manifest record when GOG API fails
    if not info["cover_url"] and title:
        g = _find_game_raw_by_title(title)
        if isinstance(g, dict):
            cover = g.get("bg_url") or g.get("image") or g.get("logo") or ""
            cover = _extract_url_from_value(cover)
            cover_abs = _abs_url(cover)
            cached_path = _cache_cover_from_url(cover_abs) if cover_abs else None
            if cached_path:
                info["cover_url"] = url_for("serve_cover", name=os.path.basename(cached_path))
            elif cover_abs:
                info["cover_url"] = cover_abs

    return jsonify(info)

# --- App entry ------------------------------------------------------------------
if __name__ == "__main__":
    # Bind to 0.0.0.0 for Docker, default port 8080
    app.run(host="0.0.0.0", port=8080)
