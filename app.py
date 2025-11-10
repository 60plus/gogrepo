import os
import json
import pickle
import shlex
import uuid
import subprocess
import threading
import ast

import pexpect
import requests
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, session

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev")

APP_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("GOGREPO_DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

GOGREPO = os.environ.get("GOGREPO_PATH", os.path.join(APP_DIR, "gogrepo.py"))
PY      = os.environ.get("PYTHON_BIN", "python3")

MANIFEST = os.path.join(DATA_DIR, "gog-manifest.dat")
COOKIES  = os.path.join(DATA_DIR, "gog-cookies.dat")

# ---------------- Jobs (asynchroniczne Update/Download + cancel + resume) ----------------
class Job:
    def __init__(self):
        self.status = "running"
        self.output = ""
        self.rc = None
        self.lock = threading.Lock()
        self.proc: subprocess.Popen | None = None

    def append(self, text: str):
        with self.lock:
            self.output += text

    def finish(self, rc: int, status: str | None = None):
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
    global _current_job_id
    job = jobs[job_id]
    try:
        proc = subprocess.Popen(
            args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )
        job.proc = proc
        for line in proc.stdout:
            job.append(line)
        rc = proc.wait()
        if job.status == "running":
            job.finish(rc)
    except Exception as e:
        job.append(f"\n[ERROR] {e}\n")
        job.finish(1)
    finally:
        with _current_job_lock:
            if _current_job_id == job_id:
                _current_job_id = None

def start_job(args, cwd=None) -> str:
    global _current_job_id
    job_id = str(uuid.uuid4())
    jobs[job_id] = Job()
    with _current_job_lock:
        _current_job_id = job_id
    t = threading.Thread(target=_run_stream, args=(job_id, args, cwd), daemon=True)
    t.start()
    return job_id

def cancel_job(job_id: str | None) -> tuple[bool, str]:
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

def run_cmd(args, cwd=None, timeout=None):
    proc = subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        return 124, "Command timed out"
    return proc.returncode, out

# ---------------- Manifest parsing + raw access ----------------
def _extract_games_from_obj(data):
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
        slug = (g.get("title") or g.get("slug") or "").trim() if hasattr(str, "trim") else (g.get("title") or g.get("slug") or "").strip()
        nice = (g.get("long_title") or slug).strip()
        pid  = g.get("product_id") or g.get("productId") or g.get("productid") or g.get("id")
        if slug and slug.lower() not in seen:
            seen.add(slug.lower())
            out.append({"title": slug, "long_title": nice, "product_id": pid})
    out.sort(key=lambda x: x["long_title"].lower())
    return out

def _load_manifest_raw():
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
    raw = _load_manifest_raw()
    return _extract_games_from_obj(raw) if raw is not None else []

# ---------------- Cover helpers ----------------
def _abs_url(u: str) -> str:
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
    for k in keys:
        v = d.get(k)
        if v:
            return v
    return ""

def _extract_url_from_value(v) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return _pick_from_dict(v, ["image_url", "url", "href", "src", "original"]) or ""
    return ""

def _get_image_from_images(images) -> str:
    # images may be dict with many keys, or list of dicts/strings
    if isinstance(images, dict):
        # preference order
        for key in ["vertical", "boxArtImage", "box_art_image", "logo", "background", "square", "tileImage", "tile_image", "cover", "image"]:
            if key in images:
                url = _extract_url_from_value(images.get(key))
                if url:
                    return url
        # direct URL fields at top-level
        url = _pick_from_dict(images, ["image_url", "url", "href", "src", "original"])
        if url:
            return url
        # scan nested dicts
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

# ---------------- UI ----------------
login_children = {}  # token -> pexpect child (czeka na 2FA)

@app.route("/")
def index():
    status = {
        "cookies": os.path.exists(COOKIES),
        "manifest": os.path.exists(MANIFEST),
        "need_2fa": session.pop("need_2fa", False),
        "login_token": session.get("login_token"),
    }
    games = load_manifest_games()
    return render_template("index.html", status=status, games=games)

# ---------------- Login 2‑etapowy ----------------
@app.route("/login", methods=["POST"])
def login():
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    otp      = (request.form.get("otp") or "").strip()
    token    = (request.form.get("login_token") or "").strip()

    if token:
        child = login_children.get(token)
        if not child:
            flash("Sesja logowania wygasła — uruchom ponownie.", "error")
            return redirect(url_for("index"))
        try:
            child.sendline(otp)
            child.expect(pexpect.EOF, timeout=240)
            flash(child.before, "info")
        except Exception as e:
            flash(f"Błąd logowania 2FA: {e}", "error")
        finally:
            try:
                child.close(force=True)
            except Exception:
                pass
            login_children.pop(token, None)
            session.pop("login_token", None)
        return redirect(url_for("index"))

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
            flash(child.before, "info")
            try:
                child.close(force=True)
            except Exception:
                pass
            return redirect(url_for("index"))

        token = str(uuid.uuid4())
        login_children[token] = child
        session["need_2fa"] = True
        session["login_token"] = token
        flash("Podaj kod 2FA z maila/aplikacji — proces logowania czeka na wpisanie kodu.", "info")
        return redirect(url_for("index"))

    except pexpect.TIMEOUT:
        flash("Timeout podczas logowania", "error")
    except Exception as e:
        flash(f"Błąd logowania: {e}", "error")
    return redirect(url_for("index"))

# ---------------- UPDATE ----------------
@app.route("/run_update", methods=["POST"])
def run_update():
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
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "unknown", "output": "", "rc": None})
    with job.lock:
        return jsonify({"status": job.status, "output": job.output, "rc": job.rc})

# Nowy endpoint do wznawiania postępu po reloadzie
@app.route("/current_job")
def current_job():
    with _current_job_lock:
        jid = _current_job_id
    # jeśli nie ma wskaźnika, spróbuj znaleźć dowolny 'running'
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
    job_id = (request.form.get("job_id") or None)
    if not job_id:
        with _current_job_lock:
            job_id = _current_job_id
    ok, msg = cancel_job(job_id)
    return jsonify({"ok": ok, "message": msg, "job_id": job_id})

# ---------------- DOWNLOAD (as job) ----------------
@app.route("/download_selected", methods=["POST"])
def download_selected():
    title = (request.form.get("selected_title") or "").strip()
    if not title:
        return jsonify({"error": "Wybierz grę z listy"}), 400
    args = [PY, GOGREPO, "download", "-id", title]
    if request.form.get("skipextras"):
        args.append("-skipextras")
    if request.form.get("skipgames"):
        args.append("-skipgames")
    job_id = start_job(args, cwd=DATA_DIR)
    return jsonify({"job_id": job_id})

@app.route("/download_all", methods=["POST"])
def download_all():
    args = [PY, GOGREPO, "download"]
    if request.form.get("skipextras"):
        args.append("-skipextras")
    if request.form.get("skipgames"):
        args.append("-skipgames")
    job_id = start_job(args, cwd=DATA_DIR)
    return jsonify({"job_id": job_id})

# ---------------- INFO (opis/okładka z GOG + fallback z manifestu) ----------------
def fetch_product_details(product_id, locale="en-US"):
    url = f"https://api.gog.com/products/{product_id}"
    params = {"expand": "description,images", "locale": locale}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

@app.route("/game_info")
def game_info():
    pid   = (request.args.get("product_id") or "").strip()
    title = (request.args.get("title") or "").strip()
    info = {"title": title, "description_html": "", "cover_url": ""}
    if pid:
        try:
            data = fetch_product_details(pid)
            desc = (data.get("description", {}) or {}).get("full") or ""
            images = data.get("images", {}) or {}
            cover = _get_image_from_images(images) or _extract_url_from_value(data.get("image"))
            info["description_html"] = desc or info["description_html"]
            info["cover_url"] = _abs_url(cover) or info["cover_url"]
        except Exception:
            pass
    if not info["cover_url"] and title:
        g = _find_game_raw_by_title(title)
        if isinstance(g, dict):
            cover = g.get("bg_url") or g.get("image") or g.get("logo") or ""
            cover = _extract_url_from_value(cover)
            info["cover_url"] = _abs_url(cover) or info["cover_url"]
    return jsonify(info)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
