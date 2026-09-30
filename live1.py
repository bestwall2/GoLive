import subprocess, threading, os, time, logging, json, sys, signal, re
from collections import deque
import requests
from flask import Flask, request, jsonify, Response

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger(__name__)

app = Flask(__name__)

MAX_CHANNELS = 10

# ── Graph API ──
GRAPH_API_VERSION = os.environ.get("FB_GRAPH_VERSION", "v26.0")
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

# ── Pipeline tuning ──
ERROR_WINDOW_SECONDS = int(os.environ.get("RELAY_ERROR_WINDOW", "60"))
ERROR_THRESHOLD = int(os.environ.get("RELAY_ERROR_THRESHOLD", "30"))
STARTUP_GRACE_SECONDS = int(os.environ.get("RELAY_STARTUP_GRACE", "60"))
BAD_LINE_RE = re.compile(
    r"(timestamp discontinuity|Stream ends prematurely)",
    re.IGNORECASE,
)

# Facebook needs time to release a stream key after a disconnect on the SAME
# live video before it accepts a new connection on it. Applies to crash
# retries and proactive drift restarts, which reconnect to the same video.
RECONNECT_DELAY = int(os.environ.get("RELAY_RECONNECT_DELAY", "35"))

# A brand-new live video is a different resource, so it doesn't need the
# full 35s — just enough for the previous pipeline to fully exit.
NEW_VIDEO_SETTLE_SECONDS = int(os.environ.get("RELAY_NEW_VIDEO_SETTLE", "8"))

# Facebook stream keys cap at 4h of cumulative active streaming. Rotate to a
# brand-new live video (fresh key) well before that, so it never has to stop.
RENEW_SECONDS = int(os.environ.get("RELAY_RENEW_SECONDS", str(3 * 3600 + 55 * 60)))  # 3h55m

CONTROL_SECRET = os.environ.get("CONTROL_SECRET", "changeme")

# Channel registry — everything (including the token) lives here, entered
# through the dashboard. Nothing is ever hardcoded in this file.
# { id: { name, access_token, page_id, input_url, running, process,
#         live_video_id, stream_url, video_created_at, status, restarts } }
channels = {}
channels_lock = threading.Lock()

# ─────────────────────────────────────────
# Graph API helpers
# ─────────────────────────────────────────

def _graph_request(method, path, access_token, **params):
    params = {k: v for k, v in params.items() if v is not None}
    params["access_token"] = access_token
    url = f"{GRAPH_BASE}/{path}"
    r = requests.request(method, url, params=params, timeout=15)
    try:
        data = r.json()
    except ValueError:
        raise RuntimeError(f"Graph API returned non-JSON (status {r.status_code}): {r.text[:200]}")
    if r.status_code >= 400 or "error" in data:
        raise RuntimeError(f"Graph API error: {data.get('error', data)}")
    return data


# Keep the broadcast hidden: UNPUBLISHED (not LIVE_NOW) means it is not
# visible on the Page/profile timeline. dash_preview_url is meant for
# previewing an UNPUBLISHED / SCHEDULED_UNPUBLISHED broadcast like this.
LIVE_VIDEO_STATUS = os.environ.get("FB_LIVE_STATUS", "UNPUBLISHED")


def create_live_video(access_token, page_id, title=None):
    params = {
        "status": LIVE_VIDEO_STATUS,
    }
    if title:
        params["title"] = title[:100]

    return _graph_request("POST", f"{page_id}/live_videos", access_token, **params)


def end_live_video(access_token, live_video_id):
    return _graph_request("POST", f"{live_video_id}", access_token, end_live_video="true")


def safe_end_live_video(access_token, live_video_id):
    if not live_video_id:
        return
    try:
        end_live_video(access_token, live_video_id)
        log.info(f"Ended live video {live_video_id}")
    except Exception as e:
        log.warning(f"Could not end live video {live_video_id} (may have already ended): {e}")


def get_dash_preview(access_token, live_video_id):
    return _graph_request(
        "GET", f"{live_video_id}", access_token,
        fields="dash_preview_url,status,secure_stream_url",
    )

# ─────────────────────────────────────────
# Pipeline (cross-platform: no bash, no process groups)
# ─────────────────────────────────────────

class Pipeline:
    """ffmpeg (source -> mpegts pipe) -> ffmpeg (mpegts -> flv to Facebook).
    Both processes' stderr are merged into one readable stream (self.stdout)."""

    def __init__(self, input_url, output_url):
        cmd1 = [
            "ffmpeg",
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5", "-reconnect_at_eof", "1",
            "-reconnect_on_network_error", "1",
            "-timeout", "10000000", "-thread_queue_size", "1024",
            "-i", input_url,
            "-fflags", "+genpts+discardcorrupt+igndts", "-err_detect", "ignore_err",
            "-c", "copy", "-f", "mpegts", "-",
        ]
        cmd2 = [
            "ffmpeg", "-re", "-f", "mpegts", "-i", "-",
            "-max_muxing_queue_size", "1024", "-max_interleave_delta", "0",
            "-c:v", "copy", "-vsync", "passthrough",
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2",
            "-af", "aresample=async=1:min_hard_comp=0.100000:first_pts=0",
            "-avoid_negative_ts", "make_zero",
            "-f", "flv", output_url,
        ]

        # shared pipe so stderr of BOTH ffmpeg processes lands in one stream
        r_fd, w_fd = os.pipe()
        self.p1 = None
        self.p2 = None
        try:
            self.p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=w_fd)
            self.p2 = subprocess.Popen(cmd2, stdin=self.p1.stdout,
                                       stdout=w_fd, stderr=w_fd)
        except Exception:
            os.close(r_fd)
            os.close(w_fd)
            if self.p1 and self.p1.poll() is None:
                self.p1.kill()
            raise
        os.close(w_fd)
        self.p1.stdout.close()
        self.stdout = os.fdopen(r_fd, "rb")
        self.procs = [self.p1, self.p2]

    @property
    def pid(self):
        return self.p2.pid

    @property
    def returncode(self):
        return self.p2.returncode

    def poll(self):
        return self.p2.poll()

    def wait(self, timeout=None):
        r = self.p2.wait(timeout=timeout)
        if self.p1.poll() is None:
            self.p1.kill()
        self.p1.wait()
        return r


def kill_process_group(proc, timeout=5):
    if proc is None:
        return
    procs = proc.procs if isinstance(proc, Pipeline) else [proc]
    for p in procs:
        if p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass
    for p in procs:
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                p.kill()
                p.wait(timeout=5)
            except Exception:
                pass


def clean_token_field(raw: str) -> str:
    """Strip whitespace/newlines that sneak in when copy-pasting a token."""
    return (raw or "").strip()

# ─────────────────────────────────────────
# Channel supervisor
# ─────────────────────────────────────────

def _set(channel_id, **kwargs):
    with channels_lock:
        c = channels.get(channel_id)
        if c:
            c.update(kwargs)


def _is_running(channel_id):
    with channels_lock:
        c = channels.get(channel_id)
        return bool(c and c["running"])


def run_channel(channel_id):
    while True:
        with channels_lock:
            c = channels.get(channel_id)
            if not c or not c["running"]:
                return
            access_token = c["access_token"]
            page_id = c["page_id"]
            input_url = c["input_url"]
            name = c["name"]

        # ── create a fresh live video for this ~4h cycle ──
        log.info(f"[{channel_id}] Creating live video via Graph API...")
        _set(channel_id, status="creating_video")
        try:
            lv = create_live_video(access_token, page_id, title=name)
        except Exception as e:
            log.error(f"[{channel_id}] Failed to create live video: {e}")
            _set(channel_id, status="error")
            for _ in range(RECONNECT_DELAY):
                if not _is_running(channel_id):
                    return
                time.sleep(1)
            continue

        live_video_id = lv["id"]
        stream_url = lv.get("secure_stream_url") or lv.get("stream_url")
        video_created_at = time.time()
        log.info(f"[{channel_id}] Live video created: id={live_video_id}")

        if not _is_running(channel_id):
            safe_end_live_video(access_token, live_video_id)
            return

        _set(channel_id, live_video_id=live_video_id, stream_url=stream_url,
             video_created_at=video_created_at, status="waiting")

        time.sleep(NEW_VIDEO_SETTLE_SECONDS)

        reason = "stopped"

        # ── inner loop: push to THIS live video, reconnecting on crash/drift,
        #    until it's time to renew, the user stops it, or it fails outright ──
        while True:
            if not _is_running(channel_id):
                reason = "stopped"
                break

            try:
                proc = Pipeline(input_url, stream_url)
            except Exception as e:
                log.error(f"[{channel_id}] FAILED TO LAUNCH pipeline: {e}")
                reason = "fatal"
                break

            _set(channel_id, process=proc, status="running")

            output_chunks = deque(maxlen=60)
            error_times = deque()
            force_restart = threading.Event()
            session_start = time.time()

            def _read_output(proc=proc, output_chunks=output_chunks,
                             error_times=error_times, force_restart=force_restart,
                             session_start=session_start):
                for raw_line in iter(proc.stdout.readline, b''):
                    if not raw_line:
                        break
                    output_chunks.append(raw_line)
                    line = raw_line.decode(errors='replace').rstrip('\n')
                    if not line:
                        continue
                    now = time.time()
                    if now - session_start < STARTUP_GRACE_SECONDS:
                        continue
                    if BAD_LINE_RE.search(line):
                        error_times.append(now)
                        cutoff = now - ERROR_WINDOW_SECONDS
                        while error_times and error_times[0] < cutoff:
                            error_times.popleft()
                        if len(error_times) >= ERROR_THRESHOLD:
                            log.warning(
                                f"[{channel_id}] Error rate too high "
                                f"({len(error_times)}/{ERROR_WINDOW_SECONDS}s) — "
                                f"forcing reconnect to reset drift"
                            )
                            force_restart.set()
                            return
                # keep draining so ffmpeg never blocks on a full stderr pipe
                for _ in iter(proc.stdout.readline, b''):
                    pass

            reader = threading.Thread(target=_read_output, daemon=True)
            reader.start()

            renew_due = False
            while proc.poll() is None:
                time.sleep(1)

                if not _is_running(channel_id):
                    kill_process_group(proc)
                    reason = "stopped"
                    break

                if time.time() - video_created_at >= RENEW_SECONDS:
                    log.info(f"[{channel_id}] Renewal window reached — rotating to a new live video")
                    kill_process_group(proc)
                    renew_due = True
                    break

                if force_restart.is_set():
                    kill_process_group(proc)
                    break  # falls through to the same-video reconnect wait below

            proc.wait()
            reader.join(timeout=5)
            try:
                proc.stdout.close()
            except Exception:
                pass
            output_tail = b''.join(output_chunks)
            exit_code = proc.returncode if proc.returncode is not None else -1

            if renew_due:
                reason = "renew"
                break

            if not _is_running(channel_id):
                reason = "stopped"
                break

            with channels_lock:
                c = channels.get(channel_id)
                if c:
                    c["restarts"] = c.get("restarts", 0) + 1
                    restart_count = c["restarts"]
                else:
                    restart_count = 0
            _set(channel_id, status="restarting")
            log.warning(f"[{channel_id}] Pipeline ended (code {exit_code}), retry #{restart_count} "
                        f"on same live video after {RECONNECT_DELAY}s")
            log.warning(f"[{channel_id}] output tail:\n{output_tail[-1500:].decode(errors='replace')}")

            stopped_during_wait = False
            for _ in range(RECONNECT_DELAY):
                if not _is_running(channel_id):
                    stopped_during_wait = True
                    break
                time.sleep(1)
            if stopped_during_wait:
                reason = "stopped"
                break
            # loop back: relaunch on the SAME stream_url

        # ── end this cycle's live video ──
        safe_end_live_video(access_token, live_video_id)

        if reason in ("stopped", "fatal"):
            _set(channel_id, status="stopped" if reason == "stopped" else "error")
            return

        # reason == "renew": loop back to the top and create the next live video
        log.info(f"[{channel_id}] Starting next cycle with a fresh live video")

# ─────────────────────────────────────────
# API
# ─────────────────────────────────────────

def reply(payload, status=200):
    return jsonify(payload), status


@app.route('/api', methods=['GET', 'POST'])
def api():
    if request.values.get('secret') != CONTROL_SECRET:
        return reply({"ok": 0, "e": "unauthorized"}, 401)

    action = request.values.get('action', '')

    if action == 'list_full':
        with channels_lock:
            data = []
            for cid, c in channels.items():
                data.append({
                    "i": cid,
                    "n": c["name"],
                    "st": c["status"],
                    "run": int(c["running"]),
                    "rc": c.get("restarts", 0),
                    "input_url": c["input_url"],
                    "page_id": c["page_id"],
                    "token_tail": c["access_token"][-6:] if c.get("access_token") else "",
                    "live_video_id": c.get("live_video_id"),
                    "video_age": int(time.time() - c["video_created_at"]) if c.get("video_created_at") else 0,
                })
        return reply({"ok": 1, "s": data})

    if action == 'add':
        name = request.values.get('name', '').strip()
        access_token = clean_token_field(request.values.get('access_token', ''))
        page_id = request.values.get('page_id', '').strip() or "me"
        input_url = request.values.get('input_url', '').strip()

        if not name or not access_token or not input_url:
            return reply({"ok": 0, "e": "missing_fields"}, 400)

        with channels_lock:
            if len(channels) >= MAX_CHANNELS:
                return reply({"ok": 0, "e": "max_channels"}, 400)
            existing_ids = set(channels.keys())
            cid = next(str(i) for i in range(1, MAX_CHANNELS + 1) if str(i) not in existing_ids)
            channels[cid] = {
                "name": name, "access_token": access_token, "page_id": page_id,
                "input_url": input_url, "running": False, "process": None,
                "live_video_id": None, "stream_url": None, "video_created_at": None,
                "status": "stopped", "restarts": 0,
            }
        log.info(f"Channel added: id={cid} name={name}")
        return reply({"ok": 1, "i": cid}, 201)

    if action == 'delete':
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            c["running"] = False
            proc = c["process"]
            access_token = c["access_token"]
            live_video_id = c.get("live_video_id")
            del channels[cid]
        if proc and proc.poll() is None:
            kill_process_group(proc)
        safe_end_live_video(access_token, live_video_id)
        log.info(f"Channel removed: id={cid}")
        return reply({"ok": 1})

    if action == 'start':
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            if c["running"]:
                return reply({"ok": 1, "st": "already"})
            c["running"] = True
            c["restarts"] = 0
            c["status"] = "starting"
        t = threading.Thread(target=run_channel, args=(cid,), daemon=True)
        t.start()
        log.info(f"Channel start requested: id={cid}")
        return reply({"ok": 1})

    if action == 'stop':
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            c["running"] = False
            c["status"] = "stopping"
            proc = c["process"]
        if proc and proc.poll() is None:
            kill_process_group(proc)
        log.info(f"Channel stop requested: id={cid}")
        return reply({"ok": 1})

    if action == 'dash':
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            access_token = c["access_token"]
            live_video_id = c.get("live_video_id")
        if not live_video_id:
            return reply({"ok": 0, "e": "no_active_video"}, 400)
        try:
            data = get_dash_preview(access_token, live_video_id)
        except Exception as e:
            return reply({"ok": 0, "e": str(e)}, 502)
        return reply({"ok": 1, "dash_preview_url": data.get("dash_preview_url"),
                      "status": data.get("status")})

    return reply({"ok": 0, "e": "bad_action"}, 400)


@app.route('/ping')
def ping():
    return 'ok'

# ─────────────────────────────────────────
# Dashboard UI
# ─────────────────────────────────────────

@app.route('/')
def dashboard():
    return Response(DASHBOARD_HTML, mimetype='text/html')

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FB Live Manager</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/dashjs/4.7.4/dash.all.min.js"></script>
<style>
  :root{
    --bg:#14171a; --panel:#1c2024; --panel2:#22262b; --line:#2c3136;
    --text:#e8e6e1; --sub:#8b9198; --live:#4caf7d; --wait:#d9a441;
    --idle:#5b6167; --err:#d9534f; --accent:#5b8bd9;
  }
  *{box-sizing:border-box;}
  body{margin:0;background:var(--bg);color:var(--text);
    font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    padding:28px 20px 80px;}
  .wrap{max-width:900px;margin:0 auto;}
  h1{font-size:20px;font-weight:600;margin:0 0 2px;}
  .tag{color:var(--sub);font-size:13px;margin:0 0 20px;}
  .authbar{display:flex;gap:8px;margin-bottom:24px;}
  .authbar input{flex:1;}
  input,button,textarea{font:inherit;}
  input[type=text],input[type=password]{
    background:var(--panel2);border:1px solid var(--line);color:var(--text);
    border-radius:6px;padding:9px 11px;font-size:14px;width:100%;}
  input:focus{outline:none;border-color:var(--accent);}
  button{cursor:pointer;border-radius:6px;border:1px solid var(--line);
    background:var(--panel2);color:var(--text);padding:8px 13px;font-size:13px;}
  button:hover{border-color:var(--accent);}
  button:disabled{opacity:.4;cursor:default;}
  button.primary{background:var(--accent);border-color:var(--accent);color:#0d1117;font-weight:600;}
  button.danger:hover{border-color:var(--err);color:var(--err);}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;
    padding:16px 18px;margin-bottom:14px;cursor:pointer;}
  .addform{display:grid;gap:10px;grid-template-columns:1fr 1fr;margin-bottom:24px;}
  .addform .full{grid-column:1/-1;}
  .addform button{grid-column:1/-1;}
  .row{display:flex;align-items:center;gap:10px;}
  .dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;}
  .dot.running{background:var(--live);box-shadow:0 0 6px var(--live);}
  .dot.waiting,.dot.restarting,.dot.stopping,.dot.starting,.dot.creating_video{background:var(--wait);}
  .dot.stopped{background:var(--idle);}
  .dot.error{background:var(--err);}
  .name{font-weight:600;font-size:15px;}
  .status{font-size:12px;color:var(--sub);text-transform:capitalize;}
  .meta{font-size:12px;color:var(--sub);margin-top:6px;line-height:1.6;
    display:flex;flex-wrap:wrap;gap:14px;}
  .actions{display:flex;gap:6px;margin-left:auto;flex-wrap:wrap;}
  .empty{color:var(--sub);font-size:13px;padding:20px 0;text-align:center;}
  .err{color:var(--err);font-size:13px;margin-top:8px;min-height:16px;}
  label{font-size:11px;color:var(--sub);display:block;margin-bottom:4px;}

  .modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);
    align-items:center;justify-content:center;z-index:50;padding:20px;}
  .modal-overlay.open{display:flex;}
  .modal{background:var(--panel);border:1px solid var(--line);border-radius:10px;
    padding:18px;max-width:640px;width:100%;}
  .modal h2{margin:0 0 10px;font-size:16px;}
  .modal video{width:100%;background:#000;border-radius:6px;}
  .modal .urlbox{font-size:11px;color:var(--sub);word-break:break-all;
    background:var(--panel2);padding:8px;border-radius:6px;margin-top:10px;}
  .modal .modal-actions{display:flex;gap:8px;margin-top:12px;}
</style>
</head>
<body>
<div class="wrap">
  <h1>FB Live Manager</h1>
  <p class="tag">Auto-creates Facebook Live Videos, streams into them, rotates every ~4h.</p>

  <div class="authbar">
    <input id="secret" type="password" placeholder="Control secret">
    <button class="primary" onclick="connect()">Connect</button>
  </div>
  <div id="authErr" class="err"></div>

  <div id="app" style="display:none;">
    <div class="card" style="cursor:default;">
      <label style="margin-bottom:8px;">Add channel</label>
      <div class="addform">
        <div><label>Name</label><input id="a_name" type="text" placeholder="e.g. France Feed"></div>
        <div><label>Page ID (blank = "me")</label><input id="a_page" type="text" placeholder="1234567890"></div>
        <div class="full"><label>Access token (Page or User)</label><input id="a_token" type="password" placeholder="EAA..."></div>
        <div class="full"><label>Input URL (m3u8 / any ffmpeg-readable source)</label><input id="a_url" type="text" placeholder="http(s)://..."></div>
        <button onclick="addChannel()">Add channel</button>
      </div>
      <div id="addErr" class="err"></div>
    </div>

    <div id="list"></div>
    <div id="emptyMsg" class="empty" style="display:none;">No channels yet — add one above.</div>
  </div>
</div>

<div class="modal-overlay" id="dashModal">
  <div class="modal">
    <h2 id="dashTitle">DASH preview</h2>
    <video id="dashVideo" controls autoplay muted></video>
    <div class="urlbox" id="dashUrlBox">Loading...</div>
    <div class="modal-actions">
      <button onclick="copyDashUrl()">Copy URL</button>
      <button onclick="closeDash()">Close</button>
    </div>
  </div>
</div>

<script>
let SECRET = localStorage.getItem('fblm_secret') || '';
let poll = null;
let dashPlayer = null;
let currentDashUrl = '';

function api(action, params={}){
  const q = new URLSearchParams({action, secret: SECRET, ...params});
  return fetch('/api?' + q.toString()).then(r => r.json());
}

function connect(){
  const s = document.getElementById('secret').value.trim() || SECRET;
  SECRET = s;
  api('list_full').then(res => {
    if(!res.ok){
      document.getElementById('authErr').textContent = 'Wrong secret, or server unreachable.';
      document.getElementById('app').style.display = 'none';
      return;
    }
    localStorage.setItem('fblm_secret', SECRET);
    document.getElementById('authErr').textContent = '';
    document.getElementById('app').style.display = 'block';
    render(res.s);
    if(!poll) poll = setInterval(refresh, 4000);
  }).catch(() => {
    document.getElementById('authErr').textContent = 'Could not reach the server.';
  });
}

function refresh(){
  api('list_full').then(res => { if(res.ok) render(res.s); });
}

function fmtTime(sec){
  const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60);
  return h + 'h ' + m + 'm';
}

function render(list){
  const el = document.getElementById('list');
  document.getElementById('emptyMsg').style.display = list.length ? 'none' : 'block';
  el.innerHTML = list.map(c => `
    <div class="card" onclick="openDash('${c.i}', event)">
      <div class="row">
        <div class="dot ${c.st}"></div>
        <div>
          <div class="name">${escapeHtml(c.n)}</div>
          <div class="status">${c.st}${c.rc ? ' · ' + c.rc + ' retries' : ''}</div>
        </div>
        <div class="actions">
          <button ${c.run ? 'disabled' : ''} onclick="event.stopPropagation(); act('start','${c.i}')">Start</button>
          <button ${c.run ? '' : 'disabled'} onclick="event.stopPropagation(); act('stop','${c.i}')">Stop</button>
          <button class="danger" onclick="event.stopPropagation(); removeChannel('${c.i}')">Remove</button>
        </div>
      </div>
      <div class="meta">
        <span>Input: ${escapeHtml(truncate(c.input_url, 40))}</span>
        <span>Page: ${escapeHtml(c.page_id)}</span>
        <span>Token •••${escapeHtml(c.token_tail)}</span>
        ${c.live_video_id ? `<span>Video ${c.live_video_id} (${fmtTime(c.video_age)} old)</span>` : ''}
      </div>
    </div>`).join('');
}

function truncate(s, n){ return s && s.length > n ? s.slice(0, n) + '…' : (s || ''); }
function escapeHtml(str){
  return String(str).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function act(action, id){ api(action, {id}).then(refresh); }

function removeChannel(id){
  if(!confirm('Remove this channel? This stops it and ends its live video.')) return;
  api('delete', {id}).then(refresh);
}

function addChannel(){
  const name = document.getElementById('a_name').value.trim();
  const page_id = document.getElementById('a_page').value.trim();
  const access_token = document.getElementById('a_token').value.trim();
  const input_url = document.getElementById('a_url').value.trim();
  const errEl = document.getElementById('addErr');
  errEl.textContent = '';
  if(!name || !access_token || !input_url){
    errEl.textContent = 'Fill in name, token and input URL.';
    return;
  }
  api('add', {name, page_id, access_token, input_url}).then(res => {
    if(!res.ok){
      errEl.textContent = res.e === 'max_channels' ? 'Limit of 10 channels reached.' : 'Could not add (' + res.e + ').';
      return;
    }
    document.getElementById('a_name').value = '';
    document.getElementById('a_page').value = '';
    document.getElementById('a_token').value = '';
    document.getElementById('a_url').value = '';
    refresh();
  });
}

function openDash(id, evt){
  document.getElementById('dashModal').classList.add('open');
  document.getElementById('dashTitle').textContent = 'Loading DASH preview...';
  document.getElementById('dashUrlBox').textContent = 'Loading...';
  currentDashUrl = '';

  api('dash', {id}).then(res => {
    if(!res.ok){
      document.getElementById('dashTitle').textContent = 'No preview available';
      document.getElementById('dashUrlBox').textContent =
        res.e === 'no_active_video' ? 'This channel is not currently live.' : ('Error: ' + res.e);
      return;
    }
    currentDashUrl = res.dash_preview_url || '';
    document.getElementById('dashTitle').textContent = 'DASH preview (' + (res.status || '') + ')';
    document.getElementById('dashUrlBox').textContent = currentDashUrl || 'No dash_preview_url returned yet.';

    if(currentDashUrl && window.dashjs){
      const videoEl = document.getElementById('dashVideo');
      if(dashPlayer){ dashPlayer.reset(); }
      dashPlayer = dashjs.MediaPlayer().create();
      dashPlayer.initialize(videoEl, currentDashUrl, true);
    }
  });
}

function closeDash(){
  document.getElementById('dashModal').classList.remove('open');
  if(dashPlayer){ dashPlayer.reset(); dashPlayer = null; }
}

function copyDashUrl(){
  if(!currentDashUrl) return;
  navigator.clipboard.writeText(currentDashUrl).catch(()=>{});
}

if(SECRET){
  document.getElementById('secret').value = SECRET;
  connect();
}
</script>
</body>
</html>
"""

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 7860)), debug=False)
