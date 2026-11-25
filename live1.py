import subprocess, threading, os, time, logging, json, sys, signal, re, html, hmac
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
# How long Start waits for a channel's previous worker to finish shutting down
# (it ends its Facebook live video first) so Stop All -> Start All just works.
STOP_WAIT_SECONDS = int(os.environ.get("RELAY_STOP_WAIT", "20"))
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
# "-max_interleave_delta", "0", i remove it because it make isuss with vps ram , but its very imp for stable stream 
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

# One rotation clock for the whole running set (not per channel):
#   epoch  = when the current rotation period began (group start, or last rotation)
#   gen    = counter bumped at every rotation; each channel remembers the value
#            it started its cycle with and rotates as soon as the counter moves.
#   roster = uids of the channels that belong to the live group. When the group
#            starts, every channel that exists at that moment is in it. A channel
#            added later is NOT in it, so Start on it only queues it until the
#            next rotation (or until the group is stopped and started again).
#   uid    = counter that gives every added channel a unique id (channel ids
#            can be reused after a delete, uids never are).
rotation = {"epoch": None, "gen": 0, "roster": set(), "uid": 0}


def _next_uid_locked():
    rotation["uid"] += 1
    return rotation["uid"]


def _others_running_locked(exclude_id):
    return any(c["running"] for k, c in channels.items() if k != exclude_id)


def clean_img_url(value):
    """Optional link to the channel's image post on Facebook (not a direct image URL).
    Stored as plain text and never used by the stream, so it is only checked for a
    sane http(s) link. Returns (ok, cleaned). Empty is allowed."""
    v = (value or "").strip()
    if not v:
        return True, ""
    if len(v) > 1000 or re.search(r"\s", v) or not re.match(r"^https?://", v, re.I):
        return False, ""
    return True, v


def _snapshot(c):
    """The settings a channel actually streams with. Edits go to the saved fields
    only; this copy is refreshed at a rotation or when a fresh group begins."""
    return {"name": c["name"], "access_token": c["access_token"],
            "page_id": c["page_id"], "input_url": c["input_url"]}


def _begin_group_locked(now):
    """A fresh timeline starts: new rotation period, every channel that exists now
    belongs to the group, and every channel takes its saved settings."""
    rotation["epoch"] = now
    rotation["roster"] = {o["uid"] for o in channels.values()}
    for o in channels.values():
        o["active"] = _snapshot(o)


def _launch_locked(cid, c):
    """Mark a channel as starting and build its worker thread. Caller holds
    channels_lock and must call .start() on the returned thread afterwards."""
    rotation["roster"].add(c["uid"])
    c["queued"] = False
    c["running"] = True
    c["restarts"] = 0
    c["status"] = "starting"
    t = threading.Thread(target=run_channel, args=(cid,), daemon=True)
    c["thread"] = t
    return t


def _check_rotation(my_gen):
    """True when a fleet-wide rotation has happened since this channel's cycle began.
    The first channel to notice the deadline fires it for everyone."""
    fire = False
    with channels_lock:
        now = time.time()
        if rotation["epoch"] and now - rotation["epoch"] >= RENEW_SECONDS:
            rotation["gen"] += 1
            rotation["epoch"] = now
            fire = True
        due = rotation["gen"] != my_gen
    if fire:
        log.info("Rotation reached — all running channels get a new live video")
        _start_queued()
    return due


def _start_queued():
    """Called when a channel rotates: bring every queued (new) channel live now."""
    to_start = []
    with channels_lock:
        for cid, c in channels.items():
            if c.get("queued") and not c["running"]:
                c["active"] = _snapshot(c)
                to_start.append((cid, _launch_locked(cid, c)))
    for cid, t in to_start:
        log.info(f"[{cid}] Rotation reached — starting queued channel")
        t.start()


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
    refresh = False   # first cycle uses the group's active settings; every later
                      # cycle (a rotation, or a retry after an error) re-reads the saved ones
    while True:
        with channels_lock:
            c = channels.get(channel_id)
            if not c or not c["running"]:
                return
            if refresh:
                c["active"] = _snapshot(c)
            refresh = True
            act = c["active"]
            access_token = act["access_token"]
            page_id = act["page_id"]
            input_url = act["input_url"]
            name = act["name"]
            my_gen = rotation["gen"]

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

            if _check_rotation(my_gen):
                reason = "renew"
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

                if _check_rotation(my_gen):
                    log.info(f"[{channel_id}] Rotation — moving to a new live video")
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
            # on a fatal error also release the channel, otherwise Start would
            # answer "already" and it could never be restarted
            _set(channel_id, status="stopped" if reason == "stopped" else "error", running=False)
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
    supplied = request.headers.get('X-Secret') or request.values.get('secret') or ''
    if not hmac.compare_digest(supplied, CONTROL_SECRET):
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
                    "img_url": c.get("img_url", ""),
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
        img_ok, img_url = clean_img_url(request.values.get('img_url', ''))

        if not name or not access_token or not input_url:
            return reply({"ok": 0, "e": "missing_fields"}, 400)
        if not img_ok:
            return reply({"ok": 0, "e": "bad_img_url"}, 400)

        with channels_lock:
            if len(channels) >= MAX_CHANNELS:
                return reply({"ok": 0, "e": "max_channels"}, 400)
            existing_ids = set(channels.keys())
            cid = next(str(i) for i in range(1, MAX_CHANNELS + 1) if str(i) not in existing_ids)
            channels[cid] = {
                "name": name, "access_token": access_token, "page_id": page_id,
                "input_url": input_url, "img_url": img_url, "running": False, "process": None,
                "live_video_id": None, "stream_url": None, "video_created_at": None,
                "status": "stopped", "restarts": 0,
                "thread": None, "queued": False, "uid": _next_uid_locked(),
                "active": {"name": name, "access_token": access_token, "page_id": page_id, "input_url": input_url},
            }
        log.info(f"Channel added: id={cid} name={name}")
        return reply({"ok": 1, "i": cid}, 201)

    if action == 'edit':
        # Saves new settings only. A running channel keeps streaming with the
        # settings it started its current cycle with; the saved values are read
        # when the next cycle starts (next ~3h55m rotation, or a fresh start).
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            # blank / missing fields keep the current value
            new_name = request.values.get('name', '').strip()
            new_token = clean_token_field(request.values.get('access_token', ''))
            new_page = request.values.get('page_id', '').strip()
            new_url = request.values.get('input_url', '').strip()
            img_ok, new_img = clean_img_url(request.values.get('img_url', ''))
            if not img_ok:
                return reply({"ok": 0, "e": "bad_img_url"}, 400)
            if new_img:
                c["img_url"] = new_img   # plain text link: takes effect at once, never waits for a rotation
            if new_name:
                c["name"] = new_name
            if new_token:
                c["access_token"] = new_token
            if new_page:
                c["page_id"] = new_page
            if new_url:
                c["input_url"] = new_url
            running = bool(c["running"])
            stream_changed = bool(new_name or new_token or new_page or new_url)
        log.info(f"Channel edited: id={cid} (saved; stream settings apply at next rotation/start)")
        if stream_changed:
            return reply({"ok": 1, "applied": 0, "pending": int(running)})
        return reply({"ok": 1, "applied": 1, "pending": 0})   # only the picture changed: done

    if action == 'delete':
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            c["running"] = False
            proc = c["process"]
            access_token = c["active"]["access_token"]   # the token the live video was created with
            live_video_id = c.get("live_video_id")
            del channels[cid]
        if proc and proc.poll() is None:
            kill_process_group(proc)
        safe_end_live_video(access_token, live_video_id)
        log.info(f"Channel removed: id={cid}")
        return reply({"ok": 1})

    if action == 'start':
        cid = request.values.get('id', '')
        force = request.values.get('force', '').lower() in ('1', 'true', 'yes')
        # If this channel's previous worker is still shutting down (it ends its
        # live video first), wait for it so Stop All -> Start All needs no pause.
        with channels_lock:
            c = channels.get(cid)
            old_t = c.get("thread") if c else None
            must_wait = bool(c and not c["running"] and old_t and old_t.is_alive())
        if must_wait:
            old_t.join(timeout=STOP_WAIT_SECONDS)
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            if c["running"]:
                return reply({"ok": 1, "st": "already"})
            now = time.time()
            in_group = c["uid"] in rotation["roster"]
            if _others_running_locked(cid):
                if force:
                    c["active"] = _snapshot(c)        # forced: join now with saved settings
                elif not in_group:
                    c["queued"] = True                # added after the group began
                    c["status"] = "queued"
                    log.info(f"Channel queued until next rotation: id={cid}")
                    return reply({"ok": 1, "st": "queued"})
            else:
                # nothing is running, so this press begins (or resumes) the group
                timeline_alive = bool(rotation["epoch"] and now - rotation["epoch"] < RENEW_SECONDS)
                # a group can only be RESUMED if at least one of its channels still exists;
                # otherwise (first start, or every old channel was deleted) there is nothing
                # to wait for, so this press begins a fresh timeline and everyone starts
                resumable = timeline_alive and any(o["uid"] in rotation["roster"]
                                                   for o in channels.values())
                if force or not resumable:
                    _begin_group_locked(now)          # fresh timeline: saved settings, everyone joins
                elif not in_group:
                    # same timeline resumed (Stop All -> Start All before the rotation
                    # time): late additions and edits keep waiting for the rotation
                    c["queued"] = True
                    c["status"] = "queued"
                    log.info(f"Channel queued until next rotation: id={cid}")
                    return reply({"ok": 1, "st": "queued"})
            old_t = c.get("thread")
            if old_t and old_t.is_alive():
                return reply({"ok": 0, "e": "still_stopping"}, 409)
            t = _launch_locked(cid, c)
        t.start()
        log.info(f"Channel start requested: id={cid} force={force}")
        return reply({"ok": 1})

    if action == 'stop':
        cid = request.values.get('id', '')
        with channels_lock:
            c = channels.get(cid)
            if not c:
                return reply({"ok": 0, "e": "notfound"}, 404)
            if c.get("queued") and not c["running"]:
                c["queued"] = False
                c["status"] = "stopped"
                log.info(f"Queued channel cancelled: id={cid}")
                return reply({"ok": 1})
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
            access_token = c["active"]["access_token"]
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


# ─────────────────────────────────────────
# Uptime / health endpoints (public, no secrets exposed)
# ─────────────────────────────────────────

START_TIME = time.time()


def fmt_duration(sec):
    sec = int(max(sec, 0))
    d, rem = divmod(sec, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _next_rotation_in():
    with channels_lock:
        epoch = rotation["epoch"]
        live = any(c["running"] for c in channels.values())
    if not epoch or not live:
        return None
    return max(int(epoch + RENEW_SECONDS - time.time()), 0)


@app.route('/ping')
def ping():
    # Point UptimeRobot / BetterStack / cron-job.org here
    return 'ok'


@app.route('/health')
def health():
    with channels_lock:
        total = len(channels)
        running = sum(1 for c in channels.values() if c["status"] == "running")
    return jsonify({
        "ok": 1,
        "uptime_seconds": int(time.time() - START_TIME),
        "channels_total": total,
        "channels_running": running,
        "next_rotation_seconds": _next_rotation_in(),
    })


STATUS_COLORS = {
    "running": "#4caf7d", "stopped": "#5b6167", "error": "#d9534f",
}


@app.route('/')
def status_page():
    rows = []
    with channels_lock:
        for c in channels.values():
            age = time.time() - c["video_created_at"] if c.get("video_created_at") else 0
            rows.append((c["name"], c["status"], c.get("restarts", 0), age))

    server_up = fmt_duration(time.time() - START_TIME)
    nr = _next_rotation_in()
    next_rot = fmt_duration(nr) if nr is not None else "n/a"
    all_ok = all(st in ("running", "stopped", "queued") for _, st, _, _ in rows)

    body = ""
    for name, st, restarts, age in rows:
        color = STATUS_COLORS.get(st, "#d9a441")
        live_for = f" · live video {fmt_duration(age)}" if st == "running" and age else ""
        body += (
            f'<div class="row"><span class="dot" style="background:{color}"></span>'
            f'<b>{html.escape(name)}</b>'
            f'<span class="sub">{html.escape(st)}{live_for} · {restarts} retries</span></div>'
        )
    if not body:
        body = '<div class="sub">No channels configured.</div>'

    page = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>Relay status</title>
<style>
body{{margin:0;padding:32px 16px;background:#14171a;color:#e8e6e1;
font-family:-apple-system,"Segoe UI",Roboto,Arial,sans-serif;}}
.wrap{{max-width:560px;margin:0 auto;}}
h1{{font-size:18px;margin:0 0 4px;}}
.sub{{color:#8b9198;font-size:13px;}}
.card{{background:#1c2024;border:1px solid #2c3136;border-radius:10px;
padding:14px 16px;margin-top:16px;}}
.row{{display:flex;align-items:center;gap:10px;padding:8px 0;flex-wrap:wrap;}}
.dot{{width:9px;height:9px;border-radius:50%;flex-shrink:0;}}
</style></head><body><div class="wrap">
<h1><span class="dot" style="display:inline-block;background:{'#4caf7d' if all_ok else '#d9a441'}"></span>
 {'All systems operational' if all_ok else 'Some channels degraded'}</h1>
<div class="sub">Server uptime: {server_up} · next rotation: {next_rot} · auto-refreshes every 15s</div>
<div class="card">{body}</div>
</div></body></html>"""
    return Response(page, mimetype='text/html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 7860)), debug=False)
