#!/usr/bin/env python3
# ============================================================
#  SMS Queue Bot — Fly.io ready, single file
#  Workflow:
#    upload .txt → parse numbers → push to Firebase sms_queue
#    orchestrator polls queue → rotate device every N sends → mark sent
# ============================================================
print("Starting SMS Queue Bot...")

import asyncio
import copy
import html
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import threading
import time
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

try:
    import google.auth.transport.requests
    from google.oauth2 import service_account
    HAS_GOOGLE_AUTH = True
except ImportError:
    HAS_GOOGLE_AUTH = False

import requests
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.constants import ParseMode, ChatType, ChatMemberStatus
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, ChatMemberHandler, filters, ContextTypes
)

# ===================== CONFIG (env-first for Fly.io) =====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.environ.get("SETTINGS_PATH") or os.path.join(BASE_DIR, "settings.json")
DB_FILE = os.environ.get("DB_PATH") or os.path.join(BASE_DIR, "bot_data.db")
DATA_DIR = os.path.dirname(DB_FILE) or BASE_DIR
RELOAD_FLAG = os.environ.get("RELOAD_FLAG") or os.path.join(DATA_DIR, ".reload_flag")
LOG_FILE = os.environ.get("LOG_PATH") or os.path.join(DATA_DIR, "smsbot.log")

BOT_TOKEN = ""
CHANNEL_USERNAME = ""
CHANNEL_TITLE = ""
CHANNEL_LINK = ""
ADMIN_IDS: List[int] = []
BATCH_SIZE = 5
POLL_INTERVAL = 8
SEND_TIMEOUT = 25
_settings_mtime = 0.0
_db_mtime = 0.0


def _ensure_dirs():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception:
        pass


def _load_settings_file() -> dict:
    try:
        if os.path.isfile(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception as e:
        print(f"settings.json warning: {e}")
    return {}


def _first_str(*vals) -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def reload_runtime_config() -> dict:
    global BOT_TOKEN, CHANNEL_USERNAME, CHANNEL_TITLE, CHANNEL_LINK, ADMIN_IDS, BATCH_SIZE, _settings_mtime
    cfg = _load_settings_file()
    try:
        _settings_mtime = os.path.getmtime(SETTINGS_FILE) if os.path.isfile(SETTINGS_FILE) else 0.0
    except Exception:
        _settings_mtime = 0.0

    BOT_TOKEN = _first_str(
        os.environ.get("BOT_TOKEN"),
        os.environ.get("TELEGRAM_AUTO_BOT_TOKEN"),
        cfg.get("telegramAutoBotToken"),
        cfg.get("telegramBotToken"),
        cfg.get("bot_token"),
    )
    CHANNEL_USERNAME = _first_str(os.environ.get("CHANNEL_USERNAME"), cfg.get("channel_username"), cfg.get("channelUsername"))
    CHANNEL_TITLE = _first_str(os.environ.get("CHANNEL_TITLE"), cfg.get("channel_title"))
    CHANNEL_LINK = _first_str(os.environ.get("CHANNEL_LINK"), cfg.get("channel_link"))

    admin_raw = _first_str(os.environ.get("ADMIN_IDS"), cfg.get("admin_ids"))
    if not admin_raw and isinstance(cfg.get("admin_ids"), list):
        admin_raw = ",".join(str(x) for x in cfg.get("admin_ids") or [])
    ADMIN_IDS = [int(x.strip()) for x in admin_raw.split(",") if x.strip().isdigit()]

    try:
        BATCH_SIZE = int(_first_str(os.environ.get("BATCH_SIZE"), cfg.get("batch_size")) or BATCH_SIZE)
    except Exception:
        pass

    return {"bot_token_set": bool(BOT_TOKEN), "admins": ADMIN_IDS[:], "batch_size": BATCH_SIZE}


_ensure_dirs()
reload_runtime_config()

DEFAULT_USER_CONFIG = {
    "firebase_list": [],
    "active_firebase_index": 0,
    "devices": [],
    "sim_index": 0,
    "monitored_groups": [],
    "default_message": "",
    "confirm_reply": False,
    "queue_enabled": True,
}

MAIN_MENU, ADD_PUBLIC_FIREBASE, ADD_PRIVATE_FIREBASE, ADD_FIREBASE_SECRET, ADD_GROUP, AWAITING_DEVICE = range(6)

# ===================== CACHE =====================
user_cache: Dict[str, dict] = {}
group_index: Dict[str, Set[str]] = {}
banned_set: Set[str] = set()
cache_lock = threading.RLock()

# ===================== LOGGING =====================
_ensure_dirs()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== DB =====================
def init_db():
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY, config TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS banned (
            user_id TEXT PRIMARY KEY, banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("PRAGMA journal_mode=WAL")
        conn.commit()
    except Exception as e:
        logger.error(f"init_db error: {e}")
    finally:
        if conn:
            conn.close()


def load_cache():
    global user_cache, group_index, banned_set
    with cache_lock:
        user_cache.clear(); group_index.clear(); banned_set.clear()
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            cur.execute("SELECT user_id, config FROM users")
            for uid, conf in cur.fetchall():
                try:
                    cfg = json.loads(conf)
                    for k, v in DEFAULT_USER_CONFIG.items():
                        if k not in cfg:
                            cfg[k] = copy.deepcopy(v)
                    fb_list = []
                    for fb in cfg.get("firebase_list", []):
                        if isinstance(fb, str):
                            fb_list.append({"url": fb, "secret": "", "service_account": None})
                        elif isinstance(fb, dict):
                            fb.setdefault("secret", ""); fb.setdefault("service_account", None)
                            fb_list.append(fb)
                    cfg["firebase_list"] = fb_list
                    cfg["devices"] = [str(d) for d in cfg.get("devices", [])]
                    if cfg.get("device_id") and cfg["device_id"] not in cfg["devices"]:
                        cfg["devices"].append(str(cfg["device_id"]))
                    groups = []
                    for g in cfg.get("monitored_groups", []):
                        if isinstance(g, str):
                            groups.append({"id": str(g), "title": str(g)})
                        else:
                            g["id"] = str(g["id"]); groups.append(g)
                    cfg["monitored_groups"] = groups
                    user_cache[uid] = cfg
                    for g in groups:
                        group_index.setdefault(str(g["id"]), set()).add(uid)
                except Exception as e:
                    logger.warning(f"skip corrupt row {uid}: {e}")
            cur.execute("SELECT user_id FROM banned")
            for (uid,) in cur.fetchall():
                banned_set.add(uid)
        except Exception as e:
            logger.error(f"load_cache error: {e}")
        finally:
            if conn:
                conn.close()
        logger.info(f"✅ cache: {len(user_cache)} users | {len(group_index)} groups")


def get_user_config(user_id: int) -> dict:
    uid = str(user_id)
    with cache_lock:
        if uid in user_cache:
            return copy.deepcopy(user_cache[uid])
        cfg = copy.deepcopy(DEFAULT_USER_CONFIG)
        user_cache[uid] = copy.deepcopy(cfg)
    _save_to_db(uid, cfg)
    return cfg


def save_user_config(user_id: int, cfg: dict):
    uid = str(user_id)
    with cache_lock:
        old_ids = {str(g["id"]) for g in user_cache.get(uid, {}).get("monitored_groups", [])}
        new_ids = {str(g["id"]) for g in cfg.get("monitored_groups", [])}
        for g in cfg.get("monitored_groups", []):
            g["id"] = str(g["id"])
        user_cache[uid] = copy.deepcopy(cfg)
        for gid in old_ids - new_ids:
            if gid in group_index:
                group_index[gid].discard(uid)
                if not group_index[gid]:
                    del group_index[gid]
        for gid in new_ids - old_ids:
            group_index.setdefault(gid, set()).add(uid)
    _save_to_db(uid, cfg)


def _save_to_db(uid: str, cfg: dict):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO users (user_id, config, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (uid, json.dumps(cfg, ensure_ascii=False)))
        conn.commit(); conn.close()
    except Exception as e:
        logger.error(f"DB save: {e}")


def is_banned(user_id: int) -> bool:
    return str(user_id) in banned_set


def ban_user(user_id: int):
    uid = str(user_id)
    with cache_lock:
        banned_set.add(uid)
    try:
        conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO banned (user_id) VALUES (?)", (uid,))
        conn.commit(); conn.close()
    except Exception as e:
        logger.error(f"ban: {e}")


def unban_user(user_id: int):
    uid = str(user_id)
    with cache_lock:
        banned_set.discard(uid)
    try:
        conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
        cur.execute("DELETE FROM banned WHERE user_id = ?", (uid,))
        conn.commit(); conn.close()
    except Exception as e:
        logger.error(f"unban: {e}")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_stats():
    with cache_lock:
        total = len(user_cache); fb = gr = act = 0
        for cfg in user_cache.values():
            fb += len(cfg.get("firebase_list", []))
            gr += len(cfg.get("monitored_groups", []))
            if cfg.get("queue_enabled", True) and cfg.get("devices"):
                act += 1
        return total, fb, gr, act, len(banned_set)


def remove_group_from_all_users(group_id: str):
    group_id = str(group_id)
    to_save = []
    with cache_lock:
        for uid in list(group_index.get(group_id, set())):
            cfg = user_cache.get(uid)
            if not cfg: continue
            cfg["monitored_groups"] = [g for g in cfg.get("monitored_groups", []) if str(g["id"]) != group_id]
            user_cache[uid] = cfg
            to_save.append((uid, copy.deepcopy(cfg)))
        group_index.pop(group_id, None)
    for uid, cfg in to_save:
        _save_to_db(uid, cfg)


def update_group_title(group_id: str, new_title: str):
    group_id = str(group_id); to_save = []
    with cache_lock:
        for uid in list(group_index.get(group_id, set())):
            cfg = user_cache.get(uid)
            if not cfg: continue
            changed = False
            for g in cfg.get("monitored_groups", []):
                if str(g["id"]) == group_id and g.get("title") != new_title:
                    g["title"] = new_title; changed = True
            if changed:
                user_cache[uid] = cfg; to_save.append((uid, copy.deepcopy(cfg)))
    for uid, cfg in to_save:
        _save_to_db(uid, cfg)

# ===================== FIREBASE =====================
FIREBASE_SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/firebase.database"
]
_token_cache: Dict[str, Tuple[str, float]] = {}


def get_service_account_token(sa_info: dict) -> Optional[str]:
    if not HAS_GOOGLE_AUTH or not isinstance(sa_info, dict):
        return None
    try:
        ce = sa_info.get("client_email", "")
        if not ce: return None
        now = time.time()
        if ce in _token_cache:
            tok, exp = _token_cache[ce]
            if now < exp - 60:
                return tok
        creds = service_account.Credentials.from_service_account_info(sa_info, scopes=FIREBASE_SCOPES)
        creds.refresh(google.auth.transport.requests.Request())
        if creds.token:
            exp = creds.expiry.timestamp() if creds.expiry else (now + 3500)
            _token_cache[ce] = (creds.token, exp)
            return creds.token
    except Exception as e:
        logger.error(f"SA token: {e}")
    return None


def normalize_firebase_base(url: str) -> str:
    url = (url or "").strip()
    if not url: return ""
    try:
        p = urllib.parse.urlparse(url)
        if not (p.scheme and p.netloc): return url.rstrip("/")
        path = (p.path or "").rstrip("/")
        if path.endswith(".json"): path = path[:-5].rstrip("/")
        for leaf in ("/clients", "/devices", "/messages", "/sms_queue", "/.json"):
            if path.endswith(leaf): path = path[: -len(leaf)].rstrip("/")
        return f"{p.scheme}://{p.netloc}{path}".rstrip("/")
    except Exception:
        return url.rstrip("/")


def parse_firebase_input(text: str) -> Tuple[str, str]:
    text = (text or "").strip()
    try:
        p = urllib.parse.urlparse(text)
        if p.scheme and p.netloc:
            qs = urllib.parse.parse_qs(p.query)
            sec = ((qs.get("auth") or qs.get("access_token") or [""])[0] or "").strip()
            return normalize_firebase_base(f"{p.scheme}://{p.netloc}{p.path}"), sec
    except Exception:
        pass
    return normalize_firebase_base(text), ""


def clean_database_secret(raw: str) -> str:
    s = (raw or "").strip()
    if not s: return ""
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    if s.lower().startswith("auth="): s = s.split("=", 1)[1].strip()
    if s.startswith("http://") or s.startswith("https://"):
        _, sec = parse_firebase_input(s)
        if sec: return sec
    if "\n" in s:
        for line in s.splitlines():
            line = line.strip()
            if line and not line.lower().startswith(("database", "secret", "http")):
                s = line; break
    return s.strip()


def validate_firebase_url(url: str) -> bool:
    u = (url or "").strip()
    return u.startswith(("https://", "http://"))


def test_firebase_auth(base_url: str, secret: str = "", service_account_info: Optional[dict] = None) -> Tuple[bool, str]:
    base = normalize_firebase_base(base_url)
    if not base: return False, "Invalid Firebase URL"
    headers: dict = {}; params: dict = {}
    sa = service_account_info if isinstance(service_account_info, dict) else None
    if sa and sa.get("private_key"):
        token = get_service_account_token(sa)
        if not token: return False, "Service Account OAuth failed"
        params["access_token"] = token
        headers["Authorization"] = f"Bearer {token}"
    else:
        sec = clean_database_secret(secret)
        if not sec: return False, "Database Secret empty"
        if sec.startswith("AIza"):
            return False, "Ye Web API Key hai — Database Secret ya Service Account use karein"
        params["auth"] = sec
    try:
        r = requests.get(f"{base}/.json", params=params, headers=headers, timeout=15)
        if r.status_code == 200: return True, "Connected"
        if r.status_code in (401, 403): return False, f"Permission denied ({r.status_code})"
        if r.status_code == 404: return False, "URL not found (404)"
        return False, f"HTTP {r.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.ConnectionError:
        return False, "Unreachable"
    except Exception as e:
        return False, f"Error: {e}"


def validate_phone_number(phone: str) -> bool:
    c = (phone or "").strip()
    if not c: return False
    if re.match(r'^[\d]{6,}$', c): return True
    return bool(re.match(r'^\+?[\d\-\s\(\)]{7,}$', c))


async def extract_json_from_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg: return None, None
    if msg.document:
        try:
            file = await context.bot.get_file(msg.document.file_id)
            content = (await file.download_as_bytearray()).decode("utf-8", errors="ignore").strip()
            return json.loads(content), None
        except json.JSONDecodeError:
            return None, "Invalid JSON file"
        except Exception as e:
            return None, f"Read error: {e}"
    text = (msg.text or "").strip()
    if text.startswith("{") and text.endswith("}"):
        try: return json.loads(text), None
        except json.JSONDecodeError: return None, "Invalid JSON"
    return None, None


def parse_service_account_dict(data: dict):
    if not isinstance(data, dict):
        return False, None, None, "Invalid JSON"
    if "project_info" in data and "client" in data:
        pi = data.get("project_info", {})
        url = pi.get("firebase_url") or f"https://{pi.get('project_id','')}-default-rtdb.firebaseio.com"
        return False, pi.get("project_id"), url, (
            "⚠️ Ye `google-services.json` hai. Admin SDK Service Account JSON chahiye:\n"
            "Firebase Console ➔ Project Settings ➔ Service accounts ➔ Generate new private key"
        )
    if data.get("type") == "service_account" and data.get("private_key"):
        pid = data.get("project_id")
        if not pid: return False, None, None, "Missing project_id"
        return True, pid, f"https://{pid}-default-rtdb.firebaseio.com", None
    return False, None, None, "Valid Service Account JSON nahi hai"


def get_active_firebase_entry(user_cfg: dict) -> dict:
    lst = user_cfg.get("firebase_list", [])
    idx = user_cfg.get("active_firebase_index", 0)
    if 0 <= idx < len(lst):
        it = lst[idx]
        return {"url": it, "secret": "", "service_account": None} if isinstance(it, str) else it
    return {}


def get_firebase_request_params(user_cfg: dict, path: str) -> Tuple[str, dict]:
    entry = get_active_firebase_entry(user_cfg)
    base = normalize_firebase_base(entry.get("url", "") or "")
    if not base: return "", {}
    clean_path = path.lstrip("/")
    url = f"{base}/{clean_path}"
    headers: dict = {}; params: dict = {}
    sa = entry.get("service_account")
    if sa and isinstance(sa, dict) and sa.get("private_key"):
        token = get_service_account_token(sa)
        if token:
            params["access_token"] = token
            headers["Authorization"] = f"Bearer {token}"
    else:
        sec = clean_database_secret(entry.get("secret", "") or "")
        if sec and not sec.startswith("AIza"):
            params["auth"] = sec
    if params:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{urllib.parse.urlencode(params)}"
    return url, headers


def fetch_json(url: str, headers: dict = None, retries: int = 2, timeout: int = 8) -> dict:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers or {}, timeout=timeout)
            if r.status_code == 200:
                txt = r.text.strip()
                return {} if txt == "null" else r.json()
            if r.status_code >= 500 and attempt < retries:
                time.sleep(0.4); continue
            return {}
        except requests.exceptions.Timeout:
            if attempt == retries: logger.warning(f"timeout {url[:80]}")
            else: time.sleep(0.3)
        except requests.exceptions.ConnectionError:
            if attempt == retries: logger.warning(f"conn err {url[:80]}")
            else: time.sleep(0.3)
        except Exception as e:
            logger.error(f"fetch: {e}"); break
    return {}


def put_json(url: str, payload: dict, headers: dict = None, timeout: int = 8) -> Tuple[bool, str]:
    try:
        r = requests.put(url, json=payload, headers=headers or {}, timeout=timeout)
        if 200 <= r.status_code < 300: return True, "OK"
        if r.status_code in (401, 403): return False, "Permission Denied"
        return False, f"HTTP {r.status_code}: {r.text[:100]}"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.ConnectionError:
        return False, "Connection failed"
    except Exception as e:
        return False, str(e)[:120]


def post_json(url: str, payload: dict, headers: dict = None, timeout: int = 8) -> Tuple[bool, str, dict]:
    try:
        r = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
        if 200 <= r.status_code < 300:
            try: return True, "OK", r.json() or {}
            except Exception: return True, "OK", {}
        if r.status_code in (401, 403): return False, "Permission Denied", {}
        return False, f"HTTP {r.status_code}: {r.text[:100]}", {}
    except requests.exceptions.Timeout:
        return False, "Timeout", {}
    except requests.exceptions.ConnectionError:
        return False, "Connection failed", {}
    except Exception as e:
        return False, str(e)[:120], {}


def patch_json(url: str, payload: dict, headers: dict = None, timeout: int = 8) -> Tuple[bool, str]:
    try:
        r = requests.patch(url, json=payload, headers=headers or {}, timeout=timeout)
        if 200 <= r.status_code < 300: return True, "OK"
        if r.status_code in (401, 403): return False, "Permission Denied"
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)[:120]

# ===================== DEVICE =====================
def _to_ms_ts(val) -> Optional[float]:
    try: fv = float(val)
    except Exception: return None
    if fv < 1e12: fv *= 1000.0
    return fv


def is_device_online(data: dict, now_ms: float = None) -> bool:
    if not isinstance(data, dict): return False
    if now_ms is None: now_ms = time.time() * 1000
    if data.get("isOnline") or data.get("online") or data.get("connected"): return True
    st = data.get("status")
    if st is True or st == 1: return True
    if isinstance(st, str) and st.strip().lower() in ("online", "active", "alive", "on", "true", "1", "connected"):
        return True
    if isinstance(st, (int, float)) and st != 0: return True
    window = 900_000.0
    for key in ("lastOnlineAt", "last_seen", "lastSeen", "lastSeenAt", "updatedAt",
                "last_update", "lastUpdate", "timestamp", "ts", "last_heartbeat", "lastHeartbeat"):
        ms = _to_ms_ts(data.get(key))
        if ms is not None and -60_000 < (now_ms - ms) < window: return True
    hb = data.get("heartbeat")
    if isinstance(hb, dict):
        ms = _to_ms_ts(hb.get("timestamp") or hb.get("ts") or hb.get("time"))
        if ms is not None and (now_ms - ms) < window: return True
    elif hb is not None:
        ms = _to_ms_ts(hb)
        if ms is not None and (now_ms - ms) < window: return True
    return False


def device_display_meta(data: dict) -> Tuple[str, str, bool]:
    if not isinstance(data, dict): return "", "", False
    model = (data.get("deviceModel") or data.get("modelName") or data.get("model")
             or data.get("phone_model") or data.get("device_name") or data.get("name") or "")
    phone = (data.get("phoneNumber") or data.get("mobNo") or data.get("mobile")
             or data.get("number") or data.get("msisdn") or "")
    return str(model).strip(), str(phone).strip(), is_device_online(data)


def fetch_all_devices(user_cfg: dict) -> Dict[str, dict]:
    merged: Dict[str, dict] = {}
    for path in ("clients.json", "devices.json"):
        url, headers = get_firebase_request_params(user_cfg, path)
        if not url: continue
        blob = fetch_json(url, headers=headers, retries=2, timeout=20)
        if not isinstance(blob, dict) or not blob: continue
        for did, data in blob.items():
            if data is None or not isinstance(data, dict): continue
            if str(did).startswith(".") or str(did) in ("webhookEvent", "actions", "config"): continue
            did_s = str(did)
            if did_s in merged and isinstance(merged[did_s], dict):
                base = dict(merged[did_s])
                base.update({k: v for k, v in data.items() if v is not None})
                merged[did_s] = base
            else:
                merged[did_s] = data
    return merged


def get_online_devices(user_cfg: dict) -> Dict[str, dict]:
    devs = fetch_all_devices(user_cfg)
    now = time.time() * 1000
    return {str(d): v for d, v in devs.items() if is_device_online(v, now)}


def get_device_data(user_cfg: dict, did: str) -> dict:
    did = str(did)
    for path in (f"clients/{did}.json", f"devices/{did}.json"):
        url, headers = get_firebase_request_params(user_cfg, path)
        if not url: continue
        data = fetch_json(url, headers=headers, timeout=10)
        if data and isinstance(data, dict): return data
    return {}


def extract_sims(device_data: dict) -> List[dict]:
    if not isinstance(device_data, dict):
        return [{"index": 0, "label": "📶 SIM 1"}, {"index": 1, "label": "📶 SIM 2"}]
    sims_raw = (device_data.get("sims") or device_data.get("simCards")
                or device_data.get("sim_cards") or device_data.get("simInfo"))
    if sims_raw and isinstance(sims_raw, list):
        out = []
        for i, s in enumerate(sims_raw):
            if not isinstance(s, dict): continue
            try: idx = int(s.get("simSlotIndex", s.get("slot", s.get("index", i))))
            except Exception: idx = i
            label = f"📶 SIM {idx + 1}"
            extra = []
            for k in ("carrierName", "carrier", "phoneNumber", "number", "msisdn"):
                v = s.get(k)
                if v and str(v).lower() not in ("no service", "unknown"):
                    extra.append(str(v))
            if extra: label += f" ({', '.join(extra)})"
            out.append({"index": idx, "label": label})
        if out: return out
    return [{"index": 0, "label": "📶 SIM 1"}, {"index": 1, "label": "📶 SIM 2"}]

# ===================== SMS =====================
def _send_sms_sync(user_cfg: dict, device_id: str, to: str, msg: str) -> Tuple[bool, str, str]:
    if not device_id: return False, "No device", ""
    sim = user_cfg.get("sim_index", 0)
    now_ms = int(time.time() * 1000)
    payload = {"from": sim, "to": to.strip(), "message": msg.strip(),
               "isSended": False, "sendOk": False, "cmdId": now_ms}
    attempts = [
        f"clients/{device_id}/webhookEvent/sendSms.json",
        f"devices/{device_id}/webhookEvent/sendSms.json",
        f"devices/{device_id}/actions/sendSms.json",
        f"clients/{device_id}/actions/sendSms.json",
    ]
    last = "No Firebase"
    for path in attempts:
        url, headers = get_firebase_request_params(user_cfg, path)
        if not url: continue
        ok, reason = put_json(url, payload, headers=headers, timeout=SEND_TIMEOUT)
        if ok:
            logger.info(f"📤 SMS OK via {path} → {to}")
            return True, "OK", path
        last = f"{reason} @ {path}"
        if reason == "Permission Denied":
            return False, reason, path
    return False, last, ""


async def send_sms_async(user_cfg: dict, device_id: str, to: str, msg: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _send_sms_sync, user_cfg, device_id, to, msg)

# ===================== QUEUE =====================
def push_numbers_to_queue_sync(user_cfg: dict, numbers: List[str], message: str, device_id: str) -> Tuple[bool, str, int]:
    if not numbers: return False, "No numbers", 0
    queue_url, headers = get_firebase_request_params(user_cfg, "sms_queue.json")
    if not queue_url: return False, "No active Firebase", 0
    pushed = 0
    for num in numbers:
        payload = {
            "to": num, "message": message, "device_id": device_id or "",
            "status": "pending", "createdAt": int(time.time() * 1000),
            "attempts": 0, "sent_at": 0,
        }
        ok, reason, _ = post_json(queue_url, payload, headers=headers, timeout=10)
        if ok: pushed += 1
        else: logger.warning(f"queue push fail {num}: {reason}")
    return pushed > 0, f"pushed {pushed}/{len(numbers)}", pushed


async def push_numbers_to_queue(user_cfg: dict, numbers: List[str], message: str, device_id: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, push_numbers_to_queue_sync, user_cfg, numbers, message, device_id)

# ===================== PARSER =====================
PHONE_REGEX = re.compile(r'\+?\d[\d\s\-\(\)]{5,}\d')


def parse_numbers_from_text(text: str) -> List[str]:
    seen = set(); out = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"): continue
        for m in PHONE_REGEX.findall(line):
            cleaned = re.sub(r'[\s\-\(\)]', '', m)
            if not cleaned.startswith("+"):
                cleaned = cleaned.lstrip("0") if cleaned.startswith("0") and len(cleaned) > 10 else cleaned
            if len(cleaned.lstrip("+")) < 7: continue
            if cleaned not in seen:
                seen.add(cleaned); out.append(cleaned)
    return out


def parse_message(text: str):
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    m = next((l for l in lines if l.startswith("🏷️ MESSAGE")), None)
    r = next((l for l in lines if l.startswith("🏷️ RECIPIENT")), None)
    if m and r:
        return r.split(":", 1)[-1].strip(), m.split(":", 1)[-1].strip()
    to_num = msg = None
    for i, l in enumerate(lines):
        if l.startswith("📱") and "To:" in l:
            p = l.split("To:", 1)
            to_num = p[1].strip() if len(p) > 1 and p[1].strip() else None
        if l.startswith("💬") and "Full Message:" in l and i + 1 < len(lines):
            msg = lines[i + 1].strip()
    if to_num and msg: return to_num, msg
    return None, None

# ===================== KEYBOARDS =====================
def get_main_keyboard(user_cfg: dict):
    reply_label = "🔔 Start Reply" if not user_cfg.get("confirm_reply") else "🔔 Stop Reply"
    queue_label = "▶️ Start Queue" if not user_cfg.get("queue_enabled", True) else "⏸️ Stop Queue"
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 Status")],
            [KeyboardButton("📁 Manage Firebase")],
            [KeyboardButton("📱 Devices"), KeyboardButton("📶 SIM")],
            [KeyboardButton("👥 Group"), KeyboardButton("✉️ Message Template")],
            [KeyboardButton(reply_label), KeyboardButton(queue_label)],
            [KeyboardButton("🚀 Upload Numbers (.txt)")],
        ],
        resize_keyboard=True
    )


FIREBASE_SUB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🌐 Add Public Firebase"), KeyboardButton("🔒 Add Private Firebase")],
        [KeyboardButton("📋 Select Firebase"), KeyboardButton("🗑️ Delete Firebase")],
        [KeyboardButton("🔙 Back")]
    ],
    resize_keyboard=True
)


GROUP_SUB = ReplyKeyboardMarkup(
    [[KeyboardButton("➕ Add Group"), KeyboardButton("➖ Delete Group")],
     [KeyboardButton("📋 Select Group")],
     [KeyboardButton("🔙 Back")]],
    resize_keyboard=True
)

# ===================== ACCESS =====================
async def is_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not CHANNEL_USERNAME: return True
    try:
        m = await context.bot.get_chat_member(CHANNEL_USERNAME, update.effective_user.id)
        return m.status not in ("left", "kicked")
    except Exception:
        return True


async def require_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    if is_banned(uid) and not is_admin(uid):
        if update.message: await update.message.reply_text("🚫 You are banned.")
        elif update.callback_query: await update.callback_query.answer("Banned.", show_alert=True)
        return False
    if CHANNEL_USERNAME and not await is_member(update, context):
        title = CHANNEL_TITLE or CHANNEL_USERNAME
        link = CHANNEL_LINK
        if not link:
            uname = CHANNEL_USERNAME.lstrip("@")
            link = f"https://t.me/{uname}" if uname else ""
        msg = f"🚫 Join [{title}]({link}) first." if link else f"🚫 Join {title} first."
        if update.callback_query: await update.callback_query.answer("Join channel first!", show_alert=True)
        elif update.message:
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        return False
    return True

# ===================== STATUS =====================
def build_status_text(user_cfg: dict) -> str:
    fb_list = user_cfg.get("firebase_list", [])
    idx = user_cfg.get("active_firebase_index", 0)
    active_fb = "None"
    if 0 <= idx < len(fb_list):
        it = fb_list[idx]
        url = it.get("url") if isinstance(it, dict) else str(it)
        sec = it.get("secret") if isinstance(it, dict) else ""
        sa = it.get("service_account") if isinstance(it, dict) else None
        tag = " (🔒 SA)" if sa else (" (🔒 Secret)" if sec else " (🌐 Public)")
        active_fb = f"{url}{tag}"
    devs = user_cfg.get("devices", [])
    sim_idx = user_cfg.get("sim_index", 0)
    groups = user_cfg.get("monitored_groups", [])
    tpl = user_cfg.get("default_message", "")
    tpl_show = (tpl[:60] + "…") if len(tpl) > 60 else (tpl or "Not set")
    msg = (
        f"📊 **Your Status**\n"
        f"🌐 Active Firebase: `{active_fb}`\n"
        f"   (Total: {len(fb_list)})\n"
        f"📱 Devices ({len(devs)}): " + (", ".join(f"`{d}`" for d in devs) if devs else "`none`") + "\n"
        f"📶 SIM: SIM {sim_idx + 1}\n"
        f"✉️ Template: {tpl_show}\n"
        f"👥 Groups: {len(groups)}\n"
    )
    for g in groups:
        msg += f"• `{g.get('title') or g['id']}`\n"
    msg += (
        f"🔔 Reply: {'ON' if user_cfg.get('confirm_reply') else 'OFF'}\n"
        f"🚀 Queue: {'ON' if user_cfg.get('queue_enabled', True) else 'OFF'}\n"
        f"🔄 Batch rotate: every {BATCH_SIZE} sends"
    )
    return msg

# ===================== ADMIN =====================
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    t, fb, gr, act, ban = get_stats()
    await update.message.reply_text(
        f"📈 **Stats**\n👥 Users: `{t}`\n🌐 Firebases: `{fb}`\n"
        f"📢 Groups: `{gr}`\n🤖 Active: `{act}`\n🚫 Banned: `{ban}`",
        parse_mode=ParseMode.MARKDOWN)


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not context.args: return await update.message.reply_text("Usage: /broadcast msg")
    msg = " ".join(context.args)
    with cache_lock: uids = list(user_cache.keys())
    st = await update.message.reply_text(f"Sending to {len(uids)}...")
    ok = fail = 0
    for uid in uids:
        try:
            await context.bot.send_message(int(uid), msg); ok += 1
        except Exception: fail += 1
        await asyncio.sleep(0.03)
    await st.edit_text(f"✅ Done\nOK: {ok}\nFail: {fail}")


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or not context.args: return
    try:
        tid = int(context.args[0])
        if is_admin(tid): return await update.message.reply_text("Cannot ban admin")
        ban_user(tid); await update.message.reply_text(f"🚫 Banned `{tid}`")
    except Exception: pass


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or not context.args: return
    try:
        unban_user(int(context.args[0])); await update.message.reply_text(f"✅ Unbanned `{context.args[0]}`")
    except Exception: pass


async def cmd_banned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    b = sorted(banned_set)
    await update.message.reply_text("🚫 Banned:\n" + "\n".join(f"`{u}`" for u in b) if b else "No banned users",
                                    parse_mode=ParseMode.MARKDOWN)


async def cmd_userinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or not context.args: return
    try: tid = int(context.args[0])
    except ValueError: return await update.message.reply_text("Bad ID")
    cfg = get_user_config(tid)
    await update.message.reply_text(
        f"👤 `{tid}`\nBanned: `{is_banned(tid)}`\nFirebases: `{len(cfg.get('firebase_list',[]))}`\n"
        f"Devices: `{len(cfg.get('devices',[]))}`\nGroups: `{len(cfg.get('monitored_groups',[]))}`\n"
        f"Queue: `{'ON' if cfg.get('queue_enabled',True) else 'OFF'}`",
        parse_mode=ParseMode.MARKDOWN)


async def cmd_deleteuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or not context.args: return
    try:
        uid = str(int(context.args[0]))
        with cache_lock:
            cfg = user_cache.pop(uid, None)
            if cfg:
                for g in cfg.get("monitored_groups", []):
                    gid = str(g["id"])
                    if gid in group_index:
                        group_index[gid].discard(uid)
                        if not group_index[gid]: del group_index[gid]
        conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE user_id = ?", (uid,))
        conn.commit(); conn.close()
        await update.message.reply_text(f"✅ Deleted `{uid}`")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

# ===================== CONVERSATION =====================
MENU_BUTTONS = {
    "📊 Status", "📁 Manage Firebase", "📱 Devices", "📶 SIM", "👥 Group",
    "✉️ Message Template",
    "🔔 Start Reply", "🔔 Stop Reply",
    "▶️ Start Queue", "⏸️ Stop Queue",
    "🌐 Add Public Firebase", "🔒 Add Private Firebase",
    "🗑️ Delete Firebase", "📋 Select Firebase",
    "➕ Add Group", "➖ Delete Group", "📋 Select Group", "🔙 Back",
    "🚀 Upload Numbers (.txt)",
}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    cfg = get_user_config(update.effective_user.id)
    await update.message.reply_text(
        "🔥 *SMS Queue Bot*\n"
        "Upload `.txt` of numbers → bot pushes to Firebase queue → orchestrator rotates devices.",
        reply_markup=get_main_keyboard(cfg), parse_mode=ParseMode.MARKDOWN)
    return MAIN_MENU


async def prompt_device_selection(update, context, user_cfg, msg_prefix: str = ""):
    online = get_online_devices(user_cfg)
    if not online:
        all_devs = fetch_all_devices(user_cfg)
        if not all_devs:
            txt = (f"{msg_prefix}\n\n" if msg_prefix else "") + "📱 <i>No devices found in Firebase.</i>"
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard(user_cfg))
            elif update.message:
                await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard(user_cfg))
            return
        devices = all_devs
        showing = "all"
    else:
        devices = online
        showing = "online"

    def sort_key(item):
        did, data = item
        on = 1 if is_device_online(data) else 0
        ts = 0.0
        for k in ("lastOnlineAt", "last_seen", "lastSeen"):
            ms = _to_ms_ts(data.get(k))
            if ms: ts = max(ts, ms)
        return (on, ts, did)

    sorted_devs = sorted(devices.items(), key=sort_key, reverse=True)
    page = int(context.user_data.get("device_page") or 0)
    page_size = 40
    total = len(sorted_devs)
    max_page = max(0, (total - 1) // page_size)
    page = min(page, max_page)
    context.user_data["device_page"] = page
    chunk = sorted_devs[page * page_size: page * page_size + page_size]

    kb = []
    for did, data in chunk:
        model, phone, is_on = device_display_meta(data if isinstance(data, dict) else {})
        icon = "🟢" if is_on else "⚪"
        short = did if len(did) <= 12 else did[:10] + "…"
        if model and phone:
            tail = phone[-4:] if len(phone) >= 4 else phone
            label = f"{icon} {model[:18]} ({tail}) · {short}"
        elif model:
            label = f"{icon} {model[:22]} · {short}"
        elif phone:
            label = f"{icon} {phone} · {short}"
        else:
            label = f"{icon} {did}"
        if len(label) > 60: label = label[:57] + "…"
        kb.append([InlineKeyboardButton(label, callback_data=f"device|{did}")])

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"device_page|{page-1}"))
    if page < max_page: nav.append(InlineKeyboardButton("➡️", callback_data=f"device_page|{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton("✏️ Manual Device ID", callback_data="device_manual")])
    kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="device_cancel")])
    markup = InlineKeyboardMarkup(kb)
    heading = (f"{msg_prefix}\n\n" if msg_prefix else "") + f"📱 <b>Select Device</b> ({showing}: <b>{total}</b>)"
    if update.callback_query and update.callback_query.message:
        try:
            await update.callback_query.message.edit_text(heading, parse_mode=ParseMode.HTML, reply_markup=markup)
        except Exception:
            await update.callback_query.message.reply_text(heading, parse_mode=ParseMode.HTML, reply_markup=markup)
    elif update.message:
        await update.message.reply_text(heading, parse_mode=ParseMode.HTML, reply_markup=markup)


async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    uid = update.effective_user.id
    cfg = get_user_config(uid)
    text = (update.message.text or "").strip()

    if context.user_data.get("awaiting_template"):
        if text not in MENU_BUTTONS and text:
            context.user_data.pop("awaiting_template", None)
            cfg["default_message"] = text
            save_user_config(uid, cfg)
            await update.message.reply_text("✅ Template saved.", reply_markup=get_main_keyboard(cfg))
            return MAIN_MENU
        context.user_data.pop("awaiting_template", None)

    if context.user_data.get("awaiting_device"):
        if text not in MENU_BUTTONS and text:
            context.user_data.pop("awaiting_device", None)
            did = text
            if did not in cfg["devices"]:
                cfg["devices"].append(did)
            save_user_config(uid, cfg)
            await update.message.reply_text(f"✅ Device `{did}` added to pool.", parse_mode=ParseMode.MARKDOWN,
                                            reply_markup=get_main_keyboard(cfg))
            return MAIN_MENU
        context.user_data.pop("awaiting_device", None)

    if text == "📊 Status":
        await update.message.reply_text(build_status_text(cfg), parse_mode=ParseMode.MARKDOWN,
                                        reply_markup=get_main_keyboard(cfg)); return MAIN_MENU

    if text == "📁 Manage Firebase":
        await update.message.reply_text("Firebase Management:", reply_markup=FIREBASE_SUB); return MAIN_MENU

    if text == "📱 Devices":
        await prompt_device_selection(update, context, cfg); return MAIN_MENU

    if text == "📶 SIM":
        devs = cfg.get("devices", [])
        if not devs:
            await update.message.reply_text("❌ Add a device first.", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU
        data = get_device_data(cfg, devs[0])
        sims = extract_sims(data) if data else []
        if not sims:
            await update.message.reply_text("❌ Could not fetch SIM info.", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU
        kb = [[InlineKeyboardButton(s["label"], callback_data=f"sim|{s['index']}")] for s in sims]
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="sim_cancel")])
        await update.message.reply_text("📶 Select SIM:", reply_markup=InlineKeyboardMarkup(kb)); return MAIN_MENU

    if text == "👥 Group":
        await update.message.reply_text("Group Management:", reply_markup=GROUP_SUB); return MAIN_MENU

    if text == "✉️ Message Template":
        context.user_data["awaiting_template"] = True
        await update.message.reply_text(
            "✉️ Send the SMS text to be sent to all numbers in the queue.\n"
            "Supports `{number}` placeholder (optional).\n\n(Any menu button cancels)")
        return MAIN_MENU

    if text in ("🔔 Start Reply", "🔔 Stop Reply"):
        cfg["confirm_reply"] = not cfg.get("confirm_reply", False)
        save_user_config(uid, cfg)
        await update.message.reply_text(f"Reply: {'ON' if cfg['confirm_reply'] else 'OFF'}",
                                        reply_markup=get_main_keyboard(cfg)); return MAIN_MENU

    if text in ("▶️ Start Queue", "⏸️ Stop Queue"):
        cfg["queue_enabled"] = not cfg.get("queue_enabled", True)
        save_user_config(uid, cfg)
        await update.message.reply_text(f"Queue: {'ON' if cfg['queue_enabled'] else 'OFF'}",
                                        reply_markup=get_main_keyboard(cfg)); return MAIN_MENU

    if text == "🚀 Upload Numbers (.txt)":
        if not cfg.get("firebase_list"):
            await update.message.reply_text("❌ Add a Firebase first.", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU
        if not cfg.get("devices"):
            await update.message.reply_text("❌ Add at least one device first.", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU
        await update.message.reply_text(
            "📄 Send a `.txt` file with one phone number per line.",
            parse_mode=ParseMode.MARKDOWN)
        return MAIN_MENU

    if text == "🌐 Add Public Firebase":
        await update.message.reply_text(
            "🌐 Send your Public Firebase URL:\n<code>https://myproject-default-rtdb.firebaseio.com</code>",
            parse_mode=ParseMode.HTML)
        return ADD_PUBLIC_FIREBASE

    if text == "🔒 Add Private Firebase":
        await update.message.reply_text(
            "🔒 Add Private Firebase:\n"
            "1️⃣ Upload Service Account <code>.json</code>\n"
            "2️⃣ Or send URL with <code>?auth=SECRET</code>\n"
            "3️⃣ Or send URL → then Database Secret",
            parse_mode=ParseMode.HTML)
        return ADD_PRIVATE_FIREBASE

    if text == "🗑️ Delete Firebase":
        lst = cfg.get("firebase_list", [])
        if not lst:
            await update.message.reply_text("No Firebase stored.", reply_markup=FIREBASE_SUB); return MAIN_MENU
        kb = []
        for i, fb in enumerate(lst):
            u = fb.get("url") if isinstance(fb, dict) else str(fb)
            s = fb.get("secret") if isinstance(fb, dict) else ""
            sa = fb.get("service_account") if isinstance(fb, dict) else None
            lock = "🔒 " if (s or sa) else "🌐 "
            kb.append([InlineKeyboardButton(f"❌ {lock}{u}", callback_data=f"del_fb|{i}")])
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="del_fb_cancel")])
        await update.message.reply_text("Select to delete:", reply_markup=InlineKeyboardMarkup(kb)); return MAIN_MENU

    if text == "📋 Select Firebase":
        lst = cfg.get("firebase_list", [])
        if not lst:
            await update.message.reply_text("No Firebase stored.", reply_markup=FIREBASE_SUB); return MAIN_MENU
        active = cfg.get("active_firebase_index", 0)
        kb = []
        for i, fb in enumerate(lst):
            u = fb.get("url") if isinstance(fb, dict) else str(fb)
            s = fb.get("secret") if isinstance(fb, dict) else ""
            sa = fb.get("service_account") if isinstance(fb, dict) else None
            lock = "🔒 " if (s or sa) else "🌐 "
            mark = " ✅" if i == active else ""
            kb.append([InlineKeyboardButton(f"{lock}{u}{mark}", callback_data=f"sel_fb|{i}")])
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="sel_fb_cancel")])
        await update.message.reply_text("Select active:", reply_markup=InlineKeyboardMarkup(kb)); return MAIN_MENU

    if text == "➕ Add Group":
        await update.message.reply_text("📢 Forward a message from the group/channel or send @username / invite link.")
        return ADD_GROUP

    if text == "➖ Delete Group":
        groups = cfg.get("monitored_groups", [])
        if not groups:
            await update.message.reply_text("No groups.", reply_markup=GROUP_SUB); return MAIN_MENU
        kb = [[InlineKeyboardButton(f"❌ {g.get('title') or g['id']}", callback_data=f"remove_group|{g['id']}")] for g in groups]
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="remove_cancel")])
        await update.message.reply_text("Select group:", reply_markup=InlineKeyboardMarkup(kb)); return MAIN_MENU

    if text == "📋 Select Group":
        groups = cfg.get("monitored_groups", [])
        if not groups:
            await update.message.reply_text("No groups.", reply_markup=GROUP_SUB); return MAIN_MENU
        kb = [[InlineKeyboardButton(f"📌 {g.get('title') or g['id']}", callback_data=f"sel_group|{g['id']}")] for g in groups]
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="sel_group_cancel")])
        await update.message.reply_text("Your groups:", reply_markup=InlineKeyboardMarkup(kb)); return MAIN_MENU

    if text == "🔙 Back":
        await update.message.reply_text("Main menu:", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU

    await update.message.reply_text("Use the buttons.", reply_markup=get_main_keyboard(cfg))
    return MAIN_MENU

# ===================== FIREBASE HANDLERS =====================
async def add_public_firebase_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    text = (update.message.text or "").strip()
    if text in MENU_BUTTONS: return await main_menu_handler(update, context)
    uid = update.effective_user.id
    cfg = get_user_config(uid)
    if not validate_firebase_url(text):
        await update.message.reply_text("❌ Invalid URL. Must start with https://")
        return ADD_PUBLIC_FIREBASE
    clean, _ = parse_firebase_input(text)
    cfg.setdefault("firebase_list", []).append({
        "url": clean, "secret": "", "service_account": None,
        "added_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    cfg["active_firebase_index"] = len(cfg["firebase_list"]) - 1
    save_user_config(uid, cfg)
    await update.message.reply_text(
        f"✅ Public Firebase Added\n🔗 <code>{html.escape(clean)}</code>",
        parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard(cfg))
    return MAIN_MENU


async def _save_private_and_prompt(update, context, uid, cfg, *, url, secret="",
                                   service_account_info=None, project_id="", mode_label="🔒 Private"):
    base = normalize_firebase_base(url)
    sec = clean_database_secret(secret) if secret else ""
    sa = service_account_info if isinstance(service_account_info, dict) else None
    loop = asyncio.get_running_loop()
    ok, reason = await loop.run_in_executor(None, lambda: test_firebase_auth(base, sec, sa))
    msg = update.message
    if not ok:
        await msg.reply_text(
            f"❌ Connect failed\n🔗 <code>{html.escape(base)}</code>\n⚠️ {html.escape(reason)}\n\n"
            f"Dubara Secret/JSON bhejein.",
            parse_mode=ParseMode.HTML)
        context.user_data["pending_firebase_url"] = base
        return ADD_FIREBASE_SECRET
    cfg.setdefault("firebase_list", []).append({
        "url": base,
        "secret": sec if not sa else "",
        "service_account": sa,
        "added_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    cfg["active_firebase_index"] = len(cfg["firebase_list"]) - 1
    save_user_config(uid, cfg)
    context.user_data.pop("pending_firebase_url", None)
    proj = f"👤 Project: <code>{html.escape(project_id)}</code>\n" if project_id else ""
    await msg.reply_text(
        f"✅ Private Firebase Connected!\n{proj}"
        f"🔗 <code>{html.escape(base)}</code>\n🛡️ {html.escape(mode_label)}",
        parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard(cfg))
    return MAIN_MENU


async def add_private_firebase_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    text = (update.message.text or "").strip()
    if text in MENU_BUTTONS:
        context.user_data.pop("pending_firebase_url", None)
        return await main_menu_handler(update, context)
    uid = update.effective_user.id
    cfg = get_user_config(uid)

    json_data, json_err = await extract_json_from_message(update, context)
    if json_err and update.message.document:
        await update.message.reply_text(f"❌ {json_err}\n\nValid .json upload karein.")
        return ADD_PRIVATE_FIREBASE
    if json_data:
        is_valid, pid, default_url, sa_err = parse_service_account_dict(json_data)
        if not is_valid:
            await update.message.reply_text(sa_err or "Invalid SA JSON", parse_mode=ParseMode.MARKDOWN)
            return ADD_PRIVATE_FIREBASE
        pending = context.user_data.get("pending_firebase_url") or ""
        use_url = normalize_firebase_base(pending) if pending else default_url
        return await _save_private_and_prompt(update, context, uid, cfg,
            url=use_url, service_account_info=json_data, project_id=pid or "",
            mode_label="🔒 Private (Service Account Key)")

    if not text:
        await update.message.reply_text("❌ Send URL or upload .json")
        return ADD_PRIVATE_FIREBASE

    pending = context.user_data.get("pending_firebase_url")
    if pending and not validate_firebase_url(text) and not text.startswith("{"):
        return await _save_private_and_prompt(update, context, uid, cfg,
            url=pending, secret=text, mode_label="🔒 Private (Database Secret)")

    if not validate_firebase_url(text):
        await update.message.reply_text("❌ Invalid URL.")
        return ADD_PRIVATE_FIREBASE

    clean, secret = parse_firebase_input(text)
    secret = clean_database_secret(secret)
    if secret:
        return await _save_private_and_prompt(update, context, uid, cfg,
            url=clean, secret=secret, mode_label="🔒 Private (Database Secret)")

    context.user_data["pending_firebase_url"] = clean
    await update.message.reply_text(
        f"🔐 URL saved:\n<code>{html.escape(clean)}</code>\n\n"
        f"Ab <b>Database Secret</b> bhejein ya Service Account <code>.json</code> upload karein.",
        parse_mode=ParseMode.HTML)
    return ADD_FIREBASE_SECRET


async def add_firebase_secret_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    text = (update.message.text or "").strip()
    if text in MENU_BUTTONS:
        context.user_data.pop("pending_firebase_url", None)
        return await main_menu_handler(update, context)
    uid = update.effective_user.id
    cfg = get_user_config(uid)
    url = context.user_data.get("pending_firebase_url")
    if not url:
        await update.message.reply_text("❌ Session expired. Start again.", reply_markup=get_main_keyboard(cfg))
        return MAIN_MENU
    json_data, json_err = await extract_json_from_message(update, context)
    if json_data:
        is_valid, pid, default_url, sa_err = parse_service_account_dict(json_data)
        if not is_valid:
            await update.message.reply_text(sa_err or "Invalid SA JSON", parse_mode=ParseMode.MARKDOWN)
            return ADD_FIREBASE_SECRET
        return await _save_private_and_prompt(update, context, uid, cfg,
            url=url or default_url, service_account_info=json_data, project_id=pid or "",
            mode_label="🔒 Private (Service Account Key)")
    secret = clean_database_secret(text)
    if not secret:
        await update.message.reply_text("❌ Empty secret.")
        return ADD_FIREBASE_SECRET
    return await _save_private_and_prompt(update, context, uid, cfg,
        url=url, secret=secret, mode_label="🔒 Private (Database Secret)")

# ===================== GROUP HANDLER =====================
async def add_group_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    text = (update.message.text or "").strip()
    if text in MENU_BUTTONS: return await main_menu_handler(update, context)
    uid = update.effective_user.id
    cfg = get_user_config(uid)
    msg = update.message
    target = None
    if msg.forward_origin and hasattr(msg.forward_origin, "chat"):
        target = msg.forward_origin.chat
    if target:
        gid = str(target.id); title = target.title or gid
        if any(str(g["id"]) == gid for g in cfg.get("monitored_groups", [])):
            await update.message.reply_text("❌ Already monitored.", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU
        cfg.setdefault("monitored_groups", []).append({"id": gid, "title": title})
        save_user_config(uid, cfg)
        await update.message.reply_text(f"✅ Added `{title}`", parse_mode=ParseMode.MARKDOWN,
                                        reply_markup=get_main_keyboard(cfg))
        return MAIN_MENU
    if text:
        try:
            chat = await context.bot.get_chat(text)
            if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
                gid = str(chat.id); title = chat.title or gid
                if any(str(g["id"]) == gid for g in cfg.get("monitored_groups", [])):
                    await update.message.reply_text("❌ Already monitored.", reply_markup=get_main_keyboard(cfg)); return MAIN_MENU
                cfg.setdefault("monitored_groups", []).append({"id": gid, "title": title})
                save_user_config(uid, cfg)
                await update.message.reply_text(f"✅ Added `{title}`", parse_mode=ParseMode.MARKDOWN,
                                                reply_markup=get_main_keyboard(cfg))
                return MAIN_MENU
            await update.message.reply_text("❌ Not a group/channel.", reply_markup=get_main_keyboard(cfg))
            return MAIN_MENU
        except Exception as e:
            await update.message.reply_text(f"❌ Could not resolve: {str(e)[:120]}", reply_markup=get_main_keyboard(cfg))
            return MAIN_MENU
    await update.message.reply_text("❌ Forward a message or send @username.", reply_markup=get_main_keyboard(cfg))
    return MAIN_MENU


async def awaiting_device_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return ConversationHandler.END
    text = (update.message.text or "").strip()
    if text in MENU_BUTTONS: return await main_menu_handler(update, context)
    uid = update.effective_user.id
    cfg = get_user_config(uid)
    if text not in cfg["devices"]: cfg["devices"].append(text)
    save_user_config(uid, cfg)
    await update.message.reply_text(f"✅ Device `{text}` added.", parse_mode=ParseMode.MARKDOWN,
                                    reply_markup=get_main_keyboard(cfg))
    return MAIN_MENU


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg = get_user_config(update.effective_user.id)
    await update.message.reply_text("Cancelled.", reply_markup=get_main_keyboard(cfg))
    return MAIN_MENU

# ===================== CALLBACKS =====================
async def device_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    q = update.callback_query; await q.answer()
    uid = update.effective_user.id; cfg = get_user_config(uid)
    data_s = q.data or ""
    if data_s == "device_cancel":
        context.user_data.pop("device_page", None); context.user_data.pop("awaiting_device", None)
        await q.edit_message_text("Cancelled."); return
    if data_s == "device_manual":
        context.user_data["awaiting_device"] = True
        await q.edit_message_text("📱 Send Device ID as next message."); return
    if data_s.startswith("device_page|"):
        try: context.user_data["device_page"] = int(data_s.split("|", 1)[1])
        except Exception: context.user_data["device_page"] = 0
        await prompt_device_selection(update, context, cfg); return
    if not data_s.startswith("device|"): return
    _, did = data_s.split("|", 1)
    if did not in cfg["devices"]:
        cfg["devices"].append(did)
    save_user_config(uid, cfg)
    await q.edit_message_text(f"✅ Device `{did}` added to pool.\nTotal devices: {len(cfg['devices'])}",
                              parse_mode=ParseMode.MARKDOWN)


async def sim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    q = update.callback_query; await q.answer()
    uid = update.effective_user.id; cfg = get_user_config(uid)
    if q.data == "sim_cancel":
        await q.edit_message_text("Cancelled."); return
    _, idx = q.data.split("|", 1)
    cfg["sim_index"] = int(idx); save_user_config(uid, cfg)
    await q.edit_message_text(f"📶 SIM {int(idx)+1} selected.")


async def firebase_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    q = update.callback_query; await q.answer()
    uid = update.effective_user.id; cfg = get_user_config(uid)
    if q.data in ("del_fb_cancel", "sel_fb_cancel"):
        await q.edit_message_text("Cancelled."); return
    if q.data.startswith("del_fb|"):
        idx = int(q.data.split("|")[1]); lst = cfg.get("firebase_list", [])
        if 0 <= idx < len(lst):
            it = lst[idx]; url = it.get("url") if isinstance(it, dict) else str(it)
            kb = [[InlineKeyboardButton("✅ Delete", callback_data=f"confirm_del_fb|{idx}")],
                  [InlineKeyboardButton("❌ Cancel", callback_data="del_fb_cancel")]]
            await q.edit_message_text(f"⚠️ Delete?\n`{url}`", parse_mode=ParseMode.MARKDOWN,
                                      reply_markup=InlineKeyboardMarkup(kb))
        return
    if q.data.startswith("confirm_del_fb|"):
        idx = int(q.data.split("|")[1]); lst = cfg.get("firebase_list", [])
        if 0 <= idx < len(lst):
            lst.pop(idx)
            active = cfg.get("active_firebase_index", 0)
            if idx <= active: cfg["active_firebase_index"] = max(0, active - 1) if lst else 0
            save_user_config(uid, cfg)
            await q.edit_message_text("🗑️ Deleted.")
        return
    if q.data.startswith("sel_fb|"):
        idx = int(q.data.split("|")[1]); lst = cfg.get("firebase_list", [])
        if 0 <= idx < len(lst):
            cfg["active_firebase_index"] = idx; save_user_config(uid, cfg)
            await q.edit_message_text("✅ Active Firebase updated.")


async def group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    q = update.callback_query; await q.answer()
    uid = update.effective_user.id; cfg = get_user_config(uid)
    if q.data in ("remove_cancel", "sel_group_cancel"):
        await q.edit_message_text("Cancelled."); return
    if q.data.startswith("remove_group|"):
        gid = q.data.split("|", 1)[1]
        kb = [[InlineKeyboardButton("✅ Remove", callback_data=f"confirm_remove|{gid}")],
              [InlineKeyboardButton("❌ Cancel", callback_data="remove_cancel")]]
        await q.edit_message_text("⚠️ Stop monitoring?", reply_markup=InlineKeyboardMarkup(kb)); return
    if q.data.startswith("confirm_remove|"):
        gid = q.data.split("|", 1)[1]
        cfg["monitored_groups"] = [g for g in cfg.get("monitored_groups", []) if str(g["id"]) != str(gid)]
        save_user_config(uid, cfg)
        await q.edit_message_text("✅ Removed."); return
    if q.data.startswith("sel_group|"):
        await q.edit_message_text("📌 Monitored.")

# ===================== TXT UPLOAD =====================
async def txt_upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    uid = update.effective_user.id
    cfg = get_user_config(uid)
    msg = update.message
    if not msg.document: return
    doc = msg.document
    fname = (doc.file_name or "").lower()
    if not (fname.endswith(".txt") or fname.endswith(".csv")):
        await msg.reply_text("❌ Only .txt / .csv files accepted.")
        return
    try:
        file = await context.bot.get_file(doc.file_id)
        raw = (await file.download_as_bytearray()).decode("utf-8", errors="ignore")
    except Exception as e:
        await msg.reply_text(f"❌ Download failed: {e}")
        return
    numbers = parse_numbers_from_text(raw)
    if not numbers:
        await msg.reply_text("❌ No valid phone numbers found in file.")
        return
    context.user_data["pending_numbers"] = numbers
    preview = "\n".join(numbers[:10])
    await msg.reply_text(
        f"📄 Parsed <b>{len(numbers)}</b> numbers (deduped).\n\n"
        f"Preview:\n<code>{html.escape(preview)}</code>\n"
        f"{'…' if len(numbers) > 10 else ''}\n\n"
        f"✉️ Template: <code>{html.escape(cfg.get('default_message') or '(not set)')}</code>\n"
        f"📱 Devices in pool: <b>{len(cfg.get('devices', []))}</b>\n"
        f"🔄 Rotate every: <b>{BATCH_SIZE}</b> sends\n\n"
        f"Confirm push to Firebase queue?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Push to Queue", callback_data="push_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="push_cancel")],
        ])
    )


async def push_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    q = update.callback_query; await q.answer()
    uid = update.effective_user.id; cfg = get_user_config(uid)
    numbers = context.user_data.get("pending_numbers") or []
    if not numbers:
        await q.edit_message_text("❌ Session expired."); return
    template = cfg.get("default_message") or ""
    if not template:
        await q.edit_message_text("❌ Set a message template first (✉️ Message Template)."); return
    if not cfg.get("devices"):
        await q.edit_message_text("❌ Add at least one device first."); return
    await q.edit_message_text(f"⏳ Pushing {len(numbers)} numbers to Firebase queue...")
    ok, reason, pushed = await push_numbers_to_queue(cfg, numbers, template, cfg["devices"][0])
    context.user_data.pop("pending_numbers", None)
    if ok:
        await q.edit_message_text(
            f"✅ <b>{pushed}/{len(numbers)}</b> numbers pushed to queue.\n"
            f"Orchestrator will start sending within {POLL_INTERVAL}s.",
            parse_mode=ParseMode.HTML)
    else:
        await q.edit_message_text(f"❌ Push failed: {reason}")


async def push_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_access(update, context): return
    q = update.callback_query; await q.answer()
    context.user_data.pop("pending_numbers", None)
    await q.edit_message_text("Cancelled.")

# ===================== GROUP MESSAGE (legacy) =====================
async def group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg: return
    chat = update.effective_chat
    chat_id = str(chat.id)
    text = msg.text or msg.caption or ""
    update_group_title(chat_id, chat.title or chat_id)
    to_num, sms = parse_message(text)
    if not to_num or not sms: return
    if not validate_phone_number(to_num): return
    with cache_lock:
        user_ids = list(group_index.get(chat_id, set()))
    for uid_str in user_ids:
        if uid_str in banned_set: continue
        with cache_lock:
            cfg = copy.deepcopy(user_cache.get(uid_str))
        if not cfg: continue
        if not cfg.get("queue_enabled", True): continue
        if not cfg.get("devices"): continue
        asyncio.create_task(push_numbers_to_queue(cfg, [to_num], sms, cfg["devices"][0]))

# ===================== ORCHESTRATOR =====================
class DeviceRotator:
    def __init__(self):
        self._counters: Dict[str, int] = {}
        self._current_idx: Dict[str, int] = {}
        self._lock = threading.Lock()

    def pick_device(self, uid: str, devices: List[str]) -> str:
        if not devices: return ""
        with self._lock:
            cnt = self._counters.get(uid, 0)
            idx = self._current_idx.get(uid, 0)
            if cnt >= BATCH_SIZE:
                idx = (idx + 1) % len(devices)
                self._current_idx[uid] = idx
                self._counters[uid] = 0
                logger.info(f"🔄 Rotated user {uid} → device index {idx} ({devices[idx]})")
            dev = devices[idx]
            self._counters[uid] = self._counters.get(uid, 0) + 1
            return dev

    def reset(self, uid: str):
        with self._lock:
            self._counters.pop(uid, None)
            self._current_idx.pop(uid, None)


rotator = DeviceRotator()


def fetch_pending_jobs_sync(user_cfg: dict, limit: int = 30) -> List[Tuple[str, dict]]:
    url, headers = get_firebase_request_params(user_cfg, "sms_queue.json")
    if not url: return []
    blob = fetch_json(url, headers=headers, timeout=15)
    if not isinstance(blob, dict): return []
    out = []
    for key, job in blob.items():
        if not isinstance(job, dict): continue
        if job.get("status", "pending") == "pending":
            out.append((str(key), job))
        if len(out) >= limit: break
    return out


def mark_job_sent_sync(user_cfg: dict, job_key: str, device_id: str) -> Tuple[bool, str]:
    url, headers = get_firebase_request_params(user_cfg, f"sms_queue/{job_key}.json")
    if not url: return False, "no url"
    return patch_json(url, {"status": "sent", "device_id": device_id,
                            "sent_at": int(time.time() * 1000)}, headers=headers, timeout=8)


def mark_job_failed_sync(user_cfg: dict, job_key: str, reason: str, attempts: int) -> Tuple[bool, str]:
    url, headers = get_firebase_request_params(user_cfg, f"sms_queue/{job_key}.json")
    if not url: return False, "no url"
    return patch_json(url, {
        "status": "failed" if attempts >= 3 else "pending",
        "last_error": reason[:200],
        "attempts": attempts + 1,
        "last_attempt_at": int(time.time() * 1000),
    }, headers=headers, timeout=8)


async def orchestrator_tick(context: ContextTypes.DEFAULT_TYPE):
    with cache_lock:
        snapshot = [(uid, copy.deepcopy(cfg)) for uid, cfg in user_cache.items()]
    for uid, cfg in snapshot:
        if uid in banned_set: continue
        if not cfg.get("queue_enabled", True): continue
        devices = cfg.get("devices", [])
        if not devices: continue
        if not cfg.get("firebase_list"): continue
        loop = asyncio.get_running_loop()
        try:
            jobs = await loop.run_in_executor(None, fetch_pending_jobs_sync, cfg, 30)
        except Exception as e:
            logger.warning(f"orchestrator fetch {uid}: {e}")
            continue
        if not jobs: continue
        logger.info(f"🚀 user {uid} has {len(jobs)} pending job(s)")
        for job_key, job in jobs:
            to = str(job.get("to") or "").strip()
            msg_text = str(job.get("message") or "").strip()
            attempts = int(job.get("attempts") or 0)
            if not to or not msg_text:
                await loop.run_in_executor(None, mark_job_failed_sync, cfg, job_key, "empty to/message", attempts)
                continue
            if "{number}" in msg_text:
                msg_text = msg_text.replace("{number}", to)
            device = rotator.pick_device(uid, devices)
            if not device: continue
            ok, reason, _ = await send_sms_async(cfg, device, to, msg_text)
            if ok:
                await loop.run_in_executor(None, mark_job_sent_sync, cfg, job_key, device)
                logger.info(f"✅ job {job_key} → {to} via {device}")
            else:
                await loop.run_in_executor(None, mark_job_failed_sync, cfg, job_key, reason, attempts)
                logger.warning(f"❌ job {job_key} → {to}: {reason}")
                rotator.reset(uid)
            await asyncio.sleep(1.2)

# ===================== AUTO-CLEAN =====================
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = update.my_chat_member
    if not r: return
    old = r.old_chat_member.status; new = r.new_chat_member.status
    if new in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED) and old in (
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.RESTRICTED):
        remove_group_from_all_users(str(r.chat.id))

# ===================== ERROR =====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}", exc_info=context.error)

# ===================== PANEL WATCH =====================
async def watch_panel_changes(context: ContextTypes.DEFAULT_TYPE):
    global _settings_mtime, _db_mtime
    try:
        if os.path.isfile(RELOAD_FLAG):
            try: os.remove(RELOAD_FLAG)
            except Exception: pass
            info = reload_runtime_config(); load_cache()
            logger.info(f"🔄 reload flag → {info}"); return
        need_cfg = need_db = False
        try:
            if os.path.isfile(SETTINGS_FILE):
                mt = os.path.getmtime(SETTINGS_FILE)
                if mt > _settings_mtime + 0.01: need_cfg = True
        except Exception: pass
        try:
            if os.path.isfile(DB_FILE):
                mt = os.path.getmtime(DB_FILE)
                if _db_mtime <= 0: _db_mtime = mt
                elif mt > _db_mtime + 0.01: need_db = True; _db_mtime = mt
        except Exception: pass
        if need_cfg:
            info = reload_runtime_config(); logger.info(f"🔄 settings.json → {info}")
        if need_db:
            load_cache(); logger.info("🔄 db changed → cache reloaded")
    except Exception as e:
        logger.warning(f"watch: {e}")

# ===================== MAIN =====================
async def main():
    info = reload_runtime_config()
    init_db(); load_cache()
    try:
        global _db_mtime
        _db_mtime = os.path.getmtime(DB_FILE) if os.path.isfile(DB_FILE) else 0.0
    except Exception:
        _db_mtime = 0.0
    logger.info(f"⚙️ token={'yes' if BOT_TOKEN else 'NO'} admins={ADMIN_IDS} batch={BATCH_SIZE}")
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN required"); sys.exit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler)],
            ADD_PUBLIC_FIREBASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_public_firebase_handler)],
            ADD_PRIVATE_FIREBASE: [MessageHandler((filters.TEXT | filters.Document.ALL) & ~filters.COMMAND, add_private_firebase_handler)],
            ADD_FIREBASE_SECRET: [MessageHandler((filters.TEXT | filters.Document.ALL) & ~filters.COMMAND, add_firebase_secret_handler)],
            ADD_GROUP: [MessageHandler(filters.ALL & ~filters.COMMAND, add_group_handler)],
            AWAITING_DEVICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, awaiting_device_handler)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv)

    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(CommandHandler("banned", cmd_banned))
    app.add_handler(CommandHandler("userinfo", cmd_userinfo))
    app.add_handler(CommandHandler("deleteuser", cmd_deleteuser))

    app.add_handler(CallbackQueryHandler(device_callback, pattern=r"^device"))
    app.add_handler(CallbackQueryHandler(sim_callback, pattern=r"^sim"))
    app.add_handler(CallbackQueryHandler(firebase_callback, pattern=r"^(del_fb|confirm_del_fb|sel_fb)"))
    app.add_handler(CallbackQueryHandler(group_callback, pattern=r"^(remove_group|confirm_remove|sel_group|remove_cancel|sel_group_cancel)"))
    app.add_handler(CallbackQueryHandler(push_confirm_callback, pattern=r"^push_confirm$"))
    app.add_handler(CallbackQueryHandler(push_cancel_callback, pattern=r"^push_cancel$"))

    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.Document.ALL & ~filters.COMMAND, txt_upload_handler))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & (
            filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP | filters.ChatType.CHANNEL),
        group_message))
    app.add_handler(MessageHandler(
        filters.CAPTION & ~filters.COMMAND & (
            filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP | filters.ChatType.CHANNEL),
        group_message))

    if app.job_queue:
        app.job_queue.run_repeating(orchestrator_tick, interval=POLL_INTERVAL, first=6)
        app.job_queue.run_repeating(watch_panel_changes, interval=4, first=3)
        logger.info(f"⏱️ Orchestrator every {POLL_INTERVAL}s | watch 4s")
    else:
        logger.warning("job_queue missing — install python-telegram-bot[job-queue]")

    app.add_error_handler(error_handler)

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
    logger.info("✅ SMS Queue Bot running")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError: pass
    await stop_event.wait()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    logger.info("Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e).lower():
            loop = asyncio.get_event_loop()
            loop.create_task(main())
            try: loop.run_forever()
            except KeyboardInterrupt: pass
        else:
            raise