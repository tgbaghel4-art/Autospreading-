# Multi-User Auto SMS Bot

Reply-keyboard menu, per-user SQLite save, 5 SMS per device then switch.

## Menu
- 🔥 Add Firebase — URL list (txt / paste), optional URL|SECRET
- 👥 Recipient Numbers — numbers as-is (no +91)
- 💬 Custom SMS — message text
- ▶️ Start SMS / ⏹ Stop SMS
- 📊 Status / 🔄 Rescan / 📱 Use All Devices
- ❓ How to use / 🗑 Clear Saved

## Rule
Device A sends first 5 recipients → Device B next 5 → Device C next 5 …

## Run
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="..."
python bot.py
```
