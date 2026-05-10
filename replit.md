# Telegram Bot — Bakong KHQR Payments

## Overview
A Python Telegram bot that accepts orders, generates Bakong KHQR payment QR codes, and tracks state in a Neon Postgres database via its HTTP `/sql` API. Single-file implementation in `telegram_bot_simple.py`.

## Stack
- **Python 3.11**
- **Pyrogram** (MTProto client — not Bot API HTTP polling)
- **TgCrypto** (fast encryption for Pyrogram)
- `bakong-khqr`, `requests`, `pillow`, `qrcode`, `urllib3`
- Neon Postgres (HTTP API, no driver required)

## Architecture
| Feature | Implementation |
|---|---|
| Transport | Pyrogram MTProto (not Bot API polling) |
| Concurrency | Full `asyncio` — no threads |
| Per-user safety | `asyncio.Lock` per user ID |
| Global data lock | `asyncio.Lock` |
| Blocking DB/HTTP calls | `asyncio.to_thread` (`run_sync`) |
| Background tasks | `asyncio.create_task` |
| Handler priority | Pyrogram `group=` parameter |
| In-memory cache | `MemCache` (TTL-based, in-process) |
| Pre-handler filters | Pyrogram custom `filters.create` |

### Handler Groups (priority — lower = higher)
| Group | Purpose |
|---|---|
| `-10` | Channel posts |
| `-5` | Maintenance mode blocker |
| `0` | `/start`, `/cancel` commands |
| `1` | Admin ⚙️ settings keyboard button |
| `2` | Admin pending input states (`admin_input:*`) |
| `3` | Admin `delete_type_select/confirm`, `broadcast_confirm` |
| `4` | Admin keyboard button labels (all `BTN_*` constants) |
| `5` | `payment_pending` guard (anyone) |
| `6` | Admin account-management session states |
| `7` | Non-admin fallback |

### Custom Filters
- `admin_filter` — passes if `from_user.id` is admin
- `maintenance_block_filter` — passes when maintenance ON and user is NOT admin
- `has_admin_input_filter` — passes when user has `admin_input:*` session state
- `admin_button_filter` — passes when text is an admin button label
- `payment_pending_filter` — passes when user has `payment_pending` session state
- `delete_type_select_filter`, `delete_type_confirm_filter`, `broadcast_confirm_filter` — specific state filters

## Required Secrets
Stored in Replit Secrets:
- `TELEGRAM_BOT_TOKEN` — from BotFather
- `TELEGRAM_API_ID` — from https://my.telegram.org (required by Pyrogram)
- `TELEGRAM_API_HASH` — from https://my.telegram.org (required by Pyrogram)
- `BAKONG_TOKEN` — Bakong KHQR API token
- `NEON_DATABASE_URL` — Neon Postgres connection string

## Run
The `Telegram Bot` workflow runs `python3 telegram_bot_simple.py`.
Pyrogram handles the MTProto connection automatically — no webhook management needed.

## Session File
`bot_session.session` is created in the project root on first run. Pyrogram stores its MTProto session there.

## 🖥️ Deploy to VPS (24/7 via systemd)

### Files included for VPS deployment
| File | Purpose |
|---|---|
| `setup.sh` | One-time setup script (run as root on Ubuntu/Debian) |
| `telegram-bot.service` | systemd service — auto-start, auto-restart on crash |
| `.env.example` | Template for environment variables |

### Step-by-step (Termius / any SSH client)

```bash
# 1. Upload files to VPS (run from your local machine or Replit shell)
scp telegram_bot_simple.py requirements.txt setup.sh telegram-bot.service .env.example root@YOUR_VPS_IP:/root/

# 2. SSH into VPS
ssh root@YOUR_VPS_IP

# 3. Run setup (installs Python, venv, dependencies, registers systemd service)
chmod +x setup.sh && sudo bash setup.sh

# 4. Create your .env file from the template
cp /root/.env.example /opt/telegram-bot/.env
nano /opt/telegram-bot/.env   # fill in your 4 secrets

# 5. Start the bot
systemctl start telegram-bot

# 6. Check it's running
systemctl status telegram-bot

# 7. Watch live logs
journalctl -u telegram-bot -f
```

### Useful commands
```bash
systemctl stop telegram-bot        # Stop bot
systemctl restart telegram-bot     # Restart bot
systemctl disable telegram-bot     # Disable auto-start on boot
journalctl -u telegram-bot -n 100  # Last 100 log lines
```

### ⚠️ Important notes
- **Do NOT copy** `bot_session.session` — it is tied to the machine. The systemd service deletes it automatically before each start.
- **4 secrets required** in `/opt/telegram-bot/.env`: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `NEON_DATABASE_URL`
- **BAKONG_TOKEN** is optional — loaded from the database if not set (admin panel ⚙️ Settings → 🔑 Bakong Token).
- **Database data is preserved** — Neon Postgres is cloud-hosted. All data stays intact across VPS migrations.

## Admin-Managed Settings (persisted in `bot_settings` DB table)
| Key | Description |
|---|---|
| `PAYMENT_NAME` | Merchant name shown on KHQR |
| `MAINTENANCE_MODE` | `true`/`false` — blocks non-admin users |
| `BAKONG_RELAY_TOKEN` | Relay token (takes priority) |
| `BAKONG_API_TOKEN` | Direct Bakong JWT token |
| `TELEGRAM_CHANNEL_ID` | Notification channel |
| `EXTRA_ADMIN_IDS` | JSON array of additional admin user IDs |

## Primary Admin
Hardcoded: `ADMIN_ID = 5002402843`. Additional admins managed via the ⚙️ settings menu.
