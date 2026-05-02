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
