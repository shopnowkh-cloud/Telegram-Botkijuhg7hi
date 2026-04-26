# Telegram Bot — Bakong KHQR Payments

## Overview
A Python Telegram bot that accepts orders, generates Bakong KHQR payment QR codes, and tracks state in a Neon Postgres database via its HTTP `/sql` API. Single-file implementation in `telegram_bot_simple.py`.

## Stack
- Python 3.11
- `python-telegram-bot`, `requests`, `pillow`, `qrcode`, `bakong-khqr`, `urllib3`
- Neon Postgres (HTTP API, no driver required)
- Telegram Bot API (polling mode)

## Required Secrets
Stored in Replit Secrets:
- `TELEGRAM_BOT_TOKEN` — from BotFather
- `BAKONG_TOKEN` — Bakong KHQR API token
- `NEON_DATABASE_URL` — Neon Postgres connection string (used with the `/sql` HTTP endpoint)
- `TELEGRAM_CHANNEL_ID` — channel for notifications (optional)

## Run
The `Telegram Bot` workflow runs `python3 telegram_bot_simple.py`. The bot deletes any existing webhook on startup and switches to long polling.

## Deployment
Configured for autoscale deployment, but as a long-running polling bot it's better suited to Replit's reserved-VM (background worker) deployment type.

## Notes
- Admin-managed settings (`PAYMENT_NAME`, `MAINTENANCE_MODE`, `BAKONG_TOKEN`, `TELEGRAM_CHANNEL_ID`, extra admin IDs, start banner file ID) persist in the `bot_settings` table and override env values at startup.
- Primary admin is hardcoded as `ADMIN_ID = 5002402843`; additional admins are managed via the `/admin` command.
