#!/bin/bash
# =========================================================
#  Telegram Bot — VPS Setup Script
#  Run once on a fresh Ubuntu/Debian VPS:
#    chmod +x setup.sh && sudo bash setup.sh
# =========================================================

set -e

BOT_USER="botuser"
BOT_DIR="/opt/telegram-bot"
SERVICE_NAME="telegram-bot"

echo "==> [1/6] Updating system packages..."
apt-get update -y && apt-get upgrade -y

echo "==> [2/6] Installing Python 3.11 and tools..."
apt-get install -y python3.11 python3.11-venv python3-pip git curl

echo "==> [3/6] Creating bot user and directory..."
id "$BOT_USER" &>/dev/null || useradd -r -s /bin/false "$BOT_USER"
mkdir -p "$BOT_DIR"
chown "$BOT_USER":"$BOT_USER" "$BOT_DIR"

echo "==> [4/6] Copying bot files..."
cp telegram_bot_simple.py "$BOT_DIR/"
cp requirements.txt "$BOT_DIR/"

echo "==> [5/6] Setting up Python virtual environment and installing dependencies..."
python3.11 -m venv "$BOT_DIR/venv"
"$BOT_DIR/venv/bin/pip" install --upgrade pip
"$BOT_DIR/venv/bin/pip" install -r "$BOT_DIR/requirements.txt"

echo "==> [6/6] Installing systemd service..."
cp telegram-bot.service /etc/systemd/system/"$SERVICE_NAME".service
systemd-analyze verify /etc/systemd/system/"$SERVICE_NAME".service || true
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

echo ""
echo "=========================================================="
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. Edit the .env file:  nano $BOT_DIR/.env"
echo "     Fill in your 4 required secrets (see .env.example)"
echo "  2. Start the bot:       systemctl start $SERVICE_NAME"
echo "  3. Check status:        systemctl status $SERVICE_NAME"
echo "  4. View live logs:      journalctl -u $SERVICE_NAME -f"
echo "=========================================================="
