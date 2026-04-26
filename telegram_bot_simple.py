#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import logging
import sys
import json
import os
import io
import threading
import hashlib
import fcntl
import re
import html
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.parse import quote as url_quote
from bakong_khqr import KHQR

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
KHMER_MESSAGE = "ជ្រើសរើស Account ដើម្បីបញ្ជាទិញ"
ADMIN_ID = 5002402843

# Additional admin user IDs (loaded from Neon at startup, managed via /admin).
# ADMIN_ID is always implicitly an admin and is the destination for notifications.
EXTRA_ADMIN_IDS = set()

def is_admin(uid):
    """Return True if uid is the primary admin or in the extra-admin set."""
    try:
        uid_int = int(uid)
    except (TypeError, ValueError):
        return False
    return uid_int == ADMIN_ID or uid_int in EXTRA_ADMIN_IDS
CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Persistent HTTP session — reuses TCP connections for faster Telegram API calls
http = requests.Session()
http.headers.update({'Connection': 'keep-alive'})
_retry_strategy = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_http_adapter = HTTPAdapter(
    max_retries=_retry_strategy,
    pool_connections=20,
    pool_maxsize=50,
)
http.mount("https://", _http_adapter)
http.mount("http://", _http_adapter)
worker_pool = ThreadPoolExecutor(max_workers=16)
background_pool = ThreadPoolExecutor(max_workers=8)
_data_lock = threading.RLock()

# Bakong KHQR configuration — token loaded from secret
BAKONG_TOKEN = os.environ.get("BAKONG_TOKEN", "")
khqr_client = KHQR(BAKONG_TOKEN)

# Payment merchant name (changeable by admin via /payment <name>)
PAYMENT_NAME = "RADY"

# Maintenance mode flag (admin /update on | /update off)
MAINTENANCE_MODE = False

# QR Code expiry timeout (seconds)
PAYMENT_TIMEOUT_SECONDS = 2 * 60

# ── Manual KHQR builder (fallback when library generates invalid strings) ──
def _crc16_ccitt(data: str) -> str:
    """CRC16-CCITT-FALSE: poly=0x1021, init=0xFFFF, no reflection."""
    crc = 0xFFFF
    for ch in data:
        crc ^= ord(ch) << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if (crc & 0x8000) else (crc << 1)
            crc &= 0xFFFF
    return f"{crc:04X}"

def _tlv(tag: str, value: str) -> str:
    return f"{tag}{len(value):02d}{value}"

def _build_khqr_manual(bank_account, merchant_name, merchant_city,
                        amount, bill_number, phone, store_label, terminal_label):
    """Build a valid KHQR EMV string with correct CRC16, bypassing the library."""
    # Phone: 85593330905 → 093330905
    if phone.startswith('855'):
        phone_local = '0' + phone[3:]
    else:
        phone_local = phone[-9:] if len(phone) > 9 else phone

    # Additional data (tag 62)
    add_data = (
        _tlv("03", store_label) +
        _tlv("02", phone_local) +
        _tlv("01", bill_number) +
        _tlv("07", terminal_label)
    )

    # Merchant info (tag 99): current time + expiry in milliseconds
    now_ms  = str(int(time.time() * 1000))
    exp_ms  = str(int((time.time() + 86400) * 1000))   # +1 day
    info_data = _tlv("00", now_ms) + _tlv("01", exp_ms)

    body = (
        _tlv("00", "01") +
        _tlv("01", "12") +
        _tlv("29", _tlv("00", bank_account)) +
        _tlv("52", "5999") +
        _tlv("53", "840") +
        _tlv("54", f"{amount:.2f}") +
        _tlv("58", "KH") +
        _tlv("59", merchant_name) +
        _tlv("60", merchant_city) +
        _tlv("62", add_data) +
        _tlv("99", info_data) +
        "6304"
    )
    return body + _crc16_ccitt(body)

def generate_payment_qr(amount):
    """Generate QR code using bakong-khqr library. Returns (img_bytes, md5) or (None, error_msg) on failure."""
    # Check token is present
    if not BAKONG_TOKEN:
        msg = "BAKONG_TOKEN មិនមានក្នុង environment"
        logger.error(msg)
        return None, msg, None
    try:
        bill_number = f"TRX{int(time.time())}"
        # Step 1: generate the KHQR string (local, no network)
        try:
            try:
                qr = khqr_client.create_qr(
                    bank_account='sovannrady@aclb',
                    merchant_name=PAYMENT_NAME,
                    merchant_city='KPS',
                    amount=amount,
                    currency='USD',
                    store_label=PAYMENT_NAME,
                    phone_number='85593330905',
                    bill_number=bill_number,
                    terminal_label='Cashier-01',
                    static=False,
                    expiration=1
                )
                logger.info("create_qr with expiration=1 succeeded")
            except TypeError:
                qr = khqr_client.create_qr(
                    bank_account='sovannrady@aclb',
                    merchant_name=PAYMENT_NAME,
                    merchant_city='KPS',
                    amount=amount,
                    currency='USD',
                    store_label=PAYMENT_NAME,
                    phone_number='85593330905',
                    bill_number=bill_number,
                    terminal_label='Cashier-01',
                    static=False
                )
                logger.info("create_qr without expiration succeeded (older library)")
            logger.info(f"KHQR string created, length={len(qr)}, start={qr[:40]}")
            # Validate required EMV fields: currency (5303840) and amount (5404)
            if '5303840' not in qr or '5404' not in qr:
                logger.warning(f"Library KHQR missing currency/amount — using manual builder")
                qr = _build_khqr_manual(
                    bank_account='sovannrady@aclb',
                    merchant_name=PAYMENT_NAME,
                    merchant_city='KPS',
                    amount=amount,
                    bill_number=bill_number,
                    phone='85593330905',
                    store_label=PAYMENT_NAME,
                    terminal_label='Cashier-01'
                )
                logger.info(f"Manual KHQR built, length={len(qr)}, start={qr[:40]}")
        except Exception as e:
            msg = f"create_qr failed: {type(e).__name__}: {e}"
            logger.error(msg)
            return None, msg, None
        # Step 2: compute MD5 locally (hashlib.md5 of the QR string — same as the library)
        md5 = compute_md5(qr)
        logger.info(f"MD5 computed: {md5}")
        # Step 3: generate image with 3-layer fallback
        img_bytes = None
        # Layer 1: bakong-khqr library's styled image (requires Pillow)
        try:
            img_bytes = khqr_client.qr_image(qr, format='bytes')
            logger.info("QR image generated via bakong-khqr library")
        except Exception as e1:
            logger.warning(f"bakong-khqr image failed ({type(e1).__name__}: {e1}), trying qrcode library")
        # Layer 2: qrcode library directly
        if not img_bytes:
            try:
                import qrcode
                qr_img = qrcode.make(qr)
                buf = io.BytesIO()
                qr_img.save(buf, format='PNG')
                img_bytes = buf.getvalue()
                logger.info("QR image generated via qrcode library")
            except Exception as e2:
                logger.warning(f"qrcode library failed ({type(e2).__name__}: {e2}), trying API fallback")
        # Layer 3: free online QR API (no libraries needed)
        if not img_bytes:
            try:
                qr_api_url = f"https://api.qrserver.com/v1/create-qr-code/?size=500x500&data={url_quote(qr)}"
                resp = http.get(qr_api_url, timeout=10)
                resp.raise_for_status()
                img_bytes = resp.content
                logger.info("QR image generated via qrserver.com API")
            except Exception as e3:
                msg = f"all 3 QR image methods failed. Last: {type(e3).__name__}: {e3}"
                logger.error(msg)
                return None, msg, None
        logger.info(f"Generated KHQR for amount ${amount}, bill {bill_number}, md5 {md5}, size {len(img_bytes)}b")
        return img_bytes, md5, qr
    except Exception as e:
        msg = f"Unexpected: {type(e).__name__}: {e}"
        logger.error(f"Failed to generate payment QR: {msg}")
        return None, msg, None

def _bakong_api_url():
    """Return correct Bakong API base URL based on token prefix."""
    if BAKONG_TOKEN and BAKONG_TOKEN.startswith("rbk"):
        return "https://api.bakongrelay.com/v1"
    return "https://api-bakong.nbc.gov.kh/v1"

def compute_md5(qr: str) -> str:
    """Compute MD5 of KHQR string locally (same algorithm the library uses)."""
    import hashlib
    return hashlib.md5(qr.encode('utf-8')).hexdigest()

def check_payment_status(md5):
    """Check payment directly against Bakong relay API — no library dependency.
    Returns (is_paid: bool, payment_data: dict or None)."""
    try:
        base = _bakong_api_url()
        resp = http.post(
            f"{base}/check_transaction_by_md5",
            json={"md5": md5},
            headers={
                "Authorization": f"Bearer {BAKONG_TOKEN}",
                "Content-Type": "application/json",
            },
            timeout=10
        )
        data = resp.json()
        logger.info(f"check_payment response: status={resp.status_code} body={data}")
        if data.get("responseCode") == 0:
            return True, data.get("data", {})
        return False, None
    except Exception as e:
        logger.error(f"Failed to check payment status: {type(e).__name__}: {e}")
    return False, None

NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL", "")
_neon_host = urlparse(NEON_DATABASE_URL).hostname if NEON_DATABASE_URL else ""
_neon_api_url = f"https://{_neon_host}/sql"
_neon_headers = {
    'Neon-Connection-String': NEON_DATABASE_URL,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def _neon_query(query, params=None):
    """Execute a SQL query via Neon HTTP API. Works on any platform (Vercel, Replit, etc.)"""
    body = {'query': query}
    if params:
        body['params'] = [str(p) if p is not None else None for p in params]
    resp = http.post(_neon_api_url, headers=_neon_headers, json=body, timeout=15)
    resp.raise_for_status()
    return resp.json()

def _init_db():
    """Create tables if they don't exist."""
    try:
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_accounts (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'
            )
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_sessions (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'
            )
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_pending_payments (
                user_id BIGINT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                account_type TEXT,
                quantity INT,
                total_price NUMERIC,
                md5_hash TEXT,
                qr_message_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_purchase_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                account_type TEXT,
                quantity INT,
                total_price NUMERIC,
                accounts JSONB DEFAULT '[]',
                purchased_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        _neon_query("""
            ALTER TABLE bot_purchase_history
            ADD COLUMN IF NOT EXISTS accounts JSONB DEFAULT '[]'
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_known_users (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                first_seen TIMESTAMPTZ DEFAULT NOW(),
                last_seen TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        _neon_query("""
            ALTER TABLE bot_known_users
            ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_sent_verifications (
                email TEXT NOT NULL,
                code TEXT NOT NULL,
                first_sent_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (email, code)
            )
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_scheduled_deletions (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                delete_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (chat_id, message_id)
            )
        """)
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_email_buyer_map (
                email TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                account_type TEXT,
                purchased_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Backfill any historical buyers into the known-users table so /users
        # never loses anyone on a Vercel cold restart. They already bought, so
        # mark admin_notified=TRUE to avoid spamming the admin about them.
        _neon_query("""
            INSERT INTO bot_known_users (user_id, first_seen, last_seen, admin_notified)
            SELECT DISTINCT user_id, MIN(purchased_at), MAX(purchased_at), TRUE
            FROM bot_purchase_history
            GROUP BY user_id
            ON CONFLICT (user_id) DO UPDATE SET admin_notified = TRUE
        """)
        # Backfill bot_email_buyer_map from all existing purchase history rows.
        # Use DISTINCT ON to keep only the most-recent purchase per email,
        # avoiding "ON CONFLICT DO UPDATE affects row a second time" errors.
        _neon_query("""
            INSERT INTO bot_email_buyer_map (email, user_id, account_type, purchased_at)
            SELECT DISTINCT ON (acc->>'email')
                acc->>'email'   AS email,
                user_id::BIGINT,
                account_type,
                purchased_at
            FROM bot_purchase_history,
                 jsonb_array_elements(
                     CASE jsonb_typeof(accounts)
                         WHEN 'array' THEN accounts
                         ELSE '[]'::jsonb
                     END
                 ) AS acc
            WHERE acc->>'email' IS NOT NULL
              AND acc->>'email' <> ''
            ORDER BY acc->>'email', purchased_at DESC
            ON CONFLICT (email) DO UPDATE
                SET user_id      = EXCLUDED.user_id,
                    account_type = EXCLUDED.account_type,
                    purchased_at = EXCLUDED.purchased_at
        """)
        r = _neon_query("SELECT COUNT(*) as cnt FROM bot_accounts")
        if int(r['rows'][0]['cnt']) == 0:
            _neon_query("INSERT INTO bot_accounts (data) VALUES ($1)",
                        [json.dumps({'accounts': [], 'account_types': {}, 'prices': {}})])
        r = _neon_query("SELECT COUNT(*) as cnt FROM bot_sessions")
        if int(r['rows'][0]['cnt']) == 0:
            _neon_query("INSERT INTO bot_sessions (data) VALUES ($1)", [json.dumps({})])
        logger.info("Replit PostgreSQL DB initialized")
    except Exception as e:
        logger.error(f"DB init failed: {e}")

def get_setting(key, default=None):
    """Read a single key from bot_settings; returns default if missing."""
    try:
        r = _neon_query("SELECT value FROM bot_settings WHERE key = $1", [key])
        rows = r.get('rows', []) or []
        if rows:
            return rows[0].get('value')
    except Exception as e:
        logger.error(f"Failed to read setting {key}: {e}")
    return default

def set_setting(key, value):
    """Upsert a key/value into bot_settings so it survives cold restarts."""
    try:
        _neon_query("""
            INSERT INTO bot_settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, [key, str(value)])
    except Exception as e:
        logger.error(f"Failed to save setting {key}: {e}")

def load_data():
    """Load accounts data from Neon via HTTP API."""
    try:
        r = _neon_query("SELECT data FROM bot_accounts LIMIT 1")
        if r['rows']:
            data = r['rows'][0]['data']
            if isinstance(data, str):
                data = json.loads(data)
            logger.info("Loaded accounts data from Neon DB")
            return data
    except Exception as e:
        logger.error(f"Failed to load data from DB: {e}")
    return {'accounts': [], 'account_types': {}, 'prices': {}}

def save_data():
    """Save accounts data to Neon via HTTP API."""
    try:
        _neon_query("UPDATE bot_accounts SET data = $1",
                    [json.dumps(accounts_data, ensure_ascii=False)])
        logger.info("Saved accounts data to Neon DB")
    except Exception as e:
        logger.error(f"Failed to save data to DB: {e}")

def load_sessions():
    """Load user sessions from Neon via HTTP API."""
    global user_sessions
    try:
        r = _neon_query("SELECT data FROM bot_sessions LIMIT 1")
        if r['rows']:
            data = r['rows'][0]['data']
            if isinstance(data, str):
                data = json.loads(data)
            user_sessions = {int(k): v for k, v in data.items()}
            logger.info("Loaded sessions from Neon DB")
    except Exception as e:
        logger.error(f"Failed to load sessions from DB: {e}")

def save_sessions():
    """Save user sessions to Neon via HTTP API."""
    try:
        with _data_lock:
            payload = {str(k): v for k, v in user_sessions.items()}
        _neon_query("UPDATE bot_sessions SET data = $1",
                    [json.dumps(payload, ensure_ascii=False)])
    except Exception as e:
        logger.error(f"Failed to save sessions to DB: {e}")

def _run_background(name, func, *args, **kwargs):
    def runner():
        try:
            func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Background task {name} failed: {type(e).__name__}: {e}")
    background_pool.submit(runner)

def save_sessions_async():
    _run_background("save_sessions", save_sessions)

def save_pending_payment_async(user_id, chat_id, session):
    _run_background("save_pending_payment", save_pending_payment, user_id, chat_id, session)

def delete_pending_payment_async(user_id):
    _run_background("delete_pending_payment", delete_pending_payment, user_id)

def save_purchase_history_async(user_id, account_type, quantity, total_price, accounts=None):
    _run_background("save_purchase_history", save_purchase_history, user_id, account_type, quantity, total_price, accounts)

def _delete_message_now(chat_id, message_id):
    response = http.post(
        f"{API_URL}/deleteMessage",
        data={'chat_id': chat_id, 'message_id': message_id},
        timeout=4
    )
    if response.status_code >= 400:
        logger.warning(f"Delete message HTTP failed: status={response.status_code} body={response.text}")
        response.raise_for_status()
    result = response.json()
    if not result.get('ok'):
        logger.warning(f"Delete message API failed: {result}")
        return False
    logger.info(f"Deleted message {message_id} from chat {chat_id}")
    return True

def delete_message_async(chat_id, message_id):
    if not message_id:
        return
    _run_background("delete_message", _delete_message_now, chat_id, message_id)

def _record_scheduled_deletion(chat_id, message_id, delay_seconds):
    try:
        _neon_query("""
            INSERT INTO bot_scheduled_deletions (chat_id, message_id, delete_at)
            VALUES ($1, $2, NOW() + ($3 || ' seconds')::interval)
            ON CONFLICT (chat_id, message_id) DO UPDATE SET
                delete_at = EXCLUDED.delete_at
        """, [str(chat_id), str(message_id), str(delay_seconds)])
    except Exception as e:
        logger.error(f"Failed to record scheduled deletion: {e}")

def _clear_scheduled_deletion(chat_id, message_id):
    try:
        _neon_query(
            "DELETE FROM bot_scheduled_deletions WHERE chat_id = $1 AND message_id = $2",
            [str(chat_id), str(message_id)]
        )
    except Exception as e:
        logger.error(f"Failed to clear scheduled deletion: {e}")

def _run_scheduled_delete(chat_id, message_id, delay_seconds):
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    for attempt in range(2):
        try:
            if _delete_message_now(chat_id, message_id):
                _clear_scheduled_deletion(chat_id, message_id)
                return
        except Exception as e:
            logger.warning(f"Failed delayed message delete attempt {attempt + 1}: {e}")
        time.sleep(2)
    # Best-effort cleanup even if Telegram rejected (message may already be gone)
    _clear_scheduled_deletion(chat_id, message_id)

def delete_message_later(chat_id, message_id, delay_seconds=120):
    if not message_id:
        return
    _record_scheduled_deletion(chat_id, message_id, delay_seconds)
    _run_background("delete_message_later", _run_scheduled_delete, chat_id, message_id, delay_seconds)

def resume_scheduled_deletions():
    """On startup, re-arm any scheduled deletions saved in the DB so they survive cold restarts."""
    try:
        r = _neon_query(
            "SELECT chat_id, message_id, "
            "GREATEST(0, EXTRACT(EPOCH FROM (delete_at - NOW())))::int AS remaining "
            "FROM bot_scheduled_deletions"
        )
        rows = r.get('rows', []) or []
        for row in rows:
            try:
                chat_id = int(row['chat_id'])
                message_id = int(row['message_id'])
                remaining = int(row.get('remaining') or 0)
                _run_background(
                    "resume_scheduled_delete",
                    _run_scheduled_delete, chat_id, message_id, remaining
                )
            except Exception as e:
                logger.warning(f"Bad scheduled deletion row {row}: {e}")
        if rows:
            logger.info(f"Resumed {len(rows)} scheduled message deletion(s) from DB")
    except Exception as e:
        logger.error(f"Failed to resume scheduled deletions: {e}")

def save_pending_payment(user_id, chat_id, session):
    """Save a pending payment to Neon DB so it persists across sessions."""
    try:
        _neon_query("""
            INSERT INTO bot_pending_payments
                (user_id, chat_id, account_type, quantity, total_price, md5_hash, qr_message_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (user_id) DO UPDATE SET
                chat_id = EXCLUDED.chat_id,
                account_type = EXCLUDED.account_type,
                quantity = EXCLUDED.quantity,
                total_price = EXCLUDED.total_price,
                md5_hash = EXCLUDED.md5_hash,
                qr_message_id = EXCLUDED.qr_message_id,
                created_at = NOW()
        """, [
            str(user_id), str(chat_id),
            session.get('account_type'), str(session.get('quantity', 1)),
            str(session.get('total_price', 0)), session.get('md5_hash'),
            str(session.get('qr_message_id', 0))
        ])
        logger.info(f"Saved pending payment for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to save pending payment: {e}")

def get_pending_payment(user_id):
    """Get a pending payment from Neon DB."""
    try:
        r = _neon_query("SELECT * FROM bot_pending_payments WHERE user_id = $1", [str(user_id)])
        if r['rows']:
            row = r['rows'][0]
            return {
                'state': 'payment_pending',
                'account_type': row.get('account_type'),
                'quantity': int(row.get('quantity') or 1),
                'total_price': float(row.get('total_price') or 0),
                'md5_hash': row.get('md5_hash'),
                'qr_message_id': int(row.get('qr_message_id') or 0),
                'chat_id': int(row.get('chat_id') or 0)
            }
    except Exception as e:
        logger.error(f"Failed to get pending payment: {e}")
    return None

def delete_pending_payment(user_id):
    """Delete a pending payment from Neon DB."""
    try:
        _neon_query("DELETE FROM bot_pending_payments WHERE user_id = $1", [str(user_id)])
        logger.info(f"Deleted pending payment for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to delete pending payment: {e}")

def save_purchase_history(user_id, account_type, quantity, total_price, accounts=None):
    """Save a completed purchase to history and update email→buyer map."""
    try:
        accounts_list = accounts or []
        accounts_json = json.dumps(accounts_list, ensure_ascii=False)
        _neon_query(
            "INSERT INTO bot_purchase_history (user_id, account_type, quantity, total_price, accounts) VALUES ($1, $2, $3, $4, $5)",
            [str(user_id), account_type, str(quantity), str(total_price), accounts_json]
        )
        # Keep bot_email_buyer_map in sync so verification SMS always reaches buyer
        for acc in accounts_list:
            if isinstance(acc, dict) and acc.get('email'):
                try:
                    _neon_query("""
                        INSERT INTO bot_email_buyer_map (email, user_id, account_type)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (email) DO UPDATE
                            SET user_id      = EXCLUDED.user_id,
                                account_type = EXCLUDED.account_type,
                                purchased_at = NOW()
                    """, [str(acc['email']).strip().lower(), str(user_id), account_type])
                except Exception as map_err:
                    logger.error(f"Failed to update email_buyer_map for {acc['email']}: {map_err}")
    except Exception as e:
        logger.error(f"Failed to save purchase history: {e}")

def get_purchase_history(user_id, limit=10):
    """Get last N purchases for a user."""
    try:
        r = _neon_query(
            "SELECT account_type, quantity, total_price, accounts, purchased_at FROM bot_purchase_history WHERE user_id = $1 ORDER BY purchased_at DESC LIMIT $2",
            [str(user_id), str(limit)]
        )
        return r.get('rows', [])
    except Exception as e:
        logger.error(f"Failed to get purchase history: {e}")
    return []

def get_all_buyer_ids():
    """Get all distinct user IDs from purchase history."""
    try:
        r = _neon_query("SELECT DISTINCT user_id FROM bot_purchase_history")
        return [int(row['user_id']) for row in r.get('rows', [])]
    except Exception as e:
        logger.error(f"Failed to get buyer IDs: {e}")
    return []

def find_buyer_by_email(email):
    """Find the buyer of a given email — checks bot_email_buyer_map first, then purchase history."""
    email = (email or '').strip().lower()
    if not email:
        return None
    try:
        # 1. Fast lookup from dedicated map table (case-insensitive for safety)
        r = _neon_query(
            "SELECT user_id FROM bot_email_buyer_map WHERE LOWER(email) = $1",
            [email]
        )
        if r.get('rows'):
            uid = int(r['rows'][0]['user_id'])
            logger.info(f"Found buyer {uid} for {email} via email_buyer_map")
            return uid
    except Exception as e:
        logger.error(f"email_buyer_map lookup failed for {email}: {e}")

    try:
        # 2. Fallback: JSONB containment on purchase history
        r = _neon_query(
            "SELECT user_id FROM bot_purchase_history "
            "WHERE accounts @> $1::jsonb "
            "ORDER BY purchased_at DESC LIMIT 1",
            [json.dumps([{"email": email}])]
        )
        if r.get('rows'):
            uid = int(r['rows'][0]['user_id'])
            # Backfill the map so next lookup is instant
            try:
                _neon_query("""
                    INSERT INTO bot_email_buyer_map (email, user_id)
                    VALUES ($1, $2)
                    ON CONFLICT (email) DO UPDATE
                        SET user_id = EXCLUDED.user_id, purchased_at = NOW()
                """, [email, str(uid)])
            except Exception:
                pass
            logger.info(f"Found buyer {uid} for {email} via purchase_history JSONB (backfilled map)")
            return uid

        # 3. Last resort: ILIKE text search (handles old plain-string rows)
        r2 = _neon_query(
            "SELECT user_id, accounts FROM bot_purchase_history "
            "WHERE accounts::text ILIKE $1 ORDER BY purchased_at DESC",
            [f"%{email}%"]
        )
        for row in r2.get('rows', []):
            accounts = row.get('accounts') or []
            if isinstance(accounts, str):
                try:
                    accounts = json.loads(accounts)
                except Exception:
                    accounts = []
            for account in accounts:
                if str(account.get('email', '')).lower() == email.lower():
                    uid = int(row.get('user_id'))
                    try:
                        _neon_query("""
                            INSERT INTO bot_email_buyer_map (email, user_id)
                            VALUES ($1, $2)
                            ON CONFLICT (email) DO UPDATE
                                SET user_id = EXCLUDED.user_id, purchased_at = NOW()
                        """, [email, str(uid)])
                    except Exception:
                        pass
                    logger.info(f"Found buyer {uid} for {email} via purchase_history ILIKE (backfilled map)")
                    return uid
    except Exception as e:
        logger.error(f"Failed to find buyer by email {email}: {e}")
    return None

def find_all_buyers_by_email(email):
    """Return ALL distinct user_ids who ever bought the given email, ordered most-recent first."""
    email = (email or '').strip().lower()
    if not email:
        return []
    buyers = []
    seen = set()
    try:
        r = _neon_query(
            "SELECT user_id, MAX(purchased_at) AS last_at FROM bot_purchase_history "
            "WHERE accounts @> $1::jsonb "
            "GROUP BY user_id ORDER BY last_at DESC",
            [json.dumps([{"email": email}])]
        )
        for row in r.get('rows', []) or []:
            uid = int(row['user_id'])
            if uid not in seen:
                seen.add(uid)
                buyers.append(uid)
    except Exception as e:
        logger.error(f"JSONB buyer scan failed for {email}: {e}")

    try:
        r2 = _neon_query(
            "SELECT user_id, accounts, purchased_at FROM bot_purchase_history "
            "WHERE accounts::text ILIKE $1 ORDER BY purchased_at DESC",
            [f"%{email}%"]
        )
        for row in r2.get('rows', []) or []:
            accounts = row.get('accounts') or []
            if isinstance(accounts, str):
                try:
                    accounts = json.loads(accounts)
                except Exception:
                    accounts = []
            for account in accounts:
                if str(account.get('email', '')).strip().lower() == email:
                    uid = int(row['user_id'])
                    if uid not in seen:
                        seen.add(uid)
                        buyers.append(uid)
                    break
    except Exception as e:
        logger.error(f"ILIKE buyer scan failed for {email}: {e}")

    return buyers

_init_db()

# Restore admin-configurable settings from Neon so they survive Vercel cold restarts
_saved_payment_name = get_setting('PAYMENT_NAME')
if _saved_payment_name:
    PAYMENT_NAME = _saved_payment_name
    logger.info(f"Loaded PAYMENT_NAME from DB: {PAYMENT_NAME}")
_saved_maintenance = get_setting('MAINTENANCE_MODE')
if _saved_maintenance is not None:
    MAINTENANCE_MODE = (str(_saved_maintenance).lower() == 'true')
    logger.info(f"Loaded MAINTENANCE_MODE from DB: {MAINTENANCE_MODE}")
_saved_extra_admins = get_setting('EXTRA_ADMIN_IDS')
if _saved_extra_admins:
    try:
        EXTRA_ADMIN_IDS = set(int(x) for x in json.loads(_saved_extra_admins))
        logger.info(f"Loaded {len(EXTRA_ADMIN_IDS)} extra admin(s) from DB")
    except Exception as e:
        logger.error(f"Failed to parse EXTRA_ADMIN_IDS from DB: {e}")
_saved_bakong = get_setting('BAKONG_TOKEN')
if _saved_bakong:
    BAKONG_TOKEN = _saved_bakong
    try:
        khqr_client = KHQR(BAKONG_TOKEN)
    except Exception as e:
        logger.error(f"Failed to rebuild KHQR client from saved token: {e}")
    logger.info(f"Loaded BAKONG_TOKEN from DB: {BAKONG_TOKEN[:10]}...")

_saved_channel_id = get_setting('TELEGRAM_CHANNEL_ID')
if _saved_channel_id:
    CHANNEL_ID = _saved_channel_id.strip()
    logger.info(f"Loaded TELEGRAM_CHANNEL_ID from DB: {CHANNEL_ID}")

# User session storage for tracking conversation state
user_sessions = {}

# Process-local cache of user IDs we've already notified the admin about.
# Backed by the bot_known_users.admin_notified column so a Vercel cold restart
# never re-spams the admin with duplicate "new user" notifications.
_notified_users = set()
_notified_users_lock = threading.Lock()

def _is_admin_notified(uid):
    """Return True if the admin has already been notified about this user.
    Checks the in-memory cache first, then falls back to the DB so the answer
    survives cold restarts."""
    with _notified_users_lock:
        if uid in _notified_users:
            return True
    try:
        r = _neon_query(
            "SELECT admin_notified FROM bot_known_users WHERE user_id = $1",
            [str(uid)]
        )
        rows = r.get('rows', []) or []
        if rows and rows[0].get('admin_notified'):
            with _notified_users_lock:
                _notified_users.add(uid)
            return True
    except Exception as e:
        logger.error(f"Failed to check admin_notified for {uid}: {e}")
    return False

def fetch_user_info(user_id):
    """Fetch a user's profile from Telegram via getChat. Returns dict or None."""
    try:
        resp = http.get(
            f"{API_URL}/getChat",
            params={'chat_id': user_id},
            timeout=5
        )
        data = resp.json()
        if data.get('ok'):
            return data.get('result') or {}
    except Exception as e:
        logger.error(f"getChat failed for {user_id}: {e}")
    return None

def backfill_known_user_profiles():
    """For known users with missing name/username, fetch from Telegram and update DB."""
    try:
        r = _neon_query(
            "SELECT user_id FROM bot_known_users "
            "WHERE COALESCE(first_name, '') = '' "
            "AND COALESCE(last_name, '') = '' "
            "AND COALESCE(username, '') = ''"
        )
        rows = r.get('rows', [])
        for row in rows:
            uid = int(row['user_id'])
            info = fetch_user_info(uid)
            if not info:
                continue
            first = info.get('first_name') or ''
            last = info.get('last_name') or ''
            uname = info.get('username') or ''
            try:
                _neon_query(
                    "UPDATE bot_known_users SET first_name=$1, last_name=$2, username=$3 WHERE user_id=$4",
                    [first, last, uname, str(uid)]
                )
                logger.info(f"Backfilled profile for {uid}: {first} {last} @{uname}")
            except Exception as e:
                logger.error(f"Failed to update profile for {uid}: {e}")
    except Exception as e:
        logger.error(f"backfill_known_user_profiles error: {e}")


def notify_admin_new_user(user):
    """Send a 'new user' notification to the admin once per cold start per user."""
    try:
        uid = user.get('id')
        if not uid or uid == ADMIN_ID:
            return
        # Cross-restart de-dupe: skip if the DB already says we've notified.
        if _is_admin_notified(uid):
            return
        with _notified_users_lock:
            if uid in _notified_users:
                return
            _notified_users.add(uid)
        first = user.get('first_name', '') or ''
        last = user.get('last_name', '') or ''
        full_name = f"{first} {last}".strip() or 'N/A'
        username = user.get('username')
        username_str = f"@{username}" if username else '—'
        msg = (
            "🆕 អ្នកប្រើប្រាស់ថ្មី!\n\n"
            f"👤 ឈ្មោះ: {html.escape(full_name)}\n"
            f"🔖 Username: {html.escape(username_str)}\n"
            f"🪪 ID: <code>{uid}</code>"
        )
        def _send():
            try:
                http.post(
                    f"{API_URL}/sendMessage",
                    data={'chat_id': ADMIN_ID, 'text': msg, 'parse_mode': 'HTML'},
                    timeout=5
                )
            except Exception as e:
                logger.error(f"Failed to send new-user notification: {e}")
            try:
                _neon_query("""
                    INSERT INTO bot_known_users (user_id, first_name, last_name, username, first_seen, last_seen, admin_notified)
                    VALUES ($1, $2, $3, $4, NOW(), NOW(), TRUE)
                    ON CONFLICT (user_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        username = EXCLUDED.username,
                        last_seen = NOW(),
                        admin_notified = TRUE
                """, [str(uid), first, last, username or ''])
            except Exception as e:
                logger.error(f"Failed to record known user {uid}: {e}")
        _run_background("notify_admin_new_user", _send)
    except Exception as e:
        logger.error(f"notify_admin_new_user error: {e}")

# Account storage - loaded from file for persistence across restarts
accounts_data = load_data()

# Always load persisted sessions on startup
load_sessions()

# Tracks the current user message_id per worker so replies never cross between users
_reply_context = threading.local()
START_BANNER_FILE_ID = get_setting('START_BANNER_FILE_ID') or os.environ.get("START_BANNER_FILE_ID", "")
if START_BANNER_FILE_ID:
    logger.info(f"Loaded START_BANNER_FILE_ID from DB/env: {START_BANNER_FILE_ID[:20]}...")

def _set_reply_to_id(message_id):
    _reply_context.message_id = message_id

def _get_reply_to_id():
    return getattr(_reply_context, 'message_id', None)

def _type_callback_id(account_type):
    return hashlib.sha1(account_type.encode('utf-8')).hexdigest()[:12]

def _account_type_from_callback_id(callback_id):
    for account_type in accounts_data.get('account_types', {}):
        if _type_callback_id(account_type) == callback_id:
            return account_type
    return None

def _short_label(text, limit=36):
    clean = " ".join(str(text).split())
    return clean if len(clean) <= limit else clean[:limit - 1] + "…"

def send_message(chat_id, text, reply_to_message_id=None, parse_mode=None, reply_markup=None, message_effect_id=None):
    """Send a message to a specific chat."""
    url = f"{API_URL}/sendMessage"
    data = {
        'chat_id': chat_id,
        'text': text
    }
    
    effective_reply_to = _get_reply_to_id() if reply_to_message_id is None else reply_to_message_id
    if effective_reply_to:
        data['reply_to_message_id'] = effective_reply_to
        data['allow_sending_without_reply'] = True
    
    if parse_mode:
        data['parse_mode'] = parse_mode

    if reply_markup == "no_keyboard":
        pass
    else:
        if reply_markup is not None and reply_markup is not False:
            effective_markup = reply_markup
        else:
            effective_markup = ADMIN_REPLY_KEYBOARD if is_admin(chat_id) else {'remove_keyboard': True}
        data['reply_markup'] = json.dumps(effective_markup)

    if message_effect_id:
        data['message_effect_id'] = message_effect_id
    
    try:
        response = http.post(url, data=data, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        body = ''
        if hasattr(e, 'response') and e.response is not None:
            body = e.response.text
        logger.error(f"Failed to send message: {e} | body: {body}")
        return None

def send_sticker(chat_id, sticker_id, reply_markup=None):
    """Send a sticker to a specific chat."""
    url = f"{API_URL}/sendSticker"
    data = {
        'chat_id': chat_id,
        'sticker': sticker_id
    }
    if reply_markup is not None:
        data['reply_markup'] = json.dumps(reply_markup)
    try:
        response = http.post(url, data=data, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to send sticker: {e}")
        return None

def send_start_banner(chat_id, caption=None, parse_mode=None, message_effect_id=None, reply_markup=None):
    """Send the start banner stored in Neon (as a Telegram file_id).

    If no banner has been uploaded yet, fall back to a plain text message
    with the same caption — no local files are read.
    """
    global START_BANNER_FILE_ID

    if START_BANNER_FILE_ID:
        url = f"{API_URL}/sendPhoto"
        data = {'chat_id': chat_id, 'photo': START_BANNER_FILE_ID}
        if caption:
            data['caption'] = caption
        if parse_mode:
            data['parse_mode'] = parse_mode
        if message_effect_id:
            data['message_effect_id'] = message_effect_id
        if reply_markup:
            data['reply_markup'] = json.dumps(reply_markup)
        try:
            response = http.post(url, data=data, timeout=6)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Cached start banner failed, falling back to text: {e}")
            START_BANNER_FILE_ID = ""
            _run_background("clear_banner_file_id", set_setting, 'START_BANNER_FILE_ID', '')

    # No banner stored in Neon — send the caption as a plain message.
    return send_message(
        chat_id,
        caption or "",
        parse_mode=parse_mode,
        reply_to_message_id=False,
        reply_markup=reply_markup,
    )

def answer_callback(callback_query_id, text=None, show_alert=False):
    data = {'callback_query_id': callback_query_id}
    if text:
        data['text'] = text
    if show_alert:
        data['show_alert'] = True
    try:
        return http.post(f"{API_URL}/answerCallbackQuery", data=data, timeout=4)
    except requests.RequestException as e:
        logger.warning(f"Failed to answer callback quickly: {e}")
        return None

def _qr_caption(amount, seconds_left):
    """Build the QR caption with the live countdown remaining."""
    if seconds_left < 0:
        seconds_left = 0
    minutes = seconds_left // 60
    seconds = seconds_left % 60
    return (
        f"💵 <b>ចំនួនទឹកប្រាក់៖</b> ${amount}\n"
        f"⏳ <b>QR នឹងផុតកំណត់ក្នុង៖</b> {minutes:02d}:{seconds:02d}"
    )


def _start_qr_countdown(chat_id, user_id, msg_id, md5_hash, amount, started_at):
    """Background thread: refresh QR caption every 30s and expire after timeout."""
    def run():
        try:
            while True:
                elapsed = int(time.time() - started_at)
                remaining = PAYMENT_TIMEOUT_SECONDS - elapsed
                with _data_lock:
                    sess = user_sessions.get(user_id)
                    still_active = bool(
                        sess
                        and sess.get('md5_hash') == md5_hash
                        and sess.get('state') == 'payment_pending'
                    )
                if not still_active:
                    return
                if remaining <= 0:
                    delete_message_async(chat_id, msg_id)
                    with _data_lock:
                        if (user_id in user_sessions
                                and user_sessions[user_id].get('md5_hash') == md5_hash):
                            del user_sessions[user_id]
                    save_sessions_async()
                    delete_pending_payment_async(user_id)
                    send_message(
                        chat_id,
                        "⌛ <b>QR Code បានផុតកំណត់</b>\n\nសូមបង្កើតការទិញម្តងទៀត។",
                        parse_mode="HTML",
                        reply_to_message_id=False,
                    )
                    try:
                        show_account_selection(chat_id)
                    except Exception as e:
                        logger.warning(f"show_account_selection after expiry failed: {e}")
                    return
                edit_message_caption(
                    chat_id, msg_id,
                    _qr_caption(amount, remaining),
                    reply_markup=CHECK_PAYMENT_KEYBOARD,
                )
                # Sleep until the next whole-second boundary from started_at
                # so the countdown ticks exactly every 1 second regardless of
                # how long the API call above took.
                next_tick = started_at + (elapsed + 1)
                sleep_time = max(0, next_tick - time.time())
                time.sleep(sleep_time)
        except Exception as e:
            logger.error(f"QR countdown thread failed: {e}")
    threading.Thread(target=run, daemon=True).start()


def edit_message_caption(chat_id, message_id, caption, parse_mode='HTML', reply_markup=None):
    """Edit the caption of a previously sent photo."""
    url = f"{API_URL}/editMessageCaption"
    data = {'chat_id': chat_id, 'message_id': message_id, 'caption': caption}
    if parse_mode:
        data['parse_mode'] = parse_mode
    if reply_markup:
        data['reply_markup'] = json.dumps(reply_markup)
    try:
        r = http.post(url, data=data, timeout=10)
        return r.json()
    except requests.RequestException as e:
        logger.warning(f"editMessageCaption failed: {e}")
        return None

def send_photo_bytes(chat_id, photo_bytes, caption=None, parse_mode=None, reply_markup=None):
    """Send a photo from raw bytes to a specific chat (no filesystem needed)."""
    url = f"{API_URL}/sendPhoto"
    data = {'chat_id': chat_id}
    if caption:
        data['caption'] = caption
    if parse_mode:
        data['parse_mode'] = parse_mode
    if reply_markup:
        data['reply_markup'] = json.dumps(reply_markup)
    try:
        files = {'photo': ('qr.png', photo_bytes, 'image/png')}
        response = http.post(url, data=data, files=files, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to send photo bytes: {e}")
        return None

def send_photo_url(chat_id, photo_url, caption=None, parse_mode=None, reply_markup=None):
    """Send a photo from a URL to a specific chat."""
    url = f"{API_URL}/sendPhoto"
    data = {
        'chat_id': chat_id,
        'photo': photo_url
    }
    if caption:
        data['caption'] = caption
    if parse_mode:
        data['parse_mode'] = parse_mode
    if reply_markup:
        data['reply_markup'] = json.dumps(reply_markup)
    try:
        response = http.post(url, data=data, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to send photo URL: {e}")
        return None

def copy_message(to_chat_id, from_chat_id, message_id):
    """Copy a message from one chat to another without showing a forwarded header."""
    url = f"{API_URL}/copyMessage"
    data = {
        'chat_id': to_chat_id,
        'from_chat_id': from_chat_id,
        'message_id': message_id
    }
    try:
        response = http.post(url, data=data, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        body = ''
        if hasattr(e, 'response') and e.response is not None:
            body = e.response.text
        logger.error(f"Failed to copy channel message: {e} | body: {body}")
        return None

def _is_configured_channel(chat_id):
    return CHANNEL_ID and str(chat_id) == str(CHANNEL_ID)

def parse_egets_verification_message(text):
    email_match = re.search(r'[\w.+%-]+@[\w.-]+\.[A-Za-z]{2,}', text or '')
    code_match = re.search(r'(?<!\d)\d{4,8}(?!\d)', text or '')
    if not email_match or not code_match:
        return None, None
    return email_match.group(0).strip().lower(), code_match.group(0)

def format_egets_verification_message(email, code):
    return (
        "📩 <b>លេខកូដផ្ទៀងផ្ទាត់ E-GetS</b>\n\n"
        f"{html.escape(email)}\n\n"
        f"<code>{html.escape(code)}</code>"
    )

def handle_channel_post(channel_post):
    """Send posts from the configured channel to the admin private chat."""
    chat = channel_post.get('chat', {})
    chat_id = chat.get('id')
    message_id = channel_post.get('message_id')
    if not _is_configured_channel(chat_id) or not message_id:
        return

    text = channel_post.get('text') or channel_post.get('caption') or ''
    verification_email, verification_code = parse_egets_verification_message(text)
    if verification_email and verification_code:
        formatted_message = format_egets_verification_message(verification_email, verification_code)
        buyer_ids = find_all_buyers_by_email(verification_email)
        delivered_to = []
        for buyer_id in buyer_ids:
            buyer_sent = send_message(buyer_id, formatted_message, parse_mode="HTML", reply_to_message_id=False, reply_markup=False)
            if buyer_sent and buyer_sent.get('result'):
                buyer_message_id = buyer_sent['result'].get('message_id')
                delete_message_later(buyer_id, buyer_message_id, 60)
                delivered_to.append(buyer_id)
                logger.info(f"Sent verification code for {verification_email} to buyer {buyer_id}")
            else:
                logger.warning(f"Direct send to buyer {buyer_id} failed for {verification_email}")
        if not delivered_to:
            logger.warning(f"No buyer reachable for {verification_email}; sending to admin")
            sent = send_message(ADMIN_ID, formatted_message, parse_mode="HTML", reply_to_message_id=False, reply_markup=False)
            if sent and sent.get('result'):
                delete_message_later(ADMIN_ID, sent['result'].get('message_id'), 60)
        return

    copied = copy_message(ADMIN_ID, chat_id, message_id)
    if copied:
        logger.info(f"Copied channel post {message_id} from {chat_id} to admin {ADMIN_ID}")
        return

    if text:
        send_message(ADMIN_ID, text, reply_to_message_id=False, reply_markup=False)

def get_updates(offset=None):
    """Get updates from Telegram API. Raises HTTPError on 4xx/5xx so caller can handle 409."""
    url = f"{API_URL}/getUpdates"
    params = {
        'timeout': 30,
        'limit': 100,
        'allowed_updates': json.dumps(['message', 'callback_query', 'channel_post', 'edited_channel_post'])
    }
    if offset:
        params['offset'] = offset
    response = http.get(url, params=params, timeout=35)
    response.raise_for_status()
    return response.json()

ACCOUNT_BTN_PREFIX = "ទិញ "
ACCOUNT_BTN_SUFFIX = " - មានក្នុងស្តុក "

def show_account_selection(chat_id):
    """Send the account selection as inline buttons (same flow for buyers and admins)."""
    available = []
    for account_type, accounts in accounts_data['account_types'].items():
        count = len(accounts)
        if count > 0:
            price = accounts_data['prices'].get(account_type, 0)
            available.append((account_type, count, price))

    if not available:
        send_message(chat_id, "_សូមអភ័យទោស អស់ពីស្តុក 🪤_", parse_mode="Markdown", reply_to_message_id=False, reply_markup=_main_kb(chat_id))
        return

    inline_rows = []
    for account_type, count, price in available:
        label = f"{account_type} {price}$ - មានក្នុងស្តុក {count}"
        inline_rows.append([{
            'text': label,
            'callback_data': f"buy:{_type_callback_id(account_type)}"
        }])
    inline_keyboard = {'inline_keyboard': inline_rows}
    send_message(chat_id, "<b>សូមជ្រើសរើស Account ដើម្បីទិញ៖</b>",
                 reply_to_message_id=False, reply_markup=inline_keyboard, parse_mode="HTML")


MAIN_REPLY_KEYBOARD = {
    'keyboard': [
        [{'text': '💵 ទិញគូប៉ុង'}]
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

ADMIN_REPLY_KEYBOARD = {
    'keyboard': [
        [{'text': '⚙️កំណត់'}]
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

ADMIN_SETTINGS_BTN = '⚙️កំណត់'

def _main_kb(uid):
    """Return the appropriate main reply keyboard based on whether the user is an admin.

    Buyers (non-admins) get the keyboard removed instead of the persistent main keyboard.
    """
    return ADMIN_REPLY_KEYBOARD if is_admin(uid) else {'remove_keyboard': True}

# ── Admin settings reply-keyboard buttons ──
BTN_ADD_ACCOUNT     = '➕ បន្ថែម Account'
BTN_DELETE_TYPE     = '🗑 លុបប្រភេទ'
BTN_USERS           = '👥 អ្នកប្រើប្រាស់'
BTN_BUYERS          = '📋 របាយការណ៍ទិញ'
BTN_PAYMENT         = '💳 ឈ្មោះ Payment'
BTN_BAKONG          = '🔑 Bakong Token'
BTN_CHANNEL         = '📢 Channel ID'
BTN_ADMINS          = '👑 គ្រប់គ្រង Admin'
BTN_MAINTENANCE     = '🛠 Maintenance Mode'
BTN_BROADCAST       = '📢 ផ្សាយព័ត៌មាន'
BTN_BANNER          = '🖼 រូប Banner'
BTN_BACK_SETTINGS   = '↩️ ត្រឡប់ទៅកំណត់'

BTN_PAYMENT_EDIT    = '✏️ ប្តូរឈ្មោះ Payment'
BTN_BAKONG_EDIT     = '✏️ ប្តូរ Bakong Token'
BTN_CHANNEL_EDIT    = '✏️ ប្តូរ Channel ID'
BTN_CHANNEL_CLEAR   = '🗑 លុប Channel ID'
BTN_ADMIN_ADD       = '➕ បន្ថែម Admin'
BTN_ADMIN_REMOVE    = '➖ ដក Admin'
BTN_MAINT_ON        = '🔴 បិទ Bot'
BTN_MAINT_OFF       = '🟢 បើក Bot'
BTN_CANCEL_INPUT    = '🚫 បោះបង់'
BTN_DELETE_CONFIRM  = '✅ បញ្ជាក់លុប'
BTN_DELETE_CANCEL   = '🚫 បោះបង់ការលុប'
BTN_BROADCAST_CONFIRM = '✅ បញ្ជាក់ផ្សាយ'
BTN_BROADCAST_CANCEL  = '🚫 បោះបង់ការផ្សាយ'

BROADCAST_CONFIRM_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_BROADCAST_CONFIRM}],
        [{'text': BTN_BROADCAST_CANCEL}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

ADMIN_SETTINGS_REPLY_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_ADD_ACCOUNT}, {'text': BTN_DELETE_TYPE}],
        [{'text': BTN_USERS}, {'text': BTN_BUYERS}],
        [{'text': BTN_PAYMENT}, {'text': BTN_BAKONG}],
        [{'text': BTN_CHANNEL}, {'text': BTN_ADMINS}],
        [{'text': BTN_BROADCAST}, {'text': BTN_BANNER}],
        [{'text': BTN_MAINTENANCE}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

PAYMENT_SUBMENU_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_PAYMENT_EDIT}],
        [{'text': BTN_BACK_SETTINGS}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

BAKONG_SUBMENU_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_BAKONG_EDIT}],
        [{'text': BTN_BACK_SETTINGS}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

CHANNEL_SUBMENU_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_CHANNEL_EDIT}, {'text': BTN_CHANNEL_CLEAR}],
        [{'text': BTN_BACK_SETTINGS}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

ADMINS_SUBMENU_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_ADMIN_ADD}, {'text': BTN_ADMIN_REMOVE}],
        [{'text': BTN_BACK_SETTINGS}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

MAINTENANCE_SUBMENU_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_MAINT_ON}, {'text': BTN_MAINT_OFF}],
        [{'text': BTN_BACK_SETTINGS}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

CANCEL_INPUT_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_CANCEL_INPUT}],
    ],
    'resize_keyboard': True,
    'one_time_keyboard': False,
    'is_persistent': True
}

ADD_ACCOUNT_KEYBOARD = {
    'keyboard': [
        [{'text': BTN_BACK_SETTINGS}],
    ],
    'resize_keyboard': True,
    'is_persistent': True
}

# Set of submenu/leaf button labels admins can press; used to keep them out of the
# unrecognized-command fallback.
ADMIN_BUTTON_LABELS = {
    BTN_ADD_ACCOUNT, BTN_DELETE_TYPE, BTN_USERS, BTN_BUYERS,
    BTN_PAYMENT, BTN_BAKONG, BTN_CHANNEL, BTN_ADMINS, BTN_MAINTENANCE, BTN_BROADCAST,
    BTN_BANNER,
    BTN_BACK_SETTINGS,
    BTN_PAYMENT_EDIT, BTN_BAKONG_EDIT,
    BTN_CHANNEL_EDIT, BTN_CHANNEL_CLEAR,
    BTN_ADMIN_ADD, BTN_ADMIN_REMOVE,
    BTN_MAINT_ON, BTN_MAINT_OFF,
}

CONFIRM_REPLY_KEYBOARD = {
    'keyboard': [[{'text': '🚫 បោះបង់'}, {'text': '✅ យល់ព្រម'}]],
    'resize_keyboard': True,
    'one_time_keyboard': True
}


def send_admin_settings_menu(chat_id):
    """Open the admin settings reply keyboard."""
    send_message(
        chat_id,
        "<b>⚙️ ការកំណត់ Admin</b>\n\nសូមជ្រើសរើសប្រតិបត្តិការខាងក្រោម៖",
        parse_mode="HTML",
        reply_to_message_id=False,
        reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD
    )


def _prompt_admin_input(chat_id, user_id, key, prompt_text):
    """Put the admin into an input-waiting state and send a prompt message."""
    with _data_lock:
        user_sessions[user_id] = {'state': f'admin_input:{key}'}
    save_sessions_async()
    send_message(
        chat_id,
        prompt_text + "\n\n<i>ចុច 🚫 បោះបង់ ដើម្បីបោះបង់</i>",
        parse_mode="HTML",
        reply_to_message_id=False,
        reply_markup=CANCEL_INPUT_KEYBOARD
    )


def _show_users_list_inline(chat_id):
    """Export the known users list as a TXT file."""
    try:
        backfill_known_user_profiles()
    except Exception as e:
        logger.error(f"Inline backfill failed: {e}")
    try:
        r = _neon_query(
            "SELECT user_id, first_name, last_name, username, first_seen "
            "FROM bot_known_users ORDER BY first_seen DESC"
        )
        rows = r.get('rows', [])
    except Exception as e:
        logger.error(f"Failed to load known users: {e}")
        rows = []
    back_keyboard = {
        'keyboard': [[{'text': BTN_BACK_SETTINGS}]],
        'resize_keyboard': True,
        'is_persistent': True,
    }
    if not rows:
        send_message(chat_id, "📭 <b>មិនទាន់មានអ្នកប្រើប្រាស់ទេ។</b>",
                     parse_mode="HTML", reply_to_message_id=False,
                     reply_markup=back_keyboard)
        return
    total = len(rows)
    lines = [f"👥 អ្នកប្រើប្រាស់សរុប: {total}", ""]
    for i, row in enumerate(rows, 1):
        first = row.get('first_name') or ''
        last = row.get('last_name') or ''
        full_name = f"{first} {last}".strip() or 'N/A'
        uname = row.get('username') or ''
        uname_str = f"@{uname}" if uname else '—'
        uid = row.get('user_id')
        lines.append(f"{i}. {full_name}")
        lines.append(f"   🔖 {uname_str}")
        lines.append(f"   🪪 {uid}")
        lines.append("")
    txt = "\n".join(lines).encode('utf-8')
    import datetime as _dt
    filename = f"users_{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
    files = {'document': (filename, txt, 'text/plain; charset=utf-8')}
    data = {'chat_id': chat_id, 'caption': f"👥 បញ្ជីអ្នកប្រើប្រាស់ — {total} នាក់"}
    try:
        resp = http.post(f"{API_URL}/sendDocument", data=data, files=files, timeout=30)
        if resp.status_code >= 400 or not resp.json().get('ok'):
            logger.error(f"sendDocument users failed: {resp.text}")
            send_message(chat_id, "❌ បរាជ័យក្នុងការផ្ញើ​ឯកសារ", reply_to_message_id=False,
                         reply_markup=back_keyboard)
            return
    except Exception as e:
        logger.error(f"users export failed: {e}")
        send_message(chat_id, f"❌ Error: <code>{html.escape(str(e))}</code>",
                     parse_mode="HTML", reply_to_message_id=False,
                     reply_markup=back_keyboard)
        return
    send_message(chat_id, "↩️ ជ្រើសរើសខាងក្រោម៖", reply_to_message_id=False,
                 reply_markup=back_keyboard)


def _show_delete_type_menu_inline(chat_id, user_id=None):
    """Show a reply keyboard of account types to delete (only types with stock)."""
    types = [
        t for t in accounts_data.get('account_types', {}).keys()
        if len(accounts_data['account_types'].get(t, [])) > 0
    ]
    if not types:
        send_message(chat_id, "⚠️ <b>មិនមានប្រភេទ Account ណាមួយទេ!</b>",
                     parse_mode="HTML", reply_to_message_id=None)
        return
    rows = []
    labels_map = {}
    for t in types:
        count = len(accounts_data['account_types'].get(t, []))
        price = accounts_data.get('prices', {}).get(t, 0)
        label = f"{_short_label(t)} ({count} pcs · ${price})"
        rows.append([{'text': label}])
        labels_map[label] = t
    rows.append([{'text': BTN_BACK_SETTINGS}])
    reply_keyboard = {
        'keyboard': rows,
        'resize_keyboard': True,
        'is_persistent': True,
    }
    uid = user_id if user_id is not None else chat_id
    with _data_lock:
        user_sessions[uid] = {
            'state': 'delete_type_select',
            'labels': labels_map,
        }
    save_sessions_async()
    send_message(chat_id, "🗑 <b>ជ្រើសរើសប្រភេទ Account ដែលចង់លុប៖</b>",
                 parse_mode="HTML", reply_to_message_id=False, reply_markup=reply_keyboard)


def _export_buyers_report_inline(chat_id):
    """Export buyers TXT report (same logic as the /buyers command)."""
    try:
        r = _neon_query(
            "SELECT ph.user_id, ph.account_type, ph.quantity, ph.total_price, "
            "ph.accounts, ph.purchased_at, "
            "ku.first_name, ku.last_name, ku.username "
            "FROM bot_purchase_history ph "
            "LEFT JOIN bot_known_users ku ON ku.user_id = ph.user_id "
            "ORDER BY ph.user_id, ph.purchased_at DESC"
        )
        rows = r.get('rows', []) or []
        if not rows:
            send_message(chat_id, "មិនមានទិន្នន័យ​ទិញ​នៅឡើយ​ទេ។", reply_to_message_id=False)
            return
        grouped = {}
        for row in rows:
            uid = str(row.get('user_id'))
            grouped.setdefault(uid, {
                'first_name': row.get('first_name') or '',
                'last_name': row.get('last_name') or '',
                'username': row.get('username') or '',
                'purchases': []
            })
            accounts = row.get('accounts') or []
            if isinstance(accounts, str):
                try:
                    accounts = json.loads(accounts)
                except Exception:
                    accounts = []
            emails = [str(a.get('email', '')) for a in accounts if isinstance(a, dict) and a.get('email')]
            grouped[uid]['purchases'].append({
                'type': row.get('account_type') or '',
                'qty': row.get('quantity') or 0,
                'price': row.get('total_price') or 0,
                'when': str(row.get('purchased_at') or ''),
                'emails': emails
            })
        lines = []
        import datetime as _dt
        _now_str = _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        lines.append(f"Buyers Report — generated {_now_str}")
        lines.append(f"Total buyers: {len(grouped)}")
        lines.append("=" * 70)
        total_emails = 0
        for uid, info in grouped.items():
            full_name = (info['first_name'] + ' ' + info['last_name']).strip() or '(no name)'
            uname = f"@{info['username']}" if info['username'] else '—'
            lines.append("")
            lines.append(f"User ID : {uid}")
            lines.append(f"Name    : {full_name}")
            lines.append(f"Username: {uname}")
            lines.append(f"Purchases ({len(info['purchases'])}):")
            for p in info['purchases']:
                lines.append(f"  [{p['when']}] {p['type']} x{p['qty']} = ${p['price']}")
                for em in p['emails']:
                    lines.append(f"      • {em}")
                    total_emails += 1
            lines.append("-" * 70)
        lines.append("")
        lines.append(f"Total emails delivered: {total_emails}")
        txt = "\n".join(lines).encode('utf-8')
        filename = f"buyers_{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        files = {'document': (filename, txt, 'text/plain')}
        data = {'chat_id': chat_id, 'caption': f"📋 Buyers report — {len(grouped)} អ្នក​ទិញ, {total_emails} email"}
        resp = http.post(f"{API_URL}/sendDocument", data=data, files=files, timeout=30)
        if resp.status_code >= 400 or not resp.json().get('ok'):
            logger.error(f"sendDocument failed: {resp.text}")
            send_message(chat_id, "❌ បរាជ័យក្នុងការផ្ញើ​ឯកសារ", reply_to_message_id=False)
    except Exception as e:
        logger.error(f"buyers export failed: {e}")
        send_message(chat_id, f"❌ Error: <code>{html.escape(str(e))}</code>", parse_mode="HTML", reply_to_message_id=False)


def _show_admins_inline(chat_id):
    """Show current admins with the admins reply submenu."""
    extras = sorted(EXTRA_ADMIN_IDS)
    extras_str = "\n".join(f"• <code>{x}</code>" for x in extras) if extras else "(គ្មាន)"
    text_msg = (
        f"👑 <b>Admin បឋម៖</b> <code>{ADMIN_ID}</code>\n\n"
        f"➕ <b>Admin បន្ថែម៖</b>\n{extras_str}"
    )
    send_message(chat_id, text_msg, parse_mode="HTML", reply_to_message_id=False,
                 reply_markup=ADMINS_SUBMENU_KEYBOARD)


def _show_channel_inline(chat_id):
    """Show current channel id with the channel reply submenu."""
    current = CHANNEL_ID if CHANNEL_ID else "(មិនទាន់កំណត់)"
    text_msg = f"📢 <b>Channel ID បច្ចុប្បន្ន៖</b>\n<code>{html.escape(str(current))}</code>"
    send_message(chat_id, text_msg, parse_mode="HTML", reply_to_message_id=False,
                 reply_markup=CHANNEL_SUBMENU_KEYBOARD)


def _show_payment_inline(chat_id):
    """Show current payment name with the payment reply submenu."""
    text_msg = f"💳 <b>ឈ្មោះ Payment បច្ចុប្បន្ន៖</b>\n<code>{html.escape(PAYMENT_NAME or '(មិនទាន់កំណត់)')}</code>"
    send_message(chat_id, text_msg, parse_mode="HTML", reply_to_message_id=False,
                 reply_markup=PAYMENT_SUBMENU_KEYBOARD)


def _show_bakong_inline(chat_id):
    """Show the full bakong token with the bakong reply submenu."""
    full = BAKONG_TOKEN if BAKONG_TOKEN else "(មិនទាន់កំណត់)"
    text_msg = f"🔑 <b>Bakong Token បច្ចុប្បន្ន៖</b>\n<code>{html.escape(full)}</code>"
    send_message(chat_id, text_msg, parse_mode="HTML", reply_to_message_id=False,
                 reply_markup=BAKONG_SUBMENU_KEYBOARD)


def _show_maintenance_inline(chat_id):
    """Show bot on/off status with the maintenance reply submenu."""
    status = "🔴 បិទ" if MAINTENANCE_MODE else "🟢 បើក"
    text_msg = f"🛠 <b>ស្ថានភាព Bot បច្ចុប្បន្ន៖</b> {status}"
    send_message(chat_id, text_msg, parse_mode="HTML", reply_to_message_id=False,
                 reply_markup=MAINTENANCE_SUBMENU_KEYBOARD)


def _start_add_account_flow(chat_id, user_id, message_id):
    """Start the add-account session."""
    with _data_lock:
        user_sessions[user_id] = {'state': 'waiting_for_accounts'}
    save_sessions_async()
    send_message(
        chat_id,
        "*បញ្ចូល Account សម្រាប់លក់ (អ៊ីមែលម្តងមួយបន្ទាត់)៖*\n\n"
        "```\nl1jebywyzos2@10mail.info\nabc123@gmail.com\n```",
        reply_to_message_id=message_id, parse_mode="Markdown",
        reply_markup=ADD_ACCOUNT_KEYBOARD
    )


def _handle_admin_settings_input(chat_id, user_id, message_id, key, text):
    """Apply pending admin-settings input from the keyboard menu.

    Returns True if the input was consumed, False otherwise.
    """
    global PAYMENT_NAME, BAKONG_TOKEN, khqr_client, CHANNEL_ID, EXTRA_ADMIN_IDS

    raw = (text or '').strip()
    cancel_words = {'បោះបង់', '🚫 បោះបង់'}
    if raw in cancel_words:
        with _data_lock:
            if user_id in user_sessions:
                del user_sessions[user_id]
        save_sessions_async()
        send_message(chat_id, "🚫 បានបោះបង់ការកំណត់", reply_to_message_id=False, reply_markup=_main_kb(user_id))
        return True

    # ↩️ Back-to-settings button: cancel input and return to settings menu
    if raw == BTN_BACK_SETTINGS:
        with _data_lock:
            if user_id in user_sessions:
                del user_sessions[user_id]
        save_sessions_async()
        send_admin_settings_menu(chat_id)
        return True

    if key == 'payment':
        if not raw:
            send_message(chat_id, "សូមផ្ញើឈ្មោះ Payment ថ្មី (ឬចុច 🚫 បោះបង់)", reply_to_message_id=False)
            return True
        PAYMENT_NAME = raw
        set_setting('PAYMENT_NAME', PAYMENT_NAME)
        with _data_lock:
            if user_id in user_sessions:
                del user_sessions[user_id]
        save_sessions_async()
        send_message(chat_id, f"✅ បានប្តូរឈ្មោះ Payment ទៅជា <b>{html.escape(PAYMENT_NAME)}</b>",
                     parse_mode="HTML", reply_to_message_id=False, reply_markup=_main_kb(user_id))
        return True

    if key == 'bakong':
        if not raw:
            send_message(chat_id, "សូមផ្ញើ Bakong token ថ្មី (ឬចុច 🚫 បោះបង់)", reply_to_message_id=False)
            return True
        try:
            new_client = KHQR(raw)
        except Exception as e:
            send_message(chat_id, f"❌ Token មិនត្រឹមត្រូវ៖ <code>{html.escape(str(e))}</code>",
                         parse_mode="HTML", reply_to_message_id=False)
            return True
        BAKONG_TOKEN = raw
        khqr_client = new_client
        set_setting('BAKONG_TOKEN', raw)
        delete_message_async(chat_id, message_id)
        with _data_lock:
            if user_id in user_sessions:
                del user_sessions[user_id]
        save_sessions_async()
        send_message(
            chat_id,
            f"✅ បានប្តូរ Bakong token (Prefix៖ <code>{html.escape(raw[:10])}…</code>)",
            parse_mode="HTML", reply_to_message_id=False, reply_markup=_main_kb(user_id)
        )
        return True

    if key == 'channel':
        if not raw:
            send_message(chat_id, "សូមផ្ញើ Channel ID ថ្មី (ឧ. <code>-1001234567890</code>) ឬ <code>off</code> ដើម្បីបិទ",
                         parse_mode="HTML", reply_to_message_id=False)
            return True
        if raw.lower() in ('off', 'none', 'clear', 'delete', 'remove'):
            CHANNEL_ID = ""
            set_setting('TELEGRAM_CHANNEL_ID', '')
            with _data_lock:
                if user_id in user_sessions:
                    del user_sessions[user_id]
            save_sessions_async()
            send_message(chat_id, "✅ បានលុប Channel ID", reply_to_message_id=False, reply_markup=_main_kb(user_id))
            return True
        CHANNEL_ID = raw
        set_setting('TELEGRAM_CHANNEL_ID', raw)
        with _data_lock:
            if user_id in user_sessions:
                del user_sessions[user_id]
        save_sessions_async()
        send_message(
            chat_id,
            f"✅ បានកំណត់ Channel ID ទៅជា <code>{html.escape(raw)}</code>\n"
            f"សូមប្រាកដថា bot ជា admin/member ក្នុង channel នោះ។",
            parse_mode="HTML", reply_to_message_id=False, reply_markup=_main_kb(user_id)
        )
        return True

    if key in ('admin_add', 'admin_remove'):
        action = 'add' if key == 'admin_add' else 'remove'
        try:
            target_id = int(raw)
        except ValueError:
            send_message(chat_id, "❌ user_id ត្រូវតែជាលេខ (ឬចុច 🚫 បោះបង់)", reply_to_message_id=False)
            return True
        if target_id == ADMIN_ID:
            send_message(chat_id, "ℹ️ Admin បឋមមិនអាចលុប/បន្ថែមបានទេ។", reply_to_message_id=False, reply_markup=_main_kb(user_id))
            with _data_lock:
                if user_id in user_sessions:
                    del user_sessions[user_id]
            save_sessions_async()
            return True
        if action == 'add':
            EXTRA_ADMIN_IDS.add(target_id)
            msg = f"✅ បានបន្ថែម <code>{target_id}</code> ជា admin"
        else:
            EXTRA_ADMIN_IDS.discard(target_id)
            msg = f"✅ បានដក <code>{target_id}</code> ចេញពី admin"
        set_setting('EXTRA_ADMIN_IDS', json.dumps(sorted(EXTRA_ADMIN_IDS)))
        with _data_lock:
            if user_id in user_sessions:
                del user_sessions[user_id]
        save_sessions_async()
        send_message(chat_id, msg, parse_mode="HTML", reply_to_message_id=False, reply_markup=_main_kb(user_id))
        return True

    if key == 'banner':
        # Photo uploads are handled in handle_message before reaching here.
        # Any text input means the admin sent something other than a photo.
        send_message(chat_id,
            "📷 សូម​ផ្ញើ​ជា​<b>រូបភាព</b> (Photo) ប៉ុណ្ណោះ ឬចុច 🚫 បោះបង់",
            parse_mode="HTML", reply_to_message_id=False)
        return True

    if key == 'broadcast':
        if not message_id:
            send_message(chat_id, "សូមផ្ញើ​សារ​ដែល​ចង់​ផ្សាយ (ឬចុច 🚫 បោះបង់)",
                         reply_to_message_id=False)
            return True
        # Plain-text messages are copied (no "Forwarded from" tag);
        # media messages (photos, videos, files, etc.) are forwarded so the
        # admin attribution is preserved.
        is_text_only = bool(raw)
        with _data_lock:
            user_sessions[user_id] = {
                'state': 'broadcast_confirm',
                'broadcast_message_id': message_id,
                'broadcast_chat_id': chat_id,
                'broadcast_use_copy': is_text_only,
            }
        save_sessions_async()
        send_message(
            chat_id,
            "❓ <b>តើ​អ្នក​ប្រាកដ​ជា​ចង់​ផ្សាយ​សារ​ខាង​លើ​នេះ​ទៅ​អ្នក​ប្រើ​ប្រាស់​ទាំង​អស់​មែន​ទេ?</b>\n\n"
            "ចុច <b>✅ បញ្ជាក់ផ្សាយ</b> ដើម្បី​ផ្សាយ ឬ <b>🚫 បោះបង់ការផ្សាយ</b> ដើម្បី​បោះបង់។",
            parse_mode="HTML",
            reply_to_message_id=False,
            reply_markup=BROADCAST_CONFIRM_KEYBOARD
        )
        return True

    return False


def _run_broadcast(admin_chat_id, source_message_id, use_copy=False):
    """Send the admin's original message to every known user, preserving its
    original formatting (entities, photos, captions, etc.).

    When use_copy=True the bot uses copyMessage so recipients see a clean message
    with no "Forwarded from" attribution (used for plain text broadcasts).
    Otherwise it uses forwardMessage so the admin attribution is preserved
    (used for media broadcasts). Runs in background."""
    try:
        try:
            r = _neon_query("SELECT user_id FROM bot_known_users")
            rows = r.get('rows', []) or []
        except Exception as e:
            logger.error(f"Broadcast: failed to load users: {e}")
            send_message(admin_chat_id, f"❌ មិន​អាច​ផ្ទុក​បញ្ជី​អ្នក​ប្រើ​ប្រាស់​បាន: <code>{html.escape(str(e))}</code>",
                         parse_mode="HTML", reply_to_message_id=False,
                         reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
            return
        total = len(rows)
        sent = 0
        failed = 0
        blocked = 0
        for row in rows:
            uid = row.get('user_id')
            if not uid:
                continue
            try:
                api_method = 'copyMessage' if use_copy else 'forwardMessage'
                resp = http.post(
                    f"{API_URL}/{api_method}",
                    data={
                        'chat_id': uid,
                        'from_chat_id': admin_chat_id,
                        'message_id': source_message_id,
                        'protect_content': 'false',
                    },
                    timeout=15
                )
                if resp.status_code == 200 and resp.json().get('ok'):
                    sent += 1
                else:
                    body = resp.json() if resp.headers.get('content-type', '').startswith('application/json') else {}
                    desc = (body or {}).get('description', '')
                    if 'blocked' in desc.lower() or 'deactivated' in desc.lower() or 'chat not found' in desc.lower():
                        blocked += 1
                    else:
                        failed += 1
                        logger.warning(f"Broadcast to {uid} failed: {resp.status_code} {desc}")
            except Exception as e:
                failed += 1
                logger.warning(f"Broadcast to {uid} error: {e}")
            # Telegram limit ~30 msg/sec; sleep to stay safely under
            time.sleep(0.05)
        summary = (
            "📢 <b>ផ្សាយ​សារ​បាន​ចប់</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👥 សរុប:         {total}\n"
            f"✅ ផ្ញើ​ជោគជ័យ:   {sent}\n"
            f"⛔ បាន​ប្លុក/លុប:  {blocked}\n"
            f"❌ បរាជ័យ:        {failed}"
        )
        send_message(admin_chat_id, summary, parse_mode="HTML",
                     reply_to_message_id=False,
                     reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
    except Exception as e:
        logger.error(f"Broadcast crashed: {e}")
        try:
            send_message(admin_chat_id, f"❌ Broadcast error: <code>{html.escape(str(e))}</code>",
                         parse_mode="HTML", reply_to_message_id=False,
                         reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
        except Exception:
            pass


def _send_order_summary(chat_id, user_id, session):
    """Send order summary with inline confirm/cancel (same for buyers and admins).

    Stores summary_message_id in session.
    """
    quantity = session['quantity']
    total_price = session['total_price']
    summary = (
        f"<b>សូមបញ្ជាក់ការបញ្ជាទិញ</b>\n\n"
        f"<blockquote>🔹 ចំនួន: {quantity}\n\n"
        f"🔹 ប្រភេទ: {session['account_type']}\n\n"
        f"🔹 តម្លៃ: {total_price}$</blockquote>"
    )
    markup = {
        'inline_keyboard': [[
            {'text': '🚫 បោះបង់', 'callback_data': 'cancel_buy'},
            {'text': '✅ យល់ព្រម', 'callback_data': 'confirm_buy'},
        ]]
    }
    resp = send_message(chat_id, summary, reply_to_message_id=False, parse_mode="HTML", reply_markup=markup)
    if resp and resp.get('result'):
        with _data_lock:
            session['summary_message_id'] = resp['result']['message_id']


def _purchase_notification_targets():
    targets = [ADMIN_ID]
    if CHANNEL_ID and str(CHANNEL_ID) != str(ADMIN_ID):
        targets.append(CHANNEL_ID)
    return targets


def send_purchase_notification(message):
    for target in _purchase_notification_targets():
        rm = ADMIN_REPLY_KEYBOARD if str(target) == str(ADMIN_ID) else "no_keyboard"
        send_message(target, message, parse_mode="HTML", reply_to_message_id=False, reply_markup=rm)


def handle_callback_query(update):
    """Handle callback query (inline button clicks)."""
    _set_reply_to_id(None)
    try:
        callback_query = update.get('callback_query')
        if not callback_query:
            return
        
        chat_id = callback_query['message']['chat']['id']
        callback_data = callback_query.get('data')
        user = callback_query.get('from', {})
        user_id = user.get('id')
        
        logger.info(f"Received callback from user {user.get('first_name', 'Unknown')} (ID: {user_id}): {callback_data}")

        notify_admin_new_user(user)
        
        # Handle buy button clicks with reply quote functionality
        if callback_data.startswith('buy:') or callback_data.startswith('buy_'):
            if callback_data.startswith('buy:'):
                account_type = _account_type_from_callback_id(callback_data[4:])
            else:
                account_type = callback_data.replace('buy_', '')
            if not account_type:
                answer_callback(callback_query['id'], 'ប្រភេទនេះមិនមានទៀតហើយ។ សូមចាប់ផ្តើមម្តងទៀត។', True)
                return
            answer_callback(callback_query['id'])
            
            # Check if account type exists and has stock
            if account_type in accounts_data['account_types']:
                with _data_lock:
                    accounts = accounts_data['account_types'][account_type]
                    count = len(accounts)
                    price = accounts_data['prices'].get(account_type, 0)
                
                if count > 0:
                    # Always allow user to select account type (reset any existing session)
                    with _data_lock:
                        user_sessions[user_id] = {
                            'state': 'waiting_for_quantity',
                            'account_type': account_type,
                            'price': price,
                            'available_count': count
                        }
                    save_sessions_async()

                    # Create regular message without reply quote
                    reply_message = "*សូមជ្រើសរើសចំនួនដែលចង់ទិញ៖*"

                    # Build inline keyboard with all available quantities (rows of 5)
                    qty_inline = [
                        {'text': str(n), 'callback_data': f'qty:{n}'}
                        for n in range(1, count + 1)
                    ]
                    qty_inline_rows = [qty_inline[i:i+5] for i in range(0, len(qty_inline), 5)]
                    qty_inline_rows.append([{'text': '🚫 បោះបង់', 'callback_data': 'cancel_buy'}])
                    qty_keyboard = {'inline_keyboard': qty_inline_rows}

                    send_message(chat_id, reply_message, reply_to_message_id=False, parse_mode="Markdown", reply_markup=qty_keyboard)

                    # Delete the original message with inline buttons
                    delete_message_async(chat_id, callback_query['message']['message_id'])

                    logger.info(f"User {user_id} selected account type {account_type}, waiting for quantity input")
                else:
                    send_message(chat_id, f"សុំទោស! Account {account_type} អស់ស្តុកហើយ។")
        
        # Handle out-of-stock button clicks
        elif callback_data.startswith('out_of_stock:') or callback_data.startswith('out_of_stock_'):
            answer_callback(callback_query['id'])
            if callback_data.startswith('out_of_stock:'):
                account_type = _account_type_from_callback_id(callback_data[13:]) or "នេះ"
            else:
                account_type = callback_data.replace('out_of_stock_', '')
            send_message(chat_id, f"សូមអភ័យទោស Account {account_type} អស់ពីស្តុក 🪤")

        # Handle confirm buy — generate QR and proceed to payment
        elif callback_data == 'confirm_buy':
            session = user_sessions.get(user_id)
            if not session or session.get('state') != 'waiting_for_confirmation':
                answer_callback(callback_query['id'], 'មិនមានការទិញដែលកំពុងរង់ចាំ។', True)
                return
            answer_callback(callback_query['id'], 'កំពុងបង្កើត QR...')
            with _data_lock:
                session['state'] = 'payment_pending'
            # Delete the summary message
            summary_message_id = callback_query['message']['message_id']
            delete_message_async(chat_id, summary_message_id)
            try:
                img_bytes, md5_or_err, qr_string = generate_payment_qr(session['total_price'])
                if not img_bytes:
                    err_detail = md5_or_err or "មិនដឹងមូលហេតុ"
                    logger.error(f"QR generation returned None: {err_detail}")
                    # Notify admin with the actual error
                    if str(user_id) == str(ADMIN_ID):
                        send_message(chat_id,
                            f"❌ *QR បរាជ័យ (Admin Debug):*\n`{err_detail}`",
                            parse_mode="Markdown")
                    else:
                        send_message(chat_id,
                            "❌ *មានបញ្ហាក្នុងការបង្កើត QR Code*\n\nសូមព្យាយាមម្តងទៀត។",
                            parse_mode="Markdown")
                        send_message(ADMIN_ID,
                            f"⚠️ *QR Error (user {user_id}):*\n`{err_detail}`",
                            parse_mode="Markdown")
                    with _data_lock:
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    save_sessions_async()
                    return
                md5_hash = md5_or_err
                session['md5_hash'] = md5_hash
                started_at = time.time()
                session['qr_sent_at'] = started_at
                amount = session['total_price']
                photo_resp = send_photo_bytes(
                    chat_id, img_bytes,
                    caption=_qr_caption(amount, PAYMENT_TIMEOUT_SECONDS),
                    parse_mode='HTML',
                    reply_markup=CHECK_PAYMENT_KEYBOARD,
                )
                if photo_resp and photo_resp.get('result'):
                    msg_id = photo_resp['result']['message_id']
                    session['photo_message_id'] = msg_id
                    session['qr_message_id'] = msg_id
                    _start_qr_countdown(chat_id, user_id, msg_id, md5_hash, amount, started_at)
                save_sessions_async()
                save_pending_payment_async(user_id, chat_id, session)
                logger.info(f"Generated QR for user {user_id}: Amount ${session['total_price']}, MD5: {md5_hash}")
            except Exception as e:
                logger.error(f"Error generating KHQR: {type(e).__name__}: {e}")
                send_message(chat_id, "❌ *មានបញ្ហាក្នុងការបង្កើត QR Code*\n\nសូមព្យាយាមម្តងទៀត។", parse_mode="Markdown")
                with _data_lock:
                    if user_id in user_sessions:
                        del user_sessions[user_id]
                save_sessions_async()
            return

        # Admin: delete type — step 1: show confirmation
        elif callback_data.startswith('dts:') and is_admin(user_id):
            type_name = _account_type_from_callback_id(callback_data[4:]) or callback_data[4:]
            if type_name not in accounts_data.get('account_types', {}):
                answer_callback(callback_query['id'], 'ប្រភេទនេះមិនមានទៀតហើយ!', True)
                return
            answer_callback(callback_query['id'])
            count = len(accounts_data['account_types'].get(type_name, []))
            price = accounts_data.get('prices', {}).get(type_name, 0)
            confirm_cb = f"dtc:{_type_callback_id(type_name)}"
            keyboard = {'inline_keyboard': [[
                {'text': '✅ បញ្ជាក់លុប', 'callback_data': confirm_cb},
                {'text': '🚫 បោះបង់', 'callback_data': 'dtcancel'}
            ]]}
            send_message(chat_id,
                f"⚠️ <b>តើអ្នកពិតជាចង់លុបប្រភេទ Account នេះមែនទេ?</b>\n\n"
                f"<blockquote>🔹 ប្រភេទ: {type_name}\n🔹 ចំនួន Account: {count}\n🔹 តម្លៃ: ${price}</blockquote>\n\n"
                f"Account ទាំងអស់ក្នុងប្រភេទនេះនឹងត្រូវបានលុបចោលជាអចិន្ត្រៃយ៍!",
                parse_mode="HTML", reply_to_message_id=None, reply_markup=keyboard)
            return

        # Admin: delete type — step 2: confirmed, perform deletion
        elif callback_data.startswith('dtc:') and is_admin(user_id):
            type_name = _account_type_from_callback_id(callback_data[4:]) or callback_data[4:]
            if type_name not in accounts_data.get('account_types', {}):
                answer_callback(callback_query['id'], 'ប្រភេទនេះមិនមានទៀតហើយ!', True)
                return
            answer_callback(callback_query['id'])
            count = len(accounts_data['account_types'].pop(type_name, []))
            accounts_data.get('prices', {}).pop(type_name, None)
            accounts_data['accounts'] = [
                a for a in accounts_data.get('accounts', [])
                if a.get('type') != type_name
            ]
            save_data()
            delete_message_async(chat_id, callback_query['message']['message_id'])
            send_message(chat_id,
                f"✅ <b>បានលុបប្រភេទ Account <code>{type_name}</code> ចំនួន {count} records ដោយជោគជ័យ!</b>",
                parse_mode="HTML", reply_to_message_id=None)
            logger.info(f"Admin {user_id} deleted account type '{type_name}' ({count} records)")
            return

        # Admin: delete type — cancelled
        elif callback_data == 'dtcancel' and is_admin(user_id):
            answer_callback(callback_query['id'])
            delete_message_async(chat_id, callback_query['message']['message_id'])
            send_message(chat_id, "🚫 <b>បានបោះបង់ការលុបប្រភេទ Account</b>",
                         parse_mode="HTML", reply_to_message_id=None)
            return

        # Admin: settings menu actions (⚙️កំណត់ keyboard)
        elif callback_data.startswith('adm:') and is_admin(user_id):
            global PAYMENT_NAME, BAKONG_TOKEN, khqr_client, CHANNEL_ID, EXTRA_ADMIN_IDS, MAINTENANCE_MODE
            action = callback_data[4:]
            answer_callback(callback_query['id'])
            menu_msg_id = callback_query['message']['message_id']

            if action == 'close':
                delete_message_async(chat_id, menu_msg_id)
                return

            if action == 'back':
                delete_message_async(chat_id, menu_msg_id)
                send_admin_settings_menu(chat_id)
                return

            if action == 'add_account':
                delete_message_async(chat_id, menu_msg_id)
                _start_add_account_flow(chat_id, user_id, None)
                return

            if action == 'delete_type':
                delete_message_async(chat_id, menu_msg_id)
                _show_delete_type_menu_inline(chat_id, user_id)
                return

            if action == 'users':
                delete_message_async(chat_id, menu_msg_id)
                _show_users_list_inline(chat_id)
                return

            if action == 'buyers':
                delete_message_async(chat_id, menu_msg_id)
                _export_buyers_report_inline(chat_id)
                return

            if action == 'payment':
                delete_message_async(chat_id, menu_msg_id)
                _show_payment_inline(chat_id)
                return

            if action == 'payment_set':
                delete_message_async(chat_id, menu_msg_id)
                _prompt_admin_input(chat_id, user_id, 'payment',
                                    f"💳 ឈ្មោះ Payment បច្ចុប្បន្ន៖ <b>{html.escape(PAYMENT_NAME or '(មិនទាន់កំណត់)')}</b>\n\nសូមផ្ញើឈ្មោះ Payment ថ្មី៖")
                return

            if action == 'bakong':
                delete_message_async(chat_id, menu_msg_id)
                _show_bakong_inline(chat_id)
                return

            if action == 'bakong_set':
                delete_message_async(chat_id, menu_msg_id)
                _prompt_admin_input(chat_id, user_id, 'bakong',
                                    "🔑 សូមផ្ញើ Bakong Token ថ្មី៖\n<i>(សារនឹងត្រូវលុបដោយស្វ័យប្រវត្តិ)</i>")
                return

            if action == 'channel':
                delete_message_async(chat_id, menu_msg_id)
                _show_channel_inline(chat_id)
                return

            if action == 'channel_set':
                delete_message_async(chat_id, menu_msg_id)
                _prompt_admin_input(chat_id, user_id, 'channel',
                                    "📢 សូមផ្ញើ Channel ID ថ្មី (ឧ. <code>-1001234567890</code>)\nឬ <code>off</code> ដើម្បីលុប")
                return

            if action == 'channel_clear':
                CHANNEL_ID = ""
                set_setting('TELEGRAM_CHANNEL_ID', '')
                delete_message_async(chat_id, menu_msg_id)
                send_message(chat_id, "✅ បានលុប Channel ID", reply_to_message_id=False, reply_markup=_main_kb(user_id))
                return

            if action == 'admins':
                delete_message_async(chat_id, menu_msg_id)
                _show_admins_inline(chat_id)
                return

            if action == 'admin_add':
                delete_message_async(chat_id, menu_msg_id)
                _prompt_admin_input(chat_id, user_id, 'admin_add',
                                    "👑 សូមផ្ញើ user_id របស់អ្នកដែលចង់បន្ថែមជា admin (ជាលេខ)៖")
                return

            if action == 'admin_remove':
                delete_message_async(chat_id, menu_msg_id)
                _prompt_admin_input(chat_id, user_id, 'admin_remove',
                                    "👑 សូមផ្ញើ user_id របស់ admin ដែលចង់ដក (ជាលេខ)៖")
                return

            if action == 'maintenance':
                delete_message_async(chat_id, menu_msg_id)
                _show_maintenance_inline(chat_id)
                return

            if action == 'maint_on':
                MAINTENANCE_MODE = True
                set_setting('MAINTENANCE_MODE', 'true')
                delete_message_async(chat_id, menu_msg_id)
                send_message(chat_id, "✅ <b>Maintenance mode ON</b>", parse_mode="HTML",
                             reply_to_message_id=False, reply_markup=_main_kb(user_id))
                return

            if action == 'maint_off':
                MAINTENANCE_MODE = False
                set_setting('MAINTENANCE_MODE', 'false')
                delete_message_async(chat_id, menu_msg_id)
                send_message(chat_id, "✅ <b>Maintenance mode OFF</b> — Bot ដំណើរការធម្មតាហើយ",
                             parse_mode="HTML", reply_to_message_id=False, reply_markup=_main_kb(user_id))
                return

            return

        # Handle cancel buy — cancel from summary screen (before QR)
        elif callback_data == 'cancel_buy':
            answer_callback(callback_query['id'])
            with _data_lock:
                if user_id in user_sessions:
                    del user_sessions[user_id]
            save_sessions_async()
            summary_message_id = callback_query['message']['message_id']
            delete_message_async(chat_id, summary_message_id)
            show_account_selection(chat_id)
            return

        # Handle quantity number button press
        elif callback_data.startswith('qty:'):
            session = user_sessions.get(user_id)
            # Allow picking a quantity both during initial selection and while
            # the order summary is open (so the user can change their mind).
            if not session or session.get('state') not in ('waiting_for_quantity', 'waiting_for_confirmation'):
                answer_callback(callback_query['id'])
                return
            try:
                quantity = int(callback_data.split(':', 1)[1])
            except (ValueError, IndexError):
                answer_callback(callback_query['id'], 'ចំនួនមិនត្រឹមត្រូវ។', True)
                return

            if quantity > session['available_count']:
                answer_callback(callback_query['id'], f"សុំទោស! មានត្រឹមតែ {session['available_count']} នៅក្នុងស្តុក", True)
                return

            # If the user is already on the summary screen and re-picks the same
            # quantity, just acknowledge — nothing to update.
            if session.get('state') == 'waiting_for_confirmation' and session.get('quantity') == quantity:
                answer_callback(callback_query['id'])
                return

            total_price = quantity * session['price']
            previous_summary_id = session.get('summary_message_id')
            with _data_lock:
                session['quantity'] = quantity
                session['total_price'] = total_price
                session['state'] = 'waiting_for_confirmation'
                # Clear stale id so the new summary id can be saved cleanly.
                session.pop('summary_message_id', None)

            # Answer immediately before any I/O so the button feels instant
            answer_callback(callback_query['id'])
            save_sessions_async()

            # Remove the previous order summary (if any) so only the latest is shown.
            if previous_summary_id:
                delete_message_async(chat_id, previous_summary_id)

            _send_order_summary(chat_id, user_id, session)
            return

        # Handle check payment button
        elif callback_data == 'check_payment':
            session = user_sessions.get(user_id)
            if not session or session.get('state') != 'payment_pending':
                session = get_pending_payment(user_id)
            if not session:
                answer_callback(callback_query['id'], 'មិនមានការទិញដែលកំពុងរង់ចាំ។', True)
                return

            md5 = session.get('md5_hash')
            if not md5:
                answer_callback(callback_query['id'], 'មានបញ្ហាក្នុងការស្វែងរក QR។ សូមចាប់ផ្តើមម្តងទៀត។', True)
                return

            is_paid, payment_data = check_payment_status(md5)
            if is_paid:
                answer_callback(callback_query['id'], '✅ បានទទួលការបង់ប្រាក់!')
                user_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                deliver_accounts(chat_id, user_id, session, payment_data=payment_data, user_name=user_name)
                delete_pending_payment_async(user_id)
                save_sessions_async()
            else:
                answer_callback(
                    callback_query['id'],
                    "⏳ មិនទាន់បានទទួលការបង់ប្រាក់។\nសូមបង់ប្រាក់ហើយចុចពិនិត្យម្ដងទៀត។",
                    True,
                )
            return

        # Handle cancel purchase
        elif callback_data == 'cancel_purchase':
            answer_callback(callback_query['id'])
            session = user_sessions.get(user_id)
            photo_message_id = session.get('photo_message_id') if session else None
            if photo_message_id:
                delete_message_async(chat_id, photo_message_id)
            qr_message_id = session.get('qr_message_id') if session else None
            if qr_message_id:
                delete_message_async(chat_id, qr_message_id)
            dot_msg_id = session.get('dot_message_id') if session else None
            if dot_msg_id:
                delete_message_async(chat_id, dot_msg_id)
            with _data_lock:
                if user_id in user_sessions:
                    del user_sessions[user_id]
            save_sessions_async()
            delete_pending_payment_async(user_id)
            show_account_selection(chat_id)

    except Exception as e:
        logger.error(f"Error handling callback query: {e}")

def handle_message(update):
    """Handle incoming message."""
    global MAINTENANCE_MODE, PAYMENT_NAME, CHANNEL_ID, START_BANNER_FILE_ID
    try:
        # Handle callback queries first
        if 'callback_query' in update:
            handle_callback_query(update)
            return

        if 'channel_post' in update:
            handle_channel_post(update['channel_post'])
            return

        if 'edited_channel_post' in update:
            handle_channel_post(update['edited_channel_post'])
            return
            
        message = update.get('message')
        if not message:
            return
        
        chat_id = message['chat']['id']
        message_id = message.get('message_id')
        text = message.get('text', '')
        user = message.get('from', {})
        user_id = user.get('id')
        
        # Set reply-quote context for all send_message calls in this handler
        _set_reply_to_id(message_id)

        logger.info(f"Received message from user {user.get('first_name', 'Unknown')} (ID: {user_id}): {text}")

        notify_admin_new_user(user)
        
        # Function to show account selection interface
        def show_account_selection_local():
            show_account_selection(chat_id)

        if MAINTENANCE_MODE and not is_admin(user_id):
            send_message(chat_id, "🔧 <b>Bot កំពុង Update សូមរង់ចាំមួយភ្លែត...</b>", parse_mode="HTML", reply_to_message_id=False)
            return

        if text.strip() == '/start':
            logger.info(f"User {user_id} triggered account selection interface")
            with _data_lock:
                had_session = user_id in user_sessions
                if had_session:
                    del user_sessions[user_id]
            if had_session:
                save_sessions_async()
            show_account_selection_local()
            return

        # Admin: open settings menu via the ⚙️កំណត់ keyboard button
        if text.strip() == ADMIN_SETTINGS_BTN and is_admin(user_id):
            # Clear any leftover admin_input session so it doesn't capture this press
            if user_id in user_sessions and str(user_sessions[user_id].get('state', '')).startswith('admin_input:'):
                with _data_lock:
                    del user_sessions[user_id]
                save_sessions_async()
            send_admin_settings_menu(chat_id)
            return

        # Admin: handle pending input from the settings menu (payment, bakong, channel, admin add/remove, banner)
        if is_admin(user_id) and user_id in user_sessions:
            _state = str(user_sessions[user_id].get('state', ''))
            if _state.startswith('admin_input:'):
                _key = _state.split(':', 1)[1]
                # Special-case banner upload: capture the photo's file_id directly.
                if _key == 'banner' and message.get('photo'):
                    photos = message.get('photo') or []
                    new_file_id = photos[-1].get('file_id', '') if photos else ''
                    if new_file_id:
                        START_BANNER_FILE_ID = new_file_id
                        set_setting('START_BANNER_FILE_ID', new_file_id)
                        with _data_lock:
                            if user_id in user_sessions:
                                del user_sessions[user_id]
                        save_sessions_async()
                        send_message(chat_id,
                            "✅ <b>បាន​រក្សា​ទុក​រូប Banner ថ្មី​រួចរាល់!</b>\n\n"
                            "<i>រូប​ត្រូវ​បាន​រក្សា​ក្នុង Neon DB ហើយ​នឹង​បង្ហាញ​នៅ​ពេល​អ្នក​ប្រើ /start។</i>",
                            parse_mode="HTML", reply_to_message_id=False,
                            reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                        logger.info(f"Admin {user_id} updated start banner (file_id stored in Neon)")
                        return
                if _handle_admin_settings_input(chat_id, user_id, message_id, _key, text):
                    return

            # Admin: handle account-type pick from the delete-type reply keyboard
            if _state == 'delete_type_select':
                stripped = text.strip()
                labels = user_sessions[user_id].get('labels', {}) or {}
                type_name = labels.get(stripped)
                if type_name and type_name in accounts_data.get('account_types', {}):
                    count = len(accounts_data['account_types'].get(type_name, []))
                    price = accounts_data.get('prices', {}).get(type_name, 0)
                    with _data_lock:
                        user_sessions[user_id] = {
                            'state': 'delete_type_confirm',
                            'type_name': type_name,
                        }
                    save_sessions_async()
                    confirm_kb = {
                        'keyboard': [
                            [{'text': BTN_DELETE_CONFIRM}],
                            [{'text': BTN_DELETE_CANCEL}],
                        ],
                        'resize_keyboard': True,
                        'is_persistent': True,
                    }
                    send_message(chat_id,
                        f"⚠️ <b>តើអ្នកពិតជាចង់លុបប្រភេទ Account នេះមែនទេ?</b>\n\n"
                        f"<blockquote>🔹 ប្រភេទ: {html.escape(type_name)}\n🔹 ចំនួន Account: {count}\n🔹 តម្លៃ: ${price}</blockquote>\n\n"
                        f"Account ទាំងអស់ក្នុងប្រភេទនេះនឹងត្រូវបានលុបចោលជាអចិន្ត្រៃយ៍!",
                        parse_mode="HTML", reply_to_message_id=False,
                        reply_markup=confirm_kb)
                    return

            # Admin: handle confirm/cancel of the delete-type reply keyboard
            if _state == 'delete_type_confirm':
                stripped = text.strip()
                type_name = user_sessions[user_id].get('type_name')
                if stripped == BTN_DELETE_CONFIRM:
                    with _data_lock:
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    save_sessions_async()
                    if not type_name or type_name not in accounts_data.get('account_types', {}):
                        send_message(chat_id, "⚠️ <b>ប្រភេទនេះមិនមានទៀតហើយ!</b>",
                                     parse_mode="HTML", reply_to_message_id=False,
                                     reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                        return
                    count = len(accounts_data['account_types'].pop(type_name, []))
                    accounts_data.get('prices', {}).pop(type_name, None)
                    accounts_data['accounts'] = [
                        a for a in accounts_data.get('accounts', [])
                        if a.get('type') != type_name
                    ]
                    save_data()
                    send_message(chat_id,
                        f"✅ <b>បានលុបប្រភេទ Account <code>{html.escape(type_name)}</code> ចំនួន {count} records ដោយជោគជ័យ!</b>",
                        parse_mode="HTML", reply_to_message_id=False,
                        reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                    logger.info(f"Admin {user_id} deleted account type '{type_name}' ({count} records)")
                    return
                if stripped == BTN_DELETE_CANCEL:
                    with _data_lock:
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    save_sessions_async()
                    send_message(chat_id, "🚫 <b>បានបោះបង់ការលុបប្រភេទ Account</b>",
                                 parse_mode="HTML", reply_to_message_id=False,
                                 reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                    return

            # Admin: handle confirm/cancel of broadcast
            if _state == 'broadcast_confirm':
                stripped = text.strip()
                if stripped == BTN_BROADCAST_CONFIRM:
                    bcast_msg_id = user_sessions[user_id].get('broadcast_message_id')
                    bcast_chat_id = user_sessions[user_id].get('broadcast_chat_id') or chat_id
                    use_copy = bool(user_sessions[user_id].get('broadcast_use_copy'))
                    with _data_lock:
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    save_sessions_async()
                    if not bcast_msg_id:
                        send_message(chat_id, "⚠️ មិន​ឃើញ​សារ​ដែល​ចង់​ផ្សាយ​ទេ សូម​ចាប់ផ្ដើម​ឡើង​វិញ។",
                                     reply_to_message_id=False,
                                     reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                        return
                    send_message(chat_id, "📢 កំពុង​ផ្សាយ​សារ ... សូមរង់ចាំ",
                                 reply_to_message_id=False, reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                    background_pool.submit(_run_broadcast, bcast_chat_id, bcast_msg_id, use_copy)
                    return
                if stripped == BTN_BROADCAST_CANCEL:
                    with _data_lock:
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    save_sessions_async()
                    send_message(chat_id, "🚫 <b>បាន​បោះបង់​ការ​ផ្សាយ</b>",
                                 parse_mode="HTML", reply_to_message_id=False,
                                 reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                    return

        # Admin: route reply-keyboard button presses from the settings menu / submenus
        if is_admin(user_id) and text.strip() in ADMIN_BUTTON_LABELS:
            btn = text.strip()

            # ── Top-level settings menu actions ──
            if btn == BTN_BACK_SETTINGS:
                # Cancel any in-progress admin session before returning to settings
                if user_id in user_sessions:
                    with _data_lock:
                        del user_sessions[user_id]
                    save_sessions_async()
                send_admin_settings_menu(chat_id)
                return

            if btn == BTN_ADD_ACCOUNT:
                _start_add_account_flow(chat_id, user_id, message_id)
                return
            if btn == BTN_DELETE_TYPE:
                _show_delete_type_menu_inline(chat_id, user_id)
                return
            if btn == BTN_USERS:
                _show_users_list_inline(chat_id)
                return
            if btn == BTN_BUYERS:
                _export_buyers_report_inline(chat_id)
                return
            if btn == BTN_PAYMENT:
                _show_payment_inline(chat_id)
                return
            if btn == BTN_BAKONG:
                _show_bakong_inline(chat_id)
                return
            if btn == BTN_CHANNEL:
                _show_channel_inline(chat_id)
                return
            if btn == BTN_ADMINS:
                _show_admins_inline(chat_id)
                return
            if btn == BTN_MAINTENANCE:
                _show_maintenance_inline(chat_id)
                return
            if btn == BTN_BROADCAST:
                _prompt_admin_input(chat_id, user_id, 'broadcast',
                    "📢 សូមផ្ញើ​សារ​ដែល​ចង់​ផ្សាយ​ទៅ​អ្នក​ប្រើ​ប្រាស់​ទាំង​អស់៖\n\n"
                    "<i>សារ​នឹង​ត្រូវ​បាន Forward ទៅ​អ្នក​ប្រើ​ប្រាស់ "
                    "ដោយ​បង្ហាញ​ស្លាក “Forwarded from” ពី​គណនី​អ្នក។</i>")
                return
            if btn == BTN_BANNER:
                current = "✅ មាន​រូប Banner រួច​ហើយ" if START_BANNER_FILE_ID else "ℹ️ មិនទាន់មានរូប Banner ទេ"
                _prompt_admin_input(chat_id, user_id, 'banner',
                    f"🖼 <b>ប្តូររូប Banner</b>\n\n{current}\n\n"
                    f"សូមផ្ញើ​រូបភាព​ថ្មី​ដែល​ចង់​ប្រើ​ជា Banner ពេល​ប្រើ /start។\n"
                    f"<i>រូប​នឹង​ត្រូវ​បាន​រក្សា​ទុក​ក្នុង Neon DB (ជា file_id របស់ Telegram)។</i>")
                return

            # ── Submenu leaf actions ──
            if btn == BTN_PAYMENT_EDIT:
                _prompt_admin_input(chat_id, user_id, 'payment',
                    "💳 សូមផ្ញើ <b>ឈ្មោះ Payment</b> ថ្មី (1–60 តួអក្សរ)៖")
                return
            if btn == BTN_BAKONG_EDIT:
                _prompt_admin_input(chat_id, user_id, 'bakong',
                    "🔑 សូមផ្ញើ <b>Bakong Token</b> ថ្មី៖")
                return
            if btn == BTN_CHANNEL_EDIT:
                _prompt_admin_input(chat_id, user_id, 'channel',
                    "📢 សូមផ្ញើ <b>Channel ID</b> ថ្មី (លេខ ដូចជា <code>-1001234567890</code>)៖")
                return
            if btn == BTN_CHANNEL_CLEAR:
                CHANNEL_ID = ""
                set_setting('TELEGRAM_CHANNEL_ID', "")
                send_message(chat_id, "✅ បានលុប Channel ID រួចរាល់", parse_mode="HTML",
                             reply_to_message_id=False, reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                return
            if btn == BTN_ADMIN_ADD:
                _prompt_admin_input(chat_id, user_id, 'admin_add',
                    "➕ សូមផ្ញើ <b>Telegram User ID</b> ដែលចង់បន្ថែមជា Admin៖")
                return
            if btn == BTN_ADMIN_REMOVE:
                _prompt_admin_input(chat_id, user_id, 'admin_remove',
                    "➖ សូមផ្ញើ <b>Telegram User ID</b> ដែលចង់ដក៖")
                return
            if btn == BTN_MAINT_ON:
                MAINTENANCE_MODE = True
                set_setting('MAINTENANCE_MODE', 'true')
                send_message(chat_id, "🔴 បានបិទ Bot", parse_mode="HTML",
                             reply_to_message_id=False, reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                return
            if btn == BTN_MAINT_OFF:
                MAINTENANCE_MODE = False
                set_setting('MAINTENANCE_MODE', 'false')
                send_message(chat_id, "🟢 បានបើក Bot", parse_mode="HTML",
                             reply_to_message_id=False, reply_markup=ADMIN_SETTINGS_REPLY_KEYBOARD)
                return

        # Check if user is in a purchase session (for all users including admin)
        if user_id in user_sessions:
            session = user_sessions[user_id]

            # Handle stale payment_pending session — silently clear and show menu
            if session.get('state') == 'payment_pending':
                with _data_lock:
                    del user_sessions[user_id]
                save_sessions_async()
                show_account_selection(chat_id)
                return

            # Handle quantity input for purchase
            if session['state'] == 'waiting_for_quantity':
                try:
                    quantity = int(text.strip())
                    if quantity <= 0:
                        send_message(chat_id, "សូមបញ្ចូលចំនួនធំជាង 0")
                        return
                    
                    if quantity > session['available_count']:
                        send_message(chat_id, f"សុំទោស! មានត្រឹមតែ {session['available_count']} នៅក្នុងស្តុក")
                        return
                    
                    # Calculate total price
                    total_price = quantity * session['price']
                    
                    # Update session with purchase details, wait for confirmation
                    with _data_lock:
                        session['quantity'] = quantity
                        session['total_price'] = total_price
                        session['state'] = 'waiting_for_confirmation'
                    save_sessions_async()
                    
                    _send_order_summary(chat_id, user_id, session)
                    return
                    
                except ValueError:
                    send_message(chat_id, "សូមបញ្ចូលចំនួនជាលេខ (ឧទាហរណ៍: 1, 2, 3)")
                    return

            # Handle confirm/cancel reply keyboard buttons
            elif session['state'] == 'waiting_for_confirmation':
                if text.strip() == '✅ យល់ព្រម':
                    with _data_lock:
                        session['state'] = 'payment_pending'
                    try:
                        img_bytes, md5_or_err, qr_string = generate_payment_qr(session['total_price'])
                        if not img_bytes:
                            err_detail = md5_or_err or "មិនដឹងមូលហេតុ"
                            send_message(chat_id, "❌ *មានបញ្ហាក្នុងការបង្កើត QR Code*\n\nសូមព្យាយាមម្តងទៀត។", parse_mode="Markdown")
                            send_message(ADMIN_ID, f"⚠️ *QR Error (user {user_id}):*\n`{err_detail}`", parse_mode="Markdown")
                            with _data_lock:
                                if user_id in user_sessions:
                                    del user_sessions[user_id]
                            save_sessions_async()
                            return
                        md5_hash = md5_or_err
                        session['md5_hash'] = md5_hash
                        started_at = time.time()
                        session['qr_sent_at'] = started_at
                        amount = session['total_price']
                        dot_resp = send_sticker(chat_id, "CAACAgUAAxkBAAILvGnnaWwK-AXFeING4WOtIIKmoFYqAAIVAAMxIPsrpHGBfRB524Y7BA", reply_markup=_main_kb(user_id))
                        if dot_resp and dot_resp.get('result'):
                            session['dot_message_id'] = dot_resp['result']['message_id']
                        photo_resp = send_photo_bytes(
                            chat_id, img_bytes,
                            caption=_qr_caption(amount, PAYMENT_TIMEOUT_SECONDS),
                            parse_mode='HTML',
                            reply_markup=CHECK_PAYMENT_KEYBOARD,
                        )
                        if photo_resp and photo_resp.get('result'):
                            msg_id = photo_resp['result']['message_id']
                            session['photo_message_id'] = msg_id
                            session['qr_message_id'] = msg_id
                            _start_qr_countdown(chat_id, user_id, msg_id, md5_hash, amount, started_at)
                        save_sessions_async()
                        save_pending_payment_async(user_id, chat_id, session)
                    except Exception as e:
                        logger.error(f"Error generating KHQR: {type(e).__name__}: {e}")
                        send_message(chat_id, "❌ *មានបញ្ហាក្នុងការបង្កើត QR Code*\n\nសូមព្យាយាមម្តងទៀត។", parse_mode="Markdown")
                        with _data_lock:
                            if user_id in user_sessions:
                                del user_sessions[user_id]
                        save_sessions_async()
                    return

                elif text.strip() == '🚫 បោះបង់':
                    summary_msg_id = session.get('summary_message_id')
                    if summary_msg_id:
                        delete_message_async(chat_id, summary_msg_id)
                    dot_msg_id = session.get('dot_message_id')
                    if dot_msg_id:
                        delete_message_async(chat_id, dot_msg_id)
                    with _data_lock:
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    save_sessions_async()
                    show_account_selection(chat_id)
                    return

        # Handle non-admin users
        if not is_admin(user_id):
            # For unrecognized commands, show account selection
            logger.info(f"Non-admin user {user_id} sent unrecognized command, showing account selection")
            show_account_selection_local()
            return
        
        # Admin-only commands
        if is_admin(user_id):
            # All admin commands removed — use the ⚙️កំណត់ keyboard menu instead.

            # Check if user is in a session
            if user_id in user_sessions:
                session = user_sessions[user_id]
                
                if session['state'] == 'waiting_for_accounts':
                    # Parse email-only accounts (one per line)
                    import re
                    email_pattern = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
                    accounts = []
                    lines = text.strip().split('\n')
                    for line in lines:
                        email = line.strip()
                        if email and email_pattern.match(email):
                            accounts.append({'email': email})
                    
                    if accounts:
                        with _data_lock:
                            session['accounts'] = accounts
                            session['state'] = 'waiting_for_account_type'
                        save_sessions_async()
                        count = len(accounts)
                        send_message(chat_id, f"*បានបញ្ចូល Account ចំនួន {count}\n\nសូមបញ្ចូលប្រភេទ Account៖*", reply_to_message_id=message_id, parse_mode="Markdown", reply_markup=ADD_ACCOUNT_KEYBOARD)
                    else:
                        send_message(chat_id, "*មិនរកឃើញអ៊ីមែលត្រឹមត្រូវ! សូមបញ្ចូលតាមទម្រង់៖*\n\n```\nl1jebywyzos2@10mail.info\nabc123@gmail.com\n```", reply_to_message_id=message_id, parse_mode="Markdown", reply_markup=ADD_ACCOUNT_KEYBOARD)
                    return
                
                elif session['state'] == 'waiting_for_account_type':
                    account_type_input = text.strip()
                    with _data_lock:
                        existing_price = accounts_data.get('prices', {}).get(account_type_input)
                        session['account_type'] = account_type_input
                        session['state'] = 'waiting_for_price'
                    save_sessions_async()
                    if existing_price is not None:
                        send_message(chat_id,
                            f"*ប្រភេទ Account `{account_type_input}` មានស្រាប់ ដែលមានតម្លៃ {existing_price}$\n\nតម្លៃត្រូវតែដូចគ្នា ({existing_price}$) ដើម្បីបន្ថែម Account បាន៖*",
                            reply_to_message_id=message_id, parse_mode="Markdown", reply_markup=ADD_ACCOUNT_KEYBOARD)
                    else:
                        send_message(chat_id, f"*សូមដាក់តម្លៃក្នុងប្រភេទ Account {account_type_input}*", reply_to_message_id=message_id, parse_mode="Markdown", reply_markup=ADD_ACCOUNT_KEYBOARD)
                    return
                
                elif session['state'] == 'waiting_for_price':
                    try:
                        price = float(text.strip().replace('$', ''))
                        account_type = session['account_type']
                        accounts = session['accounts']
                        count = len(accounts)

                        # Validate price matches existing price for this account type
                        with _data_lock:
                            existing_price = accounts_data.get('prices', {}).get(account_type)
                            # Only check emails currently in stock (not already sold)
                            all_existing_emails = {
                                a.get('email', '').lower()
                                for accs in accounts_data.get('account_types', {}).values()
                                for a in accs
                                if a.get('email')
                            }

                        if existing_price is not None and round(existing_price, 4) != round(price, 4):
                            send_message(chat_id,
                                f"❌ *មិនអាចបញ្ចូលបាន!*\n\nប្រភេទ `{account_type}` មានតម្លៃ *{existing_price}$* ស្រាប់។\nតម្លៃដែលអ្នកបញ្ចូល *{price}$* មិនដូចគ្នា។\n\nសូមបញ្ចូលឡើងវិញដោយប្រើតម្លៃ *{existing_price}$*",
                                reply_to_message_id=message_id, parse_mode="Markdown")
                            return

                        # Filter out duplicates within the new batch itself
                        seen_in_batch = set()
                        deduped_accounts = []
                        for a in accounts:
                            key = a.get('email', '').lower()
                            if key not in seen_in_batch:
                                seen_in_batch.add(key)
                                deduped_accounts.append(a)
                        accounts = deduped_accounts

                        # Filter out emails already existing across all account types
                        duplicate_emails = [a['email'] for a in accounts if a.get('email', '').lower() in all_existing_emails]
                        new_accounts = [a for a in accounts if a.get('email', '').lower() not in all_existing_emails]

                        if duplicate_emails:
                            dup_list = '\n'.join(duplicate_emails)
                            if not new_accounts:
                                send_message(chat_id,
                                    f"❌ *មិនអាចបញ្ចូលបាន!*\n\nEmail ទាំងអស់មានស្រាប់ក្នុងប្រព័ន្ធ៖\n```\n{dup_list}\n```",
                                    reply_to_message_id=message_id, parse_mode="Markdown")
                                return
                            else:
                                send_message(chat_id,
                                    f"⚠️ *Email ខាងក្រោមមានស្រាប់ ហើយត្រូវបានរំលង៖*\n```\n{dup_list}\n```",
                                    reply_to_message_id=message_id, parse_mode="Markdown")

                        accounts = new_accounts
                        count = len(accounts)

                        # Save to storage
                        with _data_lock:
                            accounts_data['accounts'].extend(accounts)
                            if account_type in accounts_data['account_types']:
                                accounts_data['account_types'][account_type].extend(accounts)
                            else:
                                accounts_data['account_types'][account_type] = accounts
                            accounts_data['prices'][account_type] = price
                            if user_id in user_sessions:
                                del user_sessions[user_id]
                        save_data()
                        save_sessions_async()

                        # Send confirmation
                        send_message(chat_id, f"*✅ បានបញ្ចូល Account ដោយជោគជ័យ*\n\n```\n🔹 ចំនួន: {count}\n\n🔹 ប្រភេទ: {account_type}\n\n🔹 តម្លៃ: {price}$\n```", reply_to_message_id=message_id, parse_mode="Markdown")

                        logger.info(f"Admin {user_id} added {count} accounts of type {account_type} with price ${price}")

                    except ValueError:
                        send_message(chat_id, "តម្លៃមិនត្រឹមត្រូវ។ សូមបញ្ចូលតម្លៃជាលេខ (ឧទាហរណ៍: 5.99)", reply_to_message_id=message_id)
                    return
            
            # If admin sent a message but it's not a recognized command or part of workflow
            # Clear any existing session and show account selection interface
            if user_id in user_sessions:
                with _data_lock:
                    del user_sessions[user_id]
                logger.info(f"Cleared session for admin {user_id} due to unrecognized command")
            
            # Show account selection interface for any unrecognized admin input
            logger.info(f"Admin {user_id} sent unrecognized command, showing account selection interface")
            show_account_selection_local()
        
        # If not admin, ignore
        
    except Exception as e:
        logger.error(f"Error handling message: {e}")

CHECK_PAYMENT_KEYBOARD = {
    'inline_keyboard': [
        [
            {'text': '🚫 បោះបង់', 'callback_data': 'cancel_purchase'},
            {'text': '✅ ពិនិត្យការបង់ប្រាក់', 'callback_data': 'check_payment'}
        ]
    ]
}

def deliver_accounts(chat_id, user_id, session, payment_data=None, user_name=''):
    """Deliver purchased accounts to user after confirmed payment."""
    account_type = session['account_type']
    quantity = session['quantity']

    # Delete KHQR photo and payment message
    photo_message_id = session.get('photo_message_id')
    if photo_message_id:
        delete_message_async(chat_id, photo_message_id)
    qr_message_id = session.get('qr_message_id')
    if qr_message_id:
        delete_message_async(chat_id, qr_message_id)

    with _data_lock:
        if account_type not in accounts_data['account_types']:
            available_count = None
            delivered_accounts = None
        else:
            available_accounts = accounts_data['account_types'][account_type]
            available_count = len(available_accounts)
            if available_count < quantity:
                delivered_accounts = None
            else:
                delivered_accounts = available_accounts[:quantity]
                accounts_data['account_types'][account_type] = available_accounts[quantity:]
                if user_id in user_sessions:
                    del user_sessions[user_id]

    if delivered_accounts is None:
        if available_count is None:
            send_message(chat_id, f"❌ *មានបញ្ហា!*\n\nគ្មាន Account ប្រភេទ {account_type} ក្នុងស្តុក។",
                         parse_mode="Markdown")
        else:
            send_message(chat_id,
                         f"❌ *មានបញ្ហា!*\n\nសុំទោស! មានត្រឹមតែ {available_count} Accounts នៅក្នុងស្តុក។",
                         parse_mode="Markdown")
        return

    save_data()
    save_purchase_history_async(user_id, account_type, quantity, session.get('total_price', 0), delivered_accounts)

    accounts_message = f'<tg-emoji emoji-id="5436040291507247633">🎉</tg-emoji> <b>ការទិញបានបញ្ជាក់ដោយជោគជ័យ</b>\n\n'
    accounts_message += f"<blockquote>🔹 ប្រភេទ: {account_type}\n"
    accounts_message += f"🔹 ចំនួន: {quantity}</blockquote>\n\n"
    accounts_message += "<b>Accounts របស់អ្នក៖</b>\n\n"
    for account in delivered_accounts:
        if 'email' in account:
            accounts_message += f"{account['email']}\n"
        else:
            accounts_message += f"{account.get('phone', '')} | {account.get('password', '')}\n"
    accounts_message += f"\n<i>សូមអរគុណសម្រាប់ការទិញ <tg-emoji emoji-id=\"5897474556834091884\">🙏</tg-emoji></i>"

    send_message(chat_id, accounts_message, parse_mode="HTML", message_effect_id="5046509860389126442", reply_markup=_main_kb(user_id))

    # Notify admin/channel about successful payment
    try:
        import datetime
        cambodia_tz = datetime.timezone(datetime.timedelta(hours=7))
        now_str = datetime.datetime.now(cambodia_tz).strftime("%d/%m/%Y %H:%M")
        pd = payment_data or {}
        from_account = pd.get('fromAccountId') or pd.get('hash') or 'N/A'
        memo = pd.get('memo') or 'គ្មាន'
        ref = pd.get('externalRef') or pd.get('transactionId') or pd.get('md5') or 'N/A'
        amount = session.get('total_price', 0)
        buyer_label = f"{user_name} ({user_id})" if user_name else str(user_id)
        admin_msg = (
            "🎉 <b>ទទួលបានការបង់ប្រាក់ជោគជ័យ</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>ឈ្មោះអ្នកទិញ(ID):</b> {buyer_label}\n"
            f"💵 <b>ទឹកប្រាក់:</b> {amount} USD\n"
            f"👤 <b>ពីធនាគារ:</b> <code>{from_account}</code>\n"
            f"📝 <b>ចំណាំ:</b> {memo}\n"
            f"🧾 <b>លេខយោង:</b> <code>{ref}</code>\n"
            f"⏰ <b>ម៉ោង:</b> {now_str}"
        )
        send_purchase_notification(admin_msg)
    except Exception as e:
        logger.error(f"Failed to send admin payment notification: {e}")

    save_sessions_async()

    logger.info(f"Payment confirmed and {quantity} accounts delivered to user {user_id}")

def main():
    """Main bot loop."""
    lock_file = open('/tmp/telegram_bot_simple.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error("Another bot process is already running in this project. Exiting duplicate process.")
        return

    logger.info("Starting Telegram Bot...")
    logger.info(f"Bot token configured: {BOT_TOKEN[:10]}...")

    # Re-arm any scheduled message deletions from the DB so a cold restart
    # doesn't leak un-deleted messages.
    resume_scheduled_deletions()

    # Delete any active webhook so polling mode works without 409 conflicts
    try:
        http.post(f"{API_URL}/deleteWebhook", timeout=10)
        logger.info("Webhook deleted — polling mode active")
    except Exception as e:
        logger.warning(f"Could not delete webhook: {e}")

    # Test bot connection
    try:
        test_url = f"{API_URL}/getMe"
        response = http.get(test_url, timeout=10)
        response.raise_for_status()
        bot_info = response.json()
        
        if bot_info.get('ok'):
            bot_data = bot_info.get('result', {})
            logger.info(f"Bot connected successfully: @{bot_data.get('username', 'Unknown')}")
        else:
            logger.error("Failed to connect to bot")
            return
            
    except requests.RequestException as e:
        logger.error(f"Failed to test bot connection: {e}")
        return
    
    # Main polling loop
    offset = None
    consecutive_409 = 0
    logger.info("Bot is now polling for updates...")
    
    while True:
        try:
            updates = get_updates(offset)
            
            if not updates or not updates.get('ok'):
                time.sleep(1)
                continue
            
            consecutive_409 = 0  # reset on success
            for update in updates.get('result', []):
                offset = update['update_id'] + 1
                worker_pool.submit(handle_message, update)
                
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 409:
                consecutive_409 += 1
                if consecutive_409 % 10 == 1:
                    logger.warning(f"409 Conflict (#{consecutive_409}) — webhook active on another server. Re-deleting webhook...")
                    try:
                        http.post(f"{API_URL}/deleteWebhook", timeout=10)
                        logger.info("Webhook re-deleted, resuming polling")
                    except Exception as we:
                        logger.warning(f"Could not re-delete webhook: {we}")
                time.sleep(3)
            else:
                logger.error(f"HTTP error in main loop: {e}")
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)