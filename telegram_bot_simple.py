#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Bot — Bakong KHQR Payments
Architecture: Pyrogram (MTProto) | Full asyncio | Priority handlers | Memory cache | Pre-handler filters
"""

# ── 1. Imports ───────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import asyncio
import contextvars
import hashlib
import html
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, quote as url_quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bakong_khqr import KHQR

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from pyrogram.errors import (
    MessageDeleteForbidden, MessageNotModified, FloodWait,
    UserIsBlocked, InputUserDeactivated, PeerIdInvalid, RPCError,
)

# ── 2. Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ── 2b. Environment Validation ────────────────────────────────────────────────
_REQUIRED_ENV_VARS = {
    "TELEGRAM_BOT_TOKEN": "Bot token from @BotFather on Telegram",
    "TELEGRAM_API_ID":    "API ID from https://my.telegram.org",
    "TELEGRAM_API_HASH":  "API Hash from https://my.telegram.org",
    "NEON_DATABASE_URL":  "Neon Postgres connection string (postgresql://...)",
}

def _validate_env() -> None:
    missing = []
    for key, description in _REQUIRED_ENV_VARS.items():
        val = os.environ.get(key, "").strip()
        if not val:
            missing.append((key, description))

    if missing:
        logger.error("=" * 60)
        logger.error("STARTUP FAILED — Missing required environment variables:")
        logger.error("=" * 60)
        for key, description in missing:
            logger.error(f"  ❌  {key}")
            logger.error(f"       └─ {description}")
        logger.error("=" * 60)
        logger.error("Set these variables in your environment (e.g. .env file")
        logger.error("or VPS environment) and restart the bot.")
        logger.error("=" * 60)
        sys.exit(1)

    api_id_raw = os.environ.get("TELEGRAM_API_ID", "").strip()
    if api_id_raw and not api_id_raw.isdigit():
        logger.error("STARTUP FAILED — TELEGRAM_API_ID must be a numeric value.")
        sys.exit(1)

    logger.info("All required environment variables are present. ✓")

_validate_env()

# ── 3. Config ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
API_ID    = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH  = os.environ.get("TELEGRAM_API_HASH", "")

ADMIN_ID: int = 5002402843
EXTRA_ADMIN_IDS: set = set()
CHANNEL_ID       = os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
PAYMENT_NAME     = "RADY"
MAINTENANCE_MODE = False
PAYMENT_TIMEOUT_SECONDS = 60
PAYMENT_POLL_INTERVAL   = 5
KHMER_MESSAGE = "ជ្រើសរើស គូប៉ុង ដើម្បីបញ្ជាទិញ"

BAKONG_RELAY_TOKEN = os.environ.get("BAKONG_RELAY_TOKEN", "")
BAKONG_API_TOKEN   = os.environ.get("BAKONG_TOKEN", "")
BAKONG_TOKEN       = BAKONG_RELAY_TOKEN if BAKONG_RELAY_TOKEN else BAKONG_API_TOKEN
khqr_client        = KHQR(BAKONG_TOKEN) if BAKONG_TOKEN else None

DROPMAIL_API_TOKEN    = os.environ.get("DROPMAIL_API_TOKEN", "")
DROPMAIL_TOKEN_EXPIRY = ""
_DROPMAIL_URL         = f"https://dropmail.me/api/graphql/{DROPMAIL_API_TOKEN}"


def is_admin(uid) -> bool:
    try:
        return int(uid) == ADMIN_ID or int(uid) in EXTRA_ADMIN_IDS
    except (TypeError, ValueError):
        return False


# ── 4. Blocking HTTP session (DB + Bakong, run in thread pool) ────────────────
_retry = Retry(
    total=3, backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"], raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=50)
http = requests.Session()
http.headers.update({"Connection": "keep-alive"})
http.mount("https://", _adapter)
http.mount("http://",  _adapter)

# ── 5. In-Memory Cache ────────────────────────────────────────────────────────
class MemCache:
    """Fast TTL-based in-memory cache.  Thread-safe for asyncio (single event loop)."""

    def __init__(self):
        self._data: dict = {}
        self._exp:  dict = {}

    def get(self, key, default=None):
        if key in self._data:
            if self._exp.get(key, float("inf")) > time.monotonic():
                return self._data[key]
            del self._data[key]
            self._exp.pop(key, None)
        return default

    def set(self, key, value, ttl: float = None):
        self._data[key] = value
        if ttl is not None:
            self._exp[key] = time.monotonic() + ttl
        else:
            self._exp.pop(key, None)

    def delete(self, key):
        self._data.pop(key, None)
        self._exp.pop(key, None)

    def clear(self):
        self._data.clear()
        self._exp.clear()


cache = MemCache()

# ── 6. Async primitives ───────────────────────────────────────────────────────
_data_lock = asyncio.Lock()           # protects accounts_data + user_sessions
_user_locks: dict = {}               # per-user asyncio.Lock


def get_user_lock(user_id: int) -> asyncio.Lock:
    """Return a per-user asyncio.Lock (created lazily). Safe in single event loop."""
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


async def run_sync(fn, *args, **kwargs):
    """Run a blocking function in the default thread pool without blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ── 7a. Neon DB setup (needed before Client for API credential fallback) ────────
NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL", "")
_neon_host    = urlparse(NEON_DATABASE_URL).hostname if NEON_DATABASE_URL else ""
_neon_api_url = f"https://{_neon_host}/sql"
_neon_headers = {
    "Neon-Connection-String": NEON_DATABASE_URL,
    "Content-Type":  "application/json",
    "Accept":        "application/json",
}


def _neon_query(query: str, params=None) -> dict:
    body = {"query": query}
    if params:
        body["params"] = [str(p) if p is not None else None for p in params]
    resp = http.post(_neon_api_url, headers=_neon_headers, json=body, timeout=15)
    resp.raise_for_status()
    return resp.json()


# Load API_ID / API_HASH from DB if not provided as env vars
if not API_ID or not API_HASH:
    try:
        _r = _neon_query(
            "SELECT key, value FROM bot_settings WHERE key IN ('TELEGRAM_API_ID', 'TELEGRAM_API_HASH')"
        )
        for _row in _r.get("rows", []):
            if _row["key"] == "TELEGRAM_API_ID" and not API_ID:
                try:
                    API_ID = int(_row["value"])
                except (ValueError, TypeError):
                    pass
            elif _row["key"] == "TELEGRAM_API_HASH" and not API_HASH:
                API_HASH = _row["value"]
        if API_ID and API_HASH:
            logger.info("Loaded TELEGRAM_API_ID and TELEGRAM_API_HASH from DB settings.")
        else:
            logger.warning("TELEGRAM_API_ID or TELEGRAM_API_HASH missing — set them via the admin panel.")
    except Exception as _e:
        logger.warning(f"Could not load API credentials from DB: {_e}")

# ── 7b. Pyrogram Client ────────────────────────────────────────────────────────
app = Client(
    name="bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

_current_client: contextvars.ContextVar = contextvars.ContextVar("_current_client", default=None)

# ── 8. Database layer (Neon HTTP API — synchronous, called via run_sync) ──────
# _neon_query and NEON connection setup defined in section 7a above.


def _init_db():
    try:
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_accounts (
                id SERIAL PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'
            )""")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_sessions (
                id SERIAL PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'
            )""")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_pending_payments (
                user_id BIGINT PRIMARY KEY, chat_id BIGINT NOT NULL,
                account_type TEXT, quantity INT, total_price NUMERIC,
                md5_hash TEXT, qr_message_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )""")
        _neon_query("ALTER TABLE bot_pending_payments ADD COLUMN IF NOT EXISTS reserved_accounts JSONB DEFAULT '[]'")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_purchase_history (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                account_type TEXT, quantity INT, total_price NUMERIC,
                accounts JSONB DEFAULT '[]', purchased_at TIMESTAMPTZ DEFAULT NOW()
            )""")
        _neon_query("ALTER TABLE bot_purchase_history ADD COLUMN IF NOT EXISTS accounts JSONB DEFAULT '[]'")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_known_users (
                user_id BIGINT PRIMARY KEY, first_name TEXT, last_name TEXT,
                username TEXT, first_seen TIMESTAMPTZ DEFAULT NOW(),
                last_seen TIMESTAMPTZ DEFAULT NOW()
            )""")
        _neon_query("ALTER TABLE bot_known_users ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_sent_verifications (
                email TEXT NOT NULL, code TEXT NOT NULL,
                first_sent_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (email, code)
            )""")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY, value TEXT
            )""")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_scheduled_deletions (
                id SERIAL PRIMARY KEY, chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL, delete_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE (chat_id, message_id)
            )""")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS bot_email_buyer_map (
                email TEXT PRIMARY KEY, user_id BIGINT NOT NULL,
                account_type TEXT, purchased_at TIMESTAMPTZ DEFAULT NOW()
            )""")
        _neon_query("""
            CREATE TABLE IF NOT EXISTS email_history (
                id BIGSERIAL PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                email_address TEXT NOT NULL,
                dropmail_session_id TEXT,
                address_id TEXT,
                restore_key TEXT,
                last_mail_id TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )""")
        _neon_query("CREATE INDEX IF NOT EXISTS idx_email_history_user ON email_history(telegram_user_id)")
        _neon_query("""
            INSERT INTO bot_known_users (user_id, first_seen, last_seen, admin_notified)
            SELECT DISTINCT user_id, MIN(purchased_at), MAX(purchased_at), TRUE
            FROM bot_purchase_history GROUP BY user_id
            ON CONFLICT (user_id) DO UPDATE SET admin_notified = TRUE
        """)
        _neon_query("""
            INSERT INTO bot_email_buyer_map (email, user_id, account_type, purchased_at)
            SELECT DISTINCT ON (acc->>'email')
                acc->>'email', user_id::BIGINT, account_type, purchased_at
            FROM bot_purchase_history,
                 jsonb_array_elements(CASE jsonb_typeof(accounts)
                     WHEN 'array' THEN accounts ELSE '[]'::jsonb END) AS acc
            WHERE acc->>'email' IS NOT NULL AND acc->>'email' <> ''
            ORDER BY acc->>'email', purchased_at DESC
            ON CONFLICT (email) DO UPDATE
                SET user_id=EXCLUDED.user_id, account_type=EXCLUDED.account_type,
                    purchased_at=EXCLUDED.purchased_at
        """)
        r = _neon_query("SELECT COUNT(*) as cnt FROM bot_accounts")
        if int(r["rows"][0]["cnt"]) == 0:
            _neon_query("INSERT INTO bot_accounts (data) VALUES ($1)",
                        [json.dumps({"accounts": [], "account_types": {}, "prices": {}})])
        r = _neon_query("SELECT COUNT(*) as cnt FROM bot_sessions")
        if int(r["rows"][0]["cnt"]) == 0:
            _neon_query("INSERT INTO bot_sessions (data) VALUES ($1)", [json.dumps({})])
        logger.info("Replit PostgreSQL DB initialized")
    except Exception as e:
        logger.error(f"DB init failed: {e}")


def _get_setting(key, default=None):
    cached = cache.get(f"setting:{key}")
    if cached is not None:
        return cached
    try:
        r = _neon_query("SELECT value FROM bot_settings WHERE key = $1", [key])
        rows = r.get("rows", [])
        val = rows[0].get("value") if rows else default
        if val is not None:
            cache.set(f"setting:{key}", val, ttl=300)
        return val
    except Exception as e:
        logger.error(f"Failed to read setting {key}: {e}")
        return default


def _set_setting(key, value):
    cache.set(f"setting:{key}", str(value), ttl=300)
    try:
        _neon_query("""
            INSERT INTO bot_settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, [key, str(value)])
    except Exception as e:
        logger.error(f"Failed to save setting {key}: {e}")


def _load_data():
    try:
        r = _neon_query("SELECT data FROM bot_accounts LIMIT 1")
        if r["rows"]:
            data = r["rows"][0]["data"]
            if isinstance(data, str):
                data = json.loads(data)
            logger.info("Loaded accounts data from Neon DB")
            return data
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
    return {"accounts": [], "account_types": {}, "prices": {}}


def _save_data():
    try:
        _neon_query("UPDATE bot_accounts SET data = $1",
                    [json.dumps(accounts_data, ensure_ascii=False)])
    except Exception as e:
        logger.error(f"Failed to save data: {e}")


def _load_sessions():
    global user_sessions
    try:
        r = _neon_query("SELECT data FROM bot_sessions LIMIT 1")
        if r["rows"]:
            data = r["rows"][0]["data"]
            if isinstance(data, str):
                data = json.loads(data)
            user_sessions = {int(k): v for k, v in data.items()}
            logger.info("Loaded sessions from Neon DB")
    except Exception as e:
        logger.error(f"Failed to load sessions: {e}")


def _save_sessions():
    try:
        payload = {str(k): v for k, v in user_sessions.items()}
        encoded = json.dumps(payload, ensure_ascii=False).replace("'", "''")
        _neon_query(f"UPDATE bot_sessions SET data = '{encoded}'::jsonb")
    except Exception as e:
        logger.error(f"Failed to save sessions: {e}")


# ── Dropmail GraphQL API (blocking, called via run_sync) ──────────────────────
def _dropmail_gql(query: str, variables: dict = None) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = http.post(_DROPMAIL_URL, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _dropmail_create_session() -> dict:
    q = """mutation { introduceSession {
        id expiresAt
        addresses { id address restoreKey }
    } }"""
    data = _dropmail_gql(q)
    sess = data.get("data", {}).get("introduceSession")
    if not sess:
        return {}
    addr = sess["addresses"][0] if sess.get("addresses") else {}
    return {
        "session_id": sess["id"],
        "email":      addr.get("address"),
        "address_id": addr.get("id"),
        "restore_key": addr.get("restoreKey"),
    }


def _dropmail_restore_session(mail_address: str, restore_key: str) -> dict:
    new_q = """mutation { introduceSession(input: { withAddress: false }) { id } }"""
    data = _dropmail_gql(new_q)
    new_sess = data.get("data", {}).get("introduceSession")
    if not new_sess:
        return {}
    new_id = new_sess["id"]
    restore_q = """mutation Restore($m:String!,$r:String!,$s:ID!) {
        restoreAddress(input:{mailAddress:$m,restoreKey:$r,sessionId:$s}) {
            id address restoreKey
        }
    }"""
    r = _dropmail_gql(restore_q, {"m": mail_address, "r": restore_key, "s": new_id})
    addr = r.get("data", {}).get("restoreAddress")
    if not addr:
        return {}
    return {
        "session_id":  new_id,
        "email":       addr.get("address"),
        "address_id":  addr.get("id"),
        "restore_key": addr.get("restoreKey"),
    }


def _dropmail_get_mails(session_id: str, after_mail_id: str = None):
    """Returns list of mails, or None if session expired."""
    if after_mail_id:
        q = """query G($id:ID!,$mid:ID!) {
            session(id:$id){ mailsAfterId(mailId:$mid){id fromAddr toAddr headerSubject text} }
        }"""
        v = {"id": session_id, "mid": after_mail_id}
    else:
        q = """query G($id:ID!) {
            session(id:$id){ mails{id fromAddr toAddr headerSubject text} }
        }"""
        v = {"id": session_id}
    data = _dropmail_gql(q, v)
    sess_data = data.get("data", {}).get("session")
    if sess_data is None:
        return None
    return sess_data.get("mailsAfterId") or sess_data.get("mails") or []


def _dropmail_delete_address(address_id: str) -> bool:
    q = """mutation D($a:ID!) { deleteAddress(input:{addressId:$a}) }"""
    try:
        data = _dropmail_gql(q, {"a": address_id})
        return bool(data.get("data", {}).get("deleteAddress"))
    except Exception:
        return False


def _dropmail_check_token_info() -> dict:
    """Query Dropmail API to verify token validity and get expiry info."""
    try:
        # Try tokenInfo query first
        q = """query { tokenInfo { expiresAt requestsRemaining } }"""
        data = _dropmail_gql(q)
        info = data.get("data", {}).get("tokenInfo") or {}
        if info:
            raw_exp = info.get("expiresAt") or "N/A"
            remaining = info.get("requestsRemaining")
            return {
                "valid": True,
                "expires": raw_exp,
                "remaining": remaining,
            }
        # Fallback: just test connectivity with __typename
        q2 = """query { __typename }"""
        data2 = _dropmail_gql(q2)
        if data2.get("data"):
            return {"valid": True, "expires": "N/A", "remaining": None}
        return {"valid": False, "expires": "N/A", "remaining": None}
    except Exception as e:
        return {"valid": False, "expires": "N/A", "remaining": None, "error": str(e)}


# ── Email history DB helpers (Neon HTTP API) ──────────────────────────────────
def _email_history_add(user_id: int, email_address: str, session_id: str,
                       address_id: str, restore_key: str):
    try:
        _neon_query("""
            INSERT INTO email_history
                (telegram_user_id, email_address, dropmail_session_id,
                 address_id, restore_key)
            VALUES ($1,$2,$3,$4,$5)
        """, [str(user_id), email_address, session_id, address_id, restore_key])
    except Exception as e:
        logger.error(f"_email_history_add failed: {e}")


def _email_history_list(user_id: int) -> list:
    try:
        r = _neon_query(
            "SELECT email_address FROM email_history WHERE telegram_user_id=$1 ORDER BY created_at DESC",
            [str(user_id)])
        return [row["email_address"] for row in r.get("rows", [])]
    except Exception as e:
        logger.error(f"_email_history_list failed: {e}")
        return []


def _email_history_entries(user_id: int) -> list:
    try:
        r = _neon_query("""
            SELECT id, telegram_user_id, email_address, dropmail_session_id,
                   address_id, restore_key, last_mail_id
            FROM email_history WHERE telegram_user_id=$1 ORDER BY created_at DESC
        """, [str(user_id)])
        return r.get("rows", [])
    except Exception as e:
        logger.error(f"_email_history_entries failed: {e}")
        return []


def _email_history_all_entries() -> list:
    try:
        r = _neon_query("""
            SELECT id, telegram_user_id, email_address, dropmail_session_id,
                   address_id, restore_key, last_mail_id
            FROM email_history WHERE restore_key IS NOT NULL
        """)
        return r.get("rows", [])
    except Exception as e:
        logger.error(f"_email_history_all_entries failed: {e}")
        return []


def _email_history_get_by_id(entry_id: int) -> dict:
    try:
        r = _neon_query("""
            SELECT id, email_address, address_id
            FROM email_history WHERE id=$1 LIMIT 1
        """, [str(entry_id)])
        rows = r.get("rows", [])
        return rows[0] if rows else {}
    except Exception as e:
        logger.error(f"_email_history_get_by_id failed: {e}")
        return {}


def _email_history_delete(entry_id: int):
    try:
        _neon_query("DELETE FROM email_history WHERE id=$1", [str(entry_id)])
    except Exception as e:
        logger.error(f"_email_history_delete failed: {e}")


def _email_history_update_session(entry_id: int, session_id: str,
                                  address_id: str, restore_key: str):
    try:
        _neon_query("""
            UPDATE email_history
            SET dropmail_session_id=$1, address_id=$2, restore_key=$3, last_mail_id=NULL
            WHERE id=$4
        """, [session_id, address_id, restore_key, str(entry_id)])
    except Exception as e:
        logger.error(f"_email_history_update_session failed: {e}")


def _email_history_update_last_mail(entry_id: int, mail_id: str):
    try:
        _neon_query("UPDATE email_history SET last_mail_id=$1 WHERE id=$2",
                    [mail_id, str(entry_id)])
    except Exception as e:
        logger.error(f"_email_history_update_last_mail failed: {e}")


def _email_history_get_by_email(user_id: int, email_address: str) -> dict:
    try:
        r = _neon_query("""
            SELECT id, telegram_user_id, email_address, dropmail_session_id,
                   address_id, restore_key, last_mail_id
            FROM email_history
            WHERE telegram_user_id=$1 AND email_address=$2
            ORDER BY created_at DESC LIMIT 1
        """, [str(user_id), email_address])
        rows = r.get("rows", [])
        return rows[0] if rows else {}
    except Exception as e:
        logger.error(f"_email_history_get_by_email failed: {e}")
        return {}


def _save_pending_payment(user_id, chat_id, session):
    try:
        reserved = session.get("reserved_accounts") or []
        _neon_query("""
            INSERT INTO bot_pending_payments
                (user_id, chat_id, account_type, quantity, total_price, md5_hash, qr_message_id, reserved_accounts)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (user_id) DO UPDATE SET
                chat_id=EXCLUDED.chat_id, account_type=EXCLUDED.account_type,
                quantity=EXCLUDED.quantity, total_price=EXCLUDED.total_price,
                md5_hash=EXCLUDED.md5_hash, qr_message_id=EXCLUDED.qr_message_id,
                reserved_accounts=EXCLUDED.reserved_accounts, created_at=NOW()
        """, [str(user_id), str(chat_id),
              session.get("account_type"), str(session.get("quantity", 1)),
              str(session.get("total_price", 0)), session.get("md5_hash"),
              str(session.get("qr_message_id", 0)),
              json.dumps(reserved, ensure_ascii=False)])
        logger.info(f"Saved pending payment for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to save pending payment: {e}")


def _delete_pending_payment(user_id):
    try:
        _neon_query("DELETE FROM bot_pending_payments WHERE user_id = $1", [str(user_id)])
        logger.info(f"Deleted pending payment for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to delete pending payment: {e}")


def _get_pending_payment(user_id):
    try:
        r = _neon_query("SELECT * FROM bot_pending_payments WHERE user_id = $1", [str(user_id)])
        if r["rows"]:
            row = r["rows"][0]
            reserved = row.get("reserved_accounts") or []
            if isinstance(reserved, str):
                try:
                    reserved = json.loads(reserved)
                except Exception:
                    reserved = []
            return {
                "state": "payment_pending",
                "account_type": row.get("account_type"),
                "quantity": int(row.get("quantity") or 1),
                "total_price": float(row.get("total_price") or 0),
                "md5_hash": row.get("md5_hash"),
                "qr_message_id": int(row.get("qr_message_id") or 0),
                "chat_id": int(row.get("chat_id") or 0),
                "reserved_accounts": reserved,
            }
    except Exception as e:
        logger.error(f"Failed to get pending payment: {e}")
    return None


def _save_purchase_history(user_id, account_type, quantity, total_price, accounts=None):
    try:
        accounts_list = accounts or []
        _neon_query(
            "INSERT INTO bot_purchase_history (user_id,account_type,quantity,total_price,accounts) VALUES ($1,$2,$3,$4,$5)",
            [str(user_id), account_type, str(quantity), str(total_price),
             json.dumps(accounts_list, ensure_ascii=False)])
        for acc in accounts_list:
            if isinstance(acc, dict) and acc.get("email"):
                try:
                    _neon_query("""
                        INSERT INTO bot_email_buyer_map (email, user_id, account_type)
                        VALUES ($1,$2,$3)
                        ON CONFLICT (email) DO UPDATE
                            SET user_id=EXCLUDED.user_id, account_type=EXCLUDED.account_type, purchased_at=NOW()
                    """, [str(acc["email"]).strip().lower(), str(user_id), account_type])
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Failed to save purchase history: {e}")


def _get_purchase_history(user_id, limit=10):
    try:
        r = _neon_query(
            "SELECT account_type,quantity,total_price,accounts,purchased_at "
            "FROM bot_purchase_history WHERE user_id=$1 ORDER BY purchased_at DESC LIMIT $2",
            [str(user_id), str(limit)])
        return r.get("rows", [])
    except Exception as e:
        logger.error(f"Failed to get purchase history: {e}")
        return []


def _find_buyer_by_email(email):
    email = (email or "").strip().lower()
    if not email:
        return None
    try:
        r = _neon_query("SELECT user_id FROM bot_email_buyer_map WHERE LOWER(email)=$1", [email])
        if r.get("rows"):
            return int(r["rows"][0]["user_id"])
    except Exception:
        pass
    try:
        r = _neon_query(
            "SELECT user_id FROM bot_purchase_history WHERE accounts @> $1::jsonb ORDER BY purchased_at DESC LIMIT 1",
            [json.dumps([{"email": email}])])
        if r.get("rows"):
            uid = int(r["rows"][0]["user_id"])
            try:
                _neon_query("""
                    INSERT INTO bot_email_buyer_map (email, user_id)
                    VALUES ($1,$2) ON CONFLICT (email) DO UPDATE SET user_id=EXCLUDED.user_id, purchased_at=NOW()
                """, [email, str(uid)])
            except Exception:
                pass
            return uid
    except Exception as e:
        logger.error(f"Failed to find buyer by email: {e}")
    return None


def _find_all_buyers_by_email(email):
    email = (email or "").strip().lower()
    if not email:
        return []
    buyers, seen = [], set()
    try:
        r = _neon_query(
            "SELECT user_id, MAX(purchased_at) AS last_at FROM bot_purchase_history "
            "WHERE accounts @> $1::jsonb GROUP BY user_id ORDER BY last_at DESC",
            [json.dumps([{"email": email}])])
        for row in r.get("rows", []):
            uid = int(row["user_id"])
            if uid not in seen:
                seen.add(uid)
                buyers.append(uid)
    except Exception:
        pass
    return buyers


def _filter_out_already_sold(user_id, reserved):
    try:
        rows = _neon_query(
            "SELECT accounts FROM bot_purchase_history WHERE user_id=$1 ORDER BY purchased_at DESC LIMIT 50",
            [str(user_id)]).get("rows", [])
    except Exception:
        return reserved
    sold_keys = set()
    for row in rows:
        accs = row.get("accounts") or []
        if isinstance(accs, str):
            try:
                accs = json.loads(accs)
            except Exception:
                accs = []
        for a in accs:
            if isinstance(a, dict):
                k = a.get("email") or a.get("phone")
                if k:
                    sold_keys.add(str(k))
    if not sold_keys:
        return reserved
    kept, dropped = [], 0
    for a in reserved:
        if not isinstance(a, dict):
            kept.append(a)
            continue
        k = a.get("email") or a.get("phone")
        if k and str(k) in sold_keys:
            dropped += 1
        else:
            kept.append(a)
    if dropped:
        logger.info(f"Skipped re-stocking {dropped} already-sold account(s) for user {user_id}")
    return kept


def _drain_bot_api_queue():
    """Consume any updates sitting in Telegram's Bot API HTTP queue.
    Pyrogram uses MTProto for updates, but stale Bot-API-queued updates can
    prevent new MTProto pushes from arriving. Draining on startup fixes this."""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
        # First call: find the highest update_id
        resp = http.get(url, params={"limit": 100, "timeout": 0}, timeout=15)
        result = resp.json().get("result", [])
        if not result:
            return
        max_id = max(u["update_id"] for u in result)
        # Second call: acknowledge them all by advancing offset past the last one
        http.get(url, params={"offset": max_id + 1, "limit": 1, "timeout": 0}, timeout=15)
        logger.info(f"Drained {len(result)} stale Bot API update(s) (last id={max_id})")
    except Exception as e:
        logger.warning(f"Bot API queue drain failed (non-fatal): {e}")


def _cleanup_expired_pending_payments():
    try:
        r = _neon_query(
            "SELECT user_id, account_type, reserved_accounts FROM bot_pending_payments "
            "WHERE created_at + ($1 || ' seconds')::interval < NOW()",
            [str(PAYMENT_TIMEOUT_SECONDS)])
        rows = r.get("rows", []) or []
        if not rows:
            return
        released = 0
        for row in rows:
            try:
                reserved = row.get("reserved_accounts") or []
                if isinstance(reserved, str):
                    try:
                        reserved = json.loads(reserved)
                    except Exception:
                        reserved = []
                user_id = row.get("user_id")
                if reserved and user_id is not None:
                    reserved = _filter_out_already_sold(user_id, reserved)
                fake_session = {"account_type": row.get("account_type"), "reserved_accounts": reserved}
                if reserved:
                    _release_reserved_accounts_sync(fake_session)
                    released += len(reserved)
                if user_id is not None:
                    _neon_query("DELETE FROM bot_pending_payments WHERE user_id=$1", [str(user_id)])
            except Exception as e:
                logger.warning(f"Bad expired payment row {row}: {e}")
        logger.info(f"Cleaned {len(rows)} expired payment(s); released {released} account(s)")
    except Exception as e:
        logger.error(f"Failed to clean expired payments: {e}")


def _record_scheduled_deletion(chat_id, message_id, delay_seconds):
    try:
        _neon_query("""
            INSERT INTO bot_scheduled_deletions (chat_id, message_id, delete_at)
            VALUES ($1,$2, NOW() + ($3 || ' seconds')::interval)
            ON CONFLICT (chat_id, message_id) DO UPDATE SET delete_at=EXCLUDED.delete_at
        """, [str(chat_id), str(message_id), str(delay_seconds)])
    except Exception as e:
        logger.error(f"Failed to record scheduled deletion: {e}")


def _clear_scheduled_deletion(chat_id, message_id):
    try:
        _neon_query(
            "DELETE FROM bot_scheduled_deletions WHERE chat_id=$1 AND message_id=$2",
            [str(chat_id), str(message_id)])
    except Exception as e:
        logger.error(f"Failed to clear scheduled deletion: {e}")


def _is_admin_notified(uid: int) -> bool:
    if uid in _notified_users:
        return True
    try:
        r = _neon_query("SELECT admin_notified FROM bot_known_users WHERE user_id=$1", [str(uid)])
        rows = r.get("rows", [])
        if rows and rows[0].get("admin_notified"):
            _notified_users.add(uid)
            return True
    except Exception:
        pass
    return False


# ── 9. KHQR / Payment helpers (sync, run via run_sync) ────────────────────────
def _crc16_ccitt(data: str) -> str:
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
    if phone.startswith("855"):
        phone_local = "0" + phone[3:]
    else:
        phone_local = phone[-9:] if len(phone) > 9 else phone
    add_data = (_tlv("03", store_label) + _tlv("02", phone_local) +
                _tlv("01", bill_number) + _tlv("07", terminal_label))
    now_ms = str(int(time.time() * 1000))
    exp_ms = str(int((time.time() + 86400) * 1000))
    info_data = _tlv("00", now_ms) + _tlv("01", exp_ms)
    body = (
        _tlv("00", "01") + _tlv("01", "12") +
        _tlv("29", _tlv("00", bank_account)) + _tlv("52", "5999") +
        _tlv("53", "840") + _tlv("54", f"{amount:.2f}") +
        _tlv("58", "KH") + _tlv("59", merchant_name) +
        _tlv("60", merchant_city) + _tlv("62", add_data) +
        _tlv("99", info_data) + "6304"
    )
    return body + _crc16_ccitt(body)


def _compute_md5(qr: str) -> str:
    return hashlib.md5(qr.encode("utf-8")).hexdigest()


def _generate_payment_qr(amount):
    """Returns (img_bytes, md5, qr_string) or (None, error_msg, None)."""
    if not BAKONG_TOKEN or not khqr_client:
        return None, "BAKONG_TOKEN មិនមាន", None
    try:
        bill_number = f"TRX{int(time.time())}"
        try:
            try:
                qr = khqr_client.create_qr(
                    bank_account="sovannrady@aclb", merchant_name=PAYMENT_NAME,
                    merchant_city="KPS", amount=amount, currency="USD",
                    store_label=PAYMENT_NAME, phone_number="85593330905",
                    bill_number=bill_number, terminal_label="Cashier-01",
                    static=False, expiration=1)
            except TypeError:
                qr = khqr_client.create_qr(
                    bank_account="sovannrady@aclb", merchant_name=PAYMENT_NAME,
                    merchant_city="KPS", amount=amount, currency="USD",
                    store_label=PAYMENT_NAME, phone_number="85593330905",
                    bill_number=bill_number, terminal_label="Cashier-01", static=False)
            if "5303840" not in qr or "5404" not in qr:
                qr = _build_khqr_manual(
                    "sovannrady@aclb", PAYMENT_NAME, "KPS", amount,
                    bill_number, "85593330905", PAYMENT_NAME, "Cashier-01")
        except Exception as e:
            return None, f"create_qr failed: {e}", None

        md5 = _compute_md5(qr)
        img_bytes = None
        try:
            img_bytes = khqr_client.qr_image(qr, format="bytes")
        except Exception as e1:
            logger.warning(f"bakong-khqr image failed: {e1}")
        if not img_bytes:
            try:
                import qrcode as _qrcode
                buf = io.BytesIO()
                _qrcode.make(qr).save(buf, format="PNG")
                img_bytes = buf.getvalue()
            except Exception as e2:
                logger.warning(f"qrcode lib failed: {e2}")
        if not img_bytes:
            try:
                resp = http.get(
                    f"https://api.qrserver.com/v1/create-qr-code/?size=500x500&data={url_quote(qr)}",
                    timeout=10)
                resp.raise_for_status()
                img_bytes = resp.content
            except Exception as e3:
                return None, f"All QR methods failed: {e3}", None
        return img_bytes, md5, qr
    except Exception as e:
        return None, f"Unexpected: {e}", None


def _bakong_api_url(token=None):
    t = token or BAKONG_TOKEN
    if t and t.startswith("rbk"):
        return "https://api.bakongrelay.com/v1"
    return "https://api-bakong.nbc.gov.kh/v1"


def _check_payment_status(md5):
    """Returns (is_paid: bool, payment_data: dict|None)."""
    tokens = []
    if BAKONG_RELAY_TOKEN:
        tokens.append(BAKONG_RELAY_TOKEN)
    if BAKONG_API_TOKEN and BAKONG_API_TOKEN not in tokens:
        tokens.append(BAKONG_API_TOKEN)
    if not tokens and BAKONG_TOKEN:
        tokens.append(BAKONG_TOKEN)
    for token in tokens:
        try:
            base = _bakong_api_url(token)
            resp = http.post(
                f"{base}/check_transaction_by_md5",
                json={"md5": md5},
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                timeout=10)
            data = resp.json()
            logger.info(f"check_payment via {'relay' if token.startswith('rbk') else 'bakong'}: "
                        f"status={resp.status_code} responseCode={data.get('responseCode')}")
            if data.get("responseCode") == 0:
                return True, data.get("data", {})
        except Exception as e:
            logger.warning(f"check_payment token error: {e}")
    return False, None


# ── 10. Global state ──────────────────────────────────────────────────────────
accounts_data: dict = {}
user_sessions: dict = {}
_notified_users: set = set()

# ── 11. Keyboard builders ─────────────────────────────────────────────────────
BTN_ADD_ACCOUNT       = "➕ បន្ថែម គូប៉ុង"
BTN_DELETE_TYPE       = "🗑 លុបប្រភេទ"
BTN_STOCK             = "📦 ស្តុក គូប៉ុង"
BTN_USERS             = "👥 អ្នកប្រើប្រាស់"
BTN_BUYERS            = "📋 របាយការណ៍ទិញ"
BTN_PAYMENT           = "💳 ឈ្មោះ Payment"
BTN_BAKONG            = "🔑 Bakong Token"
BTN_CHANNEL           = "📢 Channel ID"
BTN_ADMINS            = "👑 គ្រប់គ្រង Admin"
BTN_MAINTENANCE       = "🛠 Maintenance Mode"
BTN_BROADCAST         = "📢 ផ្សាយព័ត៌មាន"
BTN_BACK_SETTINGS     = "⬅️ ត្រឡប់ទៅកំណត់"
BTN_PAYMENT_EDIT      = "✏️ ប្តូរឈ្មោះ Payment"
BTN_BAKONG_API_EDIT   = "✏️ ប្តូរ Bakong Token"
BTN_BAKONG_TOKEN_INFO = "📅 ព័ត៌មាន Token"
BTN_CHANNEL_EDIT      = "✏️ ប្តូរ Channel ID"
BTN_CHANNEL_CLEAR     = "🗑 លុប Channel ID"
BTN_ADMIN_ADD         = "➕ បន្ថែម Admin"
BTN_ADMIN_REMOVE      = "➖ ដក Admin"
BTN_MAINT_ON          = "🔴 បិទ Bot"
BTN_MAINT_OFF         = "🟢 បើក Bot"
BTN_CANCEL_INPUT      = "🚫 បោះបង់"
BTN_DELETE_CONFIRM    = "✅ បញ្ជាក់លុប"
BTN_DELETE_CANCEL     = "🚫 បោះបង់ការលុប"
BTN_BROADCAST_CONFIRM = "✅ បញ្ជាក់ផ្សាយ"
BTN_BROADCAST_CANCEL  = "🚫 បោះបង់ការផ្សាយ"
ADMIN_SETTINGS_BTN    = "⚙️កំណត់"

BTN_EMAIL_MGMT        = "📧 អ៊ីម៉ែល"
BTN_EMAIL_NEW         = "✉️ អ៊ីម៉ែលថ្មី"
BTN_EMAIL_INBOX       = "📥 ពិនិត្យប្រអប់"
BTN_EMAIL_LIST        = "📓 បញ្ជីអ៊ីម៉ែល"
BTN_EMAIL_DELETE      = "🗑️ លុបអ៊ីម៉ែល"
BTN_EMAIL_TOKEN_EDIT  = "✏️ ប្តូរ Dropmail Token"
BTN_EMAIL_TOKEN_INFO  = "📅 ព័ត៌មាន Token"



ADMIN_BUTTON_LABELS = {
    BTN_ADD_ACCOUNT, BTN_DELETE_TYPE, BTN_STOCK, BTN_USERS, BTN_BUYERS,
    BTN_PAYMENT, BTN_BAKONG, BTN_CHANNEL, BTN_ADMINS, BTN_MAINTENANCE, BTN_BROADCAST,
    BTN_BACK_SETTINGS, BTN_PAYMENT_EDIT, BTN_BAKONG_API_EDIT, BTN_BAKONG_TOKEN_INFO,
    BTN_CHANNEL_EDIT, BTN_CHANNEL_CLEAR, BTN_ADMIN_ADD, BTN_ADMIN_REMOVE,
    BTN_MAINT_ON, BTN_MAINT_OFF,
    BTN_EMAIL_MGMT, BTN_EMAIL_NEW, BTN_EMAIL_LIST, BTN_EMAIL_DELETE,
    BTN_EMAIL_TOKEN_EDIT, BTN_EMAIL_TOKEN_INFO,
}

MAIN_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("💵 ទិញគូប៉ុង")]],
    resize_keyboard=True, is_persistent=True)

ADMIN_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(ADMIN_SETTINGS_BTN)]],
    resize_keyboard=True, is_persistent=True)

ADMIN_SETTINGS_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_ADD_ACCOUNT),  KeyboardButton(BTN_DELETE_TYPE)],
    [KeyboardButton(BTN_STOCK),        KeyboardButton(BTN_BUYERS)],
    [KeyboardButton(BTN_USERS),        KeyboardButton(BTN_EMAIL_MGMT)],
    [KeyboardButton(BTN_PAYMENT),      KeyboardButton(BTN_BAKONG)],
    [KeyboardButton(BTN_CHANNEL),      KeyboardButton(BTN_ADMINS)],
    [KeyboardButton(BTN_MAINTENANCE),  KeyboardButton(BTN_BROADCAST)],
], resize_keyboard=True, is_persistent=True)

CANCEL_INPUT_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_CANCEL_INPUT)]], resize_keyboard=True, is_persistent=True)

ADD_ACCOUNT_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_BACK_SETTINGS)]], resize_keyboard=True, is_persistent=True)

PAYMENT_SUBMENU_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_PAYMENT_EDIT)], [KeyboardButton(BTN_BACK_SETTINGS)]],
    resize_keyboard=True, is_persistent=True)

BAKONG_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_BAKONG_API_EDIT), KeyboardButton(BTN_BAKONG_TOKEN_INFO)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)

CHANNEL_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_CHANNEL_EDIT), KeyboardButton(BTN_CHANNEL_CLEAR)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)

ADMINS_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_ADMIN_ADD), KeyboardButton(BTN_ADMIN_REMOVE)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)

MAINTENANCE_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_MAINT_ON), KeyboardButton(BTN_MAINT_OFF)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)

BROADCAST_CONFIRM_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_BROADCAST_CONFIRM)],
    [KeyboardButton(BTN_BROADCAST_CANCEL)],
], resize_keyboard=True, is_persistent=True)

BACK_SETTINGS_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_BACK_SETTINGS)]], resize_keyboard=True, is_persistent=True)

EMAIL_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_EMAIL_NEW),         KeyboardButton(BTN_EMAIL_LIST)],
    [KeyboardButton(BTN_EMAIL_DELETE)],
    [KeyboardButton(BTN_EMAIL_TOKEN_EDIT),  KeyboardButton(BTN_EMAIL_TOKEN_INFO)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)



CHECK_PAYMENT_INLINE = InlineKeyboardMarkup([
    [InlineKeyboardButton("🚫 បោះបង់", callback_data="cancel_purchase")]
])


def _main_kb(uid):
    return ADMIN_KB if is_admin(uid) else ReplyKeyboardRemove()


def _type_callback_id(account_type: str) -> str:
    return hashlib.sha1(account_type.encode("utf-8")).hexdigest()[:12]


def _account_type_from_callback_id(cid: str):
    for at in accounts_data.get("account_types", {}):
        if _type_callback_id(at) == cid:
            return at
    return None


def _short_label(text, limit=36):
    clean = " ".join(str(text).split())
    return clean if len(clean) <= limit else clean[: limit - 1] + "…"


# ── 12. Async send helpers ────────────────────────────────────────────────────
def _get_client():
    """Return the active Pyrogram client for the current coroutine (main or clone)."""
    return _current_client.get() or app


def _botapi_send_copy_button(chat_id, text, code: str) -> None:
    """Blocking: send a message with a native copy_text button via Bot API HTTP."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "📋 Copy Code", "copy_text": {"text": code}}
            ]]
        },
    }
    try:
        http.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.warning(f"[botapi_send_copy_button] failed: {e}")


async def send_msg(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=None,
                   reply_to_message_id=None, message_effect_id=None):
    client = _get_client()
    try:
        kwargs = dict(chat_id=chat_id, text=text, parse_mode=parse_mode)
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        if reply_to_message_id:
            kwargs["reply_to_message_id"] = reply_to_message_id
        if message_effect_id:
            kwargs["message_effect_id"] = message_effect_id
        try:
            return await client.send_message(**kwargs)
        except TypeError:
            kwargs.pop("message_effect_id", None)
            return await client.send_message(**kwargs)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await send_msg(chat_id, text, parse_mode, reply_markup, reply_to_message_id, message_effect_id)
    except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
        pass
    except Exception as e:
        logger.error(f"send_msg({chat_id}) error: {e}")
    return None


async def delete_msg(chat_id, message_id):
    if not message_id:
        return
    try:
        await _get_client().delete_messages(chat_id, message_id)
    except (MessageDeleteForbidden, RPCError):
        pass
    except Exception as e:
        logger.warning(f"delete_msg({chat_id},{message_id}): {e}")


async def delete_msg_later(chat_id, message_id, delay_seconds=120):
    if not message_id:
        return
    await run_sync(_record_scheduled_deletion, chat_id, message_id, delay_seconds)

    async def _delayed():
        await asyncio.sleep(delay_seconds)
        await delete_msg(chat_id, message_id)
        await run_sync(_clear_scheduled_deletion, chat_id, message_id)

    asyncio.create_task(_delayed())


async def send_photo(chat_id, img_bytes, caption=None, parse_mode=ParseMode.HTML, reply_markup=None):
    client = _get_client()
    try:
        buf = io.BytesIO(img_bytes)
        buf.name = "qr.png"
        kwargs = dict(chat_id=chat_id, photo=buf)
        if caption:
            kwargs["caption"] = caption
            kwargs["parse_mode"] = parse_mode
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        return await client.send_photo(**kwargs)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await send_photo(chat_id, img_bytes, caption, parse_mode, reply_markup)
    except Exception as e:
        logger.error(f"send_photo({chat_id}) error: {e}")
    return None


async def send_document(chat_id, data_bytes, filename, caption=None):
    try:
        buf = io.BytesIO(data_bytes)
        buf.name = filename
        return await _get_client().send_document(chat_id, document=buf, caption=caption)
    except Exception as e:
        logger.error(f"send_document({chat_id}) error: {e}")
    return None


async def copy_msg(to_chat_id, from_chat_id, message_id):
    try:
        return await _get_client().copy_message(to_chat_id, from_chat_id, message_id)
    except Exception as e:
        logger.error(f"copy_msg error: {e}")
    return None


async def forward_msg(to_chat_id, from_chat_id, message_id):
    try:
        return await _get_client().forward_messages(to_chat_id, from_chat_id, message_id)
    except Exception as e:
        logger.error(f"forward_msg error: {e}")
    return None


async def edit_caption(chat_id, message_id, caption, parse_mode=ParseMode.HTML, reply_markup=None):
    try:
        kwargs = dict(chat_id=chat_id, message_id=message_id, caption=caption, parse_mode=parse_mode)
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        return await _get_client().edit_message_caption(**kwargs)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.warning(f"edit_caption error: {e}")
    return None


# ── 13. Business logic helpers ────────────────────────────────────────────────
async def _has_active_purchase(user_id: int) -> bool:
    async with _data_lock:
        sess = user_sessions.get(user_id)
        if sess and sess.get("state") == "payment_pending":
            return True
    pp = await run_sync(_get_pending_payment, user_id)
    return bool(pp)


async def _release_reserved_accounts(session):
    if not session:
        return
    reserved = session.get("reserved_accounts") or []
    if not reserved:
        return
    account_type = session.get("account_type")
    if not account_type:
        session["reserved_accounts"] = []
        return
    async with _data_lock:
        pool = accounts_data.setdefault("account_types", {}).setdefault(account_type, [])
        accounts_data["account_types"][account_type] = list(reserved) + list(pool)
        session["reserved_accounts"] = []
    await run_sync(_save_data)
    logger.info(f"Released {len(reserved)} reserved {account_type} account(s) back to pool")


def _release_reserved_accounts_sync(session):
    if not session:
        return
    reserved = session.get("reserved_accounts") or []
    if not reserved:
        return
    account_type = session.get("account_type")
    if not account_type:
        session["reserved_accounts"] = []
        return
    pool = accounts_data.setdefault("account_types", {}).setdefault(account_type, [])
    accounts_data["account_types"][account_type] = list(reserved) + list(pool)
    session["reserved_accounts"] = []
    _save_data()
    logger.info(f"Released {len(reserved)} {account_type} account(s) back (sync)")


async def _reset_user_session(user_id: int, save=True):
    async with _data_lock:
        session = user_sessions.pop(user_id, None)
    target = session if (session and session.get("reserved_accounts")) else None
    if target is None:
        target = await run_sync(_get_pending_payment, user_id)
    if target:
        await _release_reserved_accounts(target)
    asyncio.create_task(run_sync(_delete_pending_payment, user_id))
    if save and session is not None:
        asyncio.create_task(run_sync(_save_sessions))
    return session


async def show_account_selection(chat_id):
    async with _data_lock:
        available = [
            (at, len(accs), accounts_data["prices"].get(at, 0))
            for at, accs in accounts_data["account_types"].items()
            if len(accs) > 0
        ]
    if not available:
        await send_msg(chat_id, "<i>សូមអភ័យទោស អស់ពីស្តុក 🪤</i>",
                       parse_mode=ParseMode.HTML)
        return
    rows = []
    for at, count, price in available:
        label = f"{at} – មានក្នុងស្តុក {count}"
        rows.append([InlineKeyboardButton(label, callback_data=f"buy:{_type_callback_id(at)}")])
    await send_msg(chat_id, "<b>សូមជ្រើសរើសគូប៉ុងដើម្បីទិញ៖</b>",
                   reply_markup=InlineKeyboardMarkup(rows))


async def send_admin_settings_menu(chat_id):
    await send_msg(chat_id,
                   "<b>⚙️ ការកំណត់ Admin</b>\n\nសូមជ្រើសរើសប្រតិបត្តិការខាងក្រោម៖",
                   reply_markup=ADMIN_SETTINGS_KB)


async def _prompt_admin_input(chat_id, user_id, key, prompt_text):
    async with _data_lock:
        user_sessions[user_id] = {"state": f"admin_input:{key}"}
    asyncio.create_task(run_sync(_save_sessions))
    await send_msg(chat_id, prompt_text + "\n\n<i>ចុច 🚫 បោះបង់ ដើម្បីបោះបង់</i>",
                   reply_markup=CANCEL_INPUT_KB)


async def notify_admin_new_user(user_id, first_name, last_name, username):
    if not user_id or user_id == ADMIN_ID:
        return
    if user_id in _notified_users:
        return
    already = await run_sync(_is_admin_notified, user_id)
    if already:
        return
    _notified_users.add(user_id)
    full_name = f"{first_name or ''} {last_name or ''}".strip() or "N/A"
    uname_str = f"@{username}" if username else "—"
    msg = (
        "🆕 អ្នកប្រើប្រាស់ថ្មី!\n\n"
        f"👤 ឈ្មោះ: {html.escape(full_name)}\n"
        f"🔖 Username: {html.escape(uname_str)}\n"
        f"🪪 ID: <code>{user_id}</code>"
    )
    await send_msg(ADMIN_ID, msg)
    asyncio.create_task(run_sync(_upsert_known_user, user_id, first_name, last_name, username))


def _upsert_known_user(user_id, first_name, last_name, username):
    try:
        _neon_query("""
            INSERT INTO bot_known_users (user_id, first_name, last_name, username, first_seen, last_seen, admin_notified)
            VALUES ($1,$2,$3,$4,NOW(),NOW(),TRUE)
            ON CONFLICT (user_id) DO UPDATE SET
                first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
                username=EXCLUDED.username, last_seen=NOW(), admin_notified=TRUE
        """, [str(user_id), first_name or "", last_name or "", username or ""])
    except Exception as e:
        logger.error(f"_upsert_known_user failed: {e}")


async def _notify_must_finish_order(chat_id):
    await send_msg(
        chat_id,
        "⏳ <b>សូមបញ្ចប់ការទិញបច្ចុប្បន្នជាមុនសិន</b>\n\n"
        "អ្នកមានការបញ្ជាទិញមួយកំពុងដំណើរការ។ "
        "សូមបញ្ចប់ការទូទាត់ ឬចុច /cancel មុននឹងចាប់ផ្តើមការទិញថ្មី។")


# ── 14. Payment flow ──────────────────────────────────────────────────────────
async def _start_payment_for_session(chat_id, user_id, session, callback_query=None):
    account_type = session.get("account_type")
    quantity = session.get("quantity", 1)

    async with _data_lock:
        pool = accounts_data.get("account_types", {}).get(account_type, [])
        available = len(pool)
        if available < quantity:
            reserved = None
        else:
            reserved = pool[:quantity]
            accounts_data["account_types"][account_type] = pool[quantity:]
            session["reserved_accounts"] = list(reserved)
            session["available_count"] = len(accounts_data["account_types"][account_type])

    if reserved is None:
        if callback_query:
            try:
                await callback_query.answer(
                    f"សូមអភ័យទោស! មានត្រឹមតែ {available} គូប៉ុង នៅក្នុងស្តុក", show_alert=True)
            except Exception:
                pass
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        return False

    asyncio.create_task(run_sync(_save_data))
    if callback_query:
        try:
            await callback_query.answer("កំពុងបង្កើត QR...")
        except Exception:
            pass
    async with _data_lock:
        session["state"] = "payment_pending"

    img_bytes, md5_or_err, qr_string = await run_sync(_generate_payment_qr, session["total_price"])
    if not img_bytes:
        if is_admin(user_id):
            await send_msg(chat_id, f"❌ *QR បរាជ័យ (Admin Debug):*\n`{md5_or_err}`",
                           parse_mode=ParseMode.MARKDOWN)
        else:
            await send_msg(chat_id, "❌ *មានបញ្ហាក្នុងការបង្កើត QR Code*\n\nសូមព្យាយាមម្តងទៀត។",
                           parse_mode=ParseMode.MARKDOWN)
            await send_msg(ADMIN_ID, f"⚠️ *QR Error (user {user_id}):*\n`{md5_or_err}`",
                           parse_mode=ParseMode.MARKDOWN)
        await _release_reserved_accounts(session)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        return False

    md5_hash = md5_or_err
    session["md5_hash"] = md5_hash
    started_at = time.time()
    session["qr_sent_at"] = started_at

    photo_msg = await send_photo(chat_id, img_bytes, reply_markup=CHECK_PAYMENT_INLINE)
    if photo_msg:
        session["photo_message_id"] = photo_msg.id
        session["qr_message_id"] = photo_msg.id
        asyncio.create_task(_schedule_qr_expiry(chat_id, user_id, photo_msg.id, md5_hash, started_at))

    asyncio.create_task(run_sync(_save_sessions))
    asyncio.create_task(run_sync(_save_pending_payment, user_id, chat_id, session))
    logger.info(f"Generated QR for user {user_id}: Amount ${session['total_price']}, MD5: {md5_hash}")
    return True


async def _schedule_qr_expiry(chat_id, user_id, msg_id, md5_hash, started_at):
    try:
        while True:
            elapsed   = time.time() - started_at
            remaining = PAYMENT_TIMEOUT_SECONDS - elapsed
            sleep_for = min(max(remaining, 0), PAYMENT_POLL_INTERVAL)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

            async with _data_lock:
                sess = user_sessions.get(user_id)
                still_active = bool(
                    sess and sess.get("md5_hash") == md5_hash and sess.get("state") == "payment_pending")
            if not still_active:
                return

            timed_out = time.time() - started_at >= PAYMENT_TIMEOUT_SECONDS

            async with get_user_lock(user_id):
                async with _data_lock:
                    sess_now = user_sessions.get(user_id)
                    still_active = bool(
                        sess_now and sess_now.get("md5_hash") == md5_hash
                        and sess_now.get("state") == "payment_pending")
                if not still_active:
                    return

                is_paid, payment_data = await run_sync(_check_payment_status, md5_hash)

                if is_paid:
                    logger.info(f"Auto-poll detected payment for user {user_id}")
                    async with _data_lock:
                        delivered_session = user_sessions.get(user_id)
                    if delivered_session and delivered_session.get("md5_hash") == md5_hash:
                        await deliver_accounts(chat_id, user_id, delivered_session,
                                               payment_data=payment_data)
                        asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                        asyncio.create_task(run_sync(_save_sessions))
                    return

                if not timed_out:
                    continue

                # Expired
                await delete_msg(chat_id, msg_id)
                async with _data_lock:
                    expired_session = None
                    if (user_id in user_sessions
                            and user_sessions[user_id].get("md5_hash") == md5_hash):
                        expired_session = user_sessions.pop(user_id)
                if expired_session:
                    await _release_reserved_accounts(expired_session)
                else:
                    pp = await run_sync(_get_pending_payment, user_id)
                    if pp:
                        await _release_reserved_accounts(pp)
                asyncio.create_task(run_sync(_save_sessions))
                asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                await send_msg(
                    chat_id,
                    "⌛ <b>QR Code បានផុតកំណត់</b>\n\nសូមបង្កើតការទិញម្តងទៀត។")
                try:
                    await show_account_selection(chat_id)
                except Exception:
                    pass
                return
    except Exception as e:
        logger.error(f"QR expiry task failed for user {user_id}: {e}")


async def deliver_accounts(chat_id, user_id, session, payment_data=None, user_name=""):
    account_type = session["account_type"]
    quantity     = session["quantity"]

    for key in ("photo_message_id", "qr_message_id"):
        mid = session.get(key)
        if mid:
            asyncio.create_task(delete_msg(chat_id, mid))

    reserved = session.get("reserved_accounts") or []
    async with _data_lock:
        if reserved and len(reserved) >= quantity:
            delivered = list(reserved)[:quantity]
            session["reserved_accounts"] = []
            user_sessions.pop(user_id, None)
        elif account_type not in accounts_data["account_types"]:
            delivered = None
        else:
            pool = accounts_data["account_types"][account_type]
            if len(pool) < quantity:
                delivered = None
            else:
                delivered = pool[:quantity]
                accounts_data["account_types"][account_type] = pool[quantity:]
                user_sessions.pop(user_id, None)

    if delivered is None:
        await send_msg(chat_id, f"❌ *មានបញ្ហា!*\n\nគ្មាន គូប៉ុង ប្រភេទ {account_type} ក្នុងស្តុក។",
                       parse_mode=ParseMode.MARKDOWN)
        return

    await run_sync(_save_data)
    await run_sync(_delete_pending_payment, user_id)
    asyncio.create_task(run_sync(_save_purchase_history, user_id, account_type, quantity,
                                 session.get("total_price", 0), delivered))

    msg = (
        f'<tg-emoji emoji-id="5436040291507247633">🎉</tg-emoji> '
        f'<b>ការទិញបានបញ្ជាក់ដោយជោគជ័យ</b>\n\n'
        f"<blockquote>🔹 ប្រភេទ: {account_type}\n🔹 ចំនួន: {quantity}</blockquote>\n\n"
        f"<b>គូប៉ុង របស់អ្នក៖</b>\n\n"
    )
    for acc in delivered:
        if "email" in acc:
            msg += f"{acc['email']}\n"
        else:
            msg += f"{acc.get('phone','')} | {acc.get('password','')}\n"
    msg += f'\n<i>សូមអរគុណសម្រាប់ការទិញ <tg-emoji emoji-id="5897474556834091884">🙏</tg-emoji></i>'

    await send_msg(chat_id, msg, message_effect_id="5046509860389126442",
                   reply_markup=_main_kb(user_id))

    try:
        cambodia_tz = timezone(timedelta(hours=7))
        now_str = datetime.now(cambodia_tz).strftime("%d/%m/%Y %H:%M")
        pd = payment_data or {}
        from_account = pd.get("fromAccountId") or pd.get("hash") or "N/A"
        memo = pd.get("memo") or "គ្មាន"
        ref  = pd.get("externalRef") or pd.get("transactionId") or pd.get("md5") or "N/A"
        amount = session.get("total_price", 0)
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
        await send_msg(ADMIN_ID, admin_msg)
        if CHANNEL_ID and str(CHANNEL_ID) != str(ADMIN_ID):
            await send_msg(CHANNEL_ID, admin_msg)
    except Exception as e:
        logger.error(f"Failed to send admin payment notification: {e}")

    asyncio.create_task(run_sync(_save_sessions))
    logger.info(f"Payment confirmed and {quantity} accounts delivered to user {user_id}")


# ── 15. Admin helper functions ────────────────────────────────────────────────
async def _show_users_list_inline(chat_id):
    try:
        r = await run_sync(
            _neon_query,
            "SELECT user_id,first_name,last_name,username,first_seen FROM bot_known_users ORDER BY first_seen DESC")
        rows = r.get("rows", [])
    except Exception as e:
        rows = []
    if not rows:
        await send_msg(chat_id, "📭 <b>មិនទាន់មានអ្នកប្រើប្រាស់ទេ។</b>",
                       reply_markup=BACK_SETTINGS_KB)
        return
    total = len(rows)
    lines = [f"👥 អ្នកប្រើប្រាស់សរុប: {total}", ""]
    for i, row in enumerate(rows, 1):
        full_name = (f"{row.get('first_name') or ''} {row.get('last_name') or ''}").strip() or "N/A"
        uname = row.get("username") or ""
        lines += [f"{i}. {full_name}", f"   🔖 {'@'+uname if uname else '—'}", f"   🪪 {row.get('user_id')}", ""]
    fname = f"users_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
    await send_document(chat_id, "\n".join(lines).encode("utf-8"), fname,
                        caption=f"👥 បញ្ជីអ្នកប្រើប្រាស់ — {total} នាក់")
    await send_admin_settings_menu(chat_id)


async def _show_delete_type_menu_inline(chat_id, user_id):
    async with _data_lock:
        types = list(accounts_data.get("account_types", {}).keys())
    if not types:
        await send_msg(chat_id, "⚠️ <b>មិនមានប្រភេទ គូប៉ុង ណាមួយទេ!</b>")
        return
    rows_kb, labels_map = [], {}
    for t in types:
        async with _data_lock:
            count = len(accounts_data["account_types"].get(t, []))
        label = f"{_short_label(t)} – មានក្នុងស្តុក {count}"
        rows_kb.append([KeyboardButton(label)])
        labels_map[label] = t
    rows_kb.append([KeyboardButton(BTN_BACK_SETTINGS)])
    async with _data_lock:
        user_sessions[user_id] = {"state": "delete_type_select", "labels": labels_map}
    asyncio.create_task(run_sync(_save_sessions))
    await send_msg(chat_id, "🗑 <b>ជ្រើសរើសប្រភេទ គូប៉ុង ដែលចង់លុប៖</b>",
                   reply_markup=ReplyKeyboardMarkup(rows_kb, resize_keyboard=True, is_persistent=True))


async def _export_buyers_report_inline(chat_id):
    try:
        r = await run_sync(_neon_query, """
            SELECT ph.user_id,ph.account_type,ph.quantity,ph.total_price,
                   ph.accounts,ph.purchased_at,ku.first_name,ku.last_name,ku.username
            FROM bot_purchase_history ph
            LEFT JOIN bot_known_users ku ON ku.user_id=ph.user_id
            ORDER BY ph.user_id,ph.purchased_at DESC
        """)
        rows = r.get("rows", []) or []
        if not rows:
            await send_msg(chat_id, "មិនមានទិន្នន័យ​ទិញ​នៅឡើយ​ទេ។")
            return
        grouped = {}
        for row in rows:
            uid = str(row.get("user_id"))
            grouped.setdefault(uid, {"first_name": row.get("first_name") or "",
                                     "last_name": row.get("last_name") or "",
                                     "username": row.get("username") or "", "purchases": []})
            accs = row.get("accounts") or []
            if isinstance(accs, str):
                try:
                    accs = json.loads(accs)
                except Exception:
                    accs = []
            emails = [str(a.get("email", "")) for a in accs if isinstance(a, dict) and a.get("email")]
            grouped[uid]["purchases"].append({"type": row.get("account_type") or "",
                                              "qty": row.get("quantity") or 0,
                                              "price": row.get("total_price") or 0,
                                              "when": str(row.get("purchased_at") or ""),
                                              "emails": emails})
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        total_emails = 0
        W = 60
        lines = [
            "=" * W,
            "  BUYERS REPORT".center(W),
            f"  {now_str}".center(W),
            "=" * W,
            f"  Total buyers : {len(grouped)}",
        ]
        for uid, info in grouped.items():
            fn = (info["first_name"] + " " + info["last_name"]).strip() or "(no name)"
            un = f"@{info['username']}" if info["username"] else "—"
            lines += [
                "",
                "─" * W,
                f"  ID       : {uid}",
                f"  Name     : {fn}",
                f"  Username : {un}",
                f"  Purchases: {len(info['purchases'])}",
                "─" * W,
            ]
            for i, p in enumerate(info["purchases"], 1):
                when = p["when"][:19] if len(p["when"]) >= 19 else p["when"]
                lines += [
                    f"  [{i}] {p['type']}",
                    f"      Qty   : {p['qty']}",
                    f"      Price : ${p['price']}",
                    f"      Date  : {when}",
                    f"      Emails:",
                ]
                for em in p["emails"]:
                    lines.append(f"        • {em}")
                    total_emails += 1
                if not p["emails"]:
                    lines.append("        (none)")
        lines += [
            "",
            "=" * W,
            f"  Total emails delivered : {total_emails}".ljust(W - 2),
            "=" * W,
        ]
        fname = f"buyers_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        await send_document(chat_id, "\n".join(lines).encode("utf-8"), fname,
                            caption=f"📋 របាយការណ៍ទិញ — {len(grouped)} អ្នក​ទិញ, {total_emails} email")
        await send_admin_settings_menu(chat_id)
    except Exception as e:
        logger.error(f"buyers export failed: {e}")
        await send_msg(chat_id, f"❌ Error: <code>{html.escape(str(e))}</code>")


async def _export_stock_inline(chat_id):
    try:
        async with _data_lock:
            types  = dict(accounts_data.get("account_types", {}))
            prices = dict(accounts_data.get("prices", {}))
            reserved_by_type = {}
            for sess in user_sessions.values():
                if not isinstance(sess, dict) or sess.get("state") != "payment_pending":
                    continue
                t = sess.get("account_type")
                if not t:
                    continue
                for acc in (sess.get("reserved_accounts") or []):
                    if isinstance(acc, dict) and acc.get("email"):
                        reserved_by_type.setdefault(t, []).append(str(acc["email"]))
        type_names = sorted(types)
        if not type_names:
            await send_msg(chat_id, "📦 មិនមានប្រភេទ គូប៉ុង ឡើយទេ។",
                           reply_markup=ADMIN_SETTINGS_KB)
            return
        total_avail, total_res = 0, 0
        for t in type_names:
            total_avail += len(types.get(t) or [])
            total_res   += len(reserved_by_type.get(t, []))
        header = (f"📦 <b>ស្តុក គូប៉ុង</b> — {len(type_names)} ប្រភេទ, {total_avail} នៅសល់"
                  + (f", {total_res} កំពុងកក់ទុក" if total_res else ""))
        await send_msg(chat_id, header)
        for t in type_names:
            pool  = types.get(t) or []
            avail = len(pool)
            res   = reserved_by_type.get(t, [])
            email_lines = []
            for acc in pool:
                if isinstance(acc, dict):
                    em = acc.get("email")
                    if em:
                        email_lines.append(f"• {html.escape(em)}")
                    else:
                        email_lines.append(f"• {html.escape(acc.get('phone',''))} | {html.escape(acc.get('password',''))}")
            if res:
                email_lines.append(f"\n🔒 <i>កំពុងកក់ទុក ({len(res)})</i>")
                for em in res:
                    email_lines.append(f"· {html.escape(em)}")
            block = (f"<b>{html.escape(t)}</b>  💰 ${prices.get(t, 0)}  📦 {avail}\n"
                     + ("\n".join(email_lines) if email_lines else "<i>(គ្មាន)</i>"))
            # Split into chunks if block exceeds Telegram limit
            MAX = 4000
            while len(block) > MAX:
                cut = block.rfind("\n", 0, MAX)
                if cut == -1:
                    cut = MAX
                await send_msg(chat_id, block[:cut])
                block = block[cut:].lstrip("\n")
            if block:
                await send_msg(chat_id, block)
        await send_admin_settings_menu(chat_id)
    except Exception as e:
        logger.error(f"stock export failed: {e}")
        await send_msg(chat_id, f"❌ Error: <code>{html.escape(str(e))}</code>")


async def _show_admins_inline(chat_id):
    extras = sorted(EXTRA_ADMIN_IDS)
    extras_str = "\n".join(f"• <code>{x}</code>" for x in extras) if extras else "(គ្មាន)"
    await send_msg(
        chat_id,
        f"👑 <b>Admin បឋម៖</b> <code>{ADMIN_ID}</code>\n\n"
        f"➕ <b>Admin បន្ថែម៖</b>\n{extras_str}",
        reply_markup=ADMINS_SUBMENU_KB)


async def _show_channel_inline(chat_id):
    current = CHANNEL_ID if CHANNEL_ID else "(មិនទាន់កំណត់)"
    await send_msg(chat_id,
                   f"📢 <b>Channel ID បច្ចុប្បន្ន៖</b>\n<code>{html.escape(str(current))}</code>",
                   reply_markup=CHANNEL_SUBMENU_KB)


async def _show_payment_inline(chat_id):
    await send_msg(chat_id,
                   f"💳 <b>ឈ្មោះ Payment បច្ចុប្បន្ន៖</b>\n<code>{html.escape(PAYMENT_NAME or '(មិនទាន់កំណត់)')}</code>",
                   reply_markup=PAYMENT_SUBMENU_KB)


async def _show_bakong_inline(chat_id):
    api_t = BAKONG_API_TOKEN if BAKONG_API_TOKEN else "(មិនទាន់កំណត់)"
    await send_msg(
        chat_id,
        f"🔑 <b>Bakong Token បច្ចុប្បន្ន៖</b>\n\n"
        f"<code>{html.escape(api_t)}</code>",
        reply_markup=BAKONG_SUBMENU_KB)


def _decode_jwt_expiry(token: str):
    """Decode a JWT token and return (exp_dt, days_left) or (None, None) on failure."""
    import base64
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None, None
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
        exp = payload.get("exp")
        if not exp:
            return None, None
        exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
        days_left = (exp_dt - datetime.now(tz=timezone.utc)).days
        return exp_dt, days_left
    except Exception:
        return None, None


def _days_status(days_left) -> str:
    """Return a status string based on days remaining."""
    if days_left is None:
        return "✅ Active"
    if days_left < 0:
        return f"❌ ផុតកំណត់រួចហើយ ({abs(days_left)} ថ្ងៃមុន)"
    if days_left == 0:
        return "⚠️ ផុតកំណត់ថ្ងៃនេះ!"
    if days_left <= 7:
        return f"⚠️ នឹងផុតក្នុង {days_left} ថ្ងៃ"
    return f"✅ នៅសល់ {days_left} ថ្ងៃ"


async def _send_combined_token_info(chat_id: int, reply_markup) -> None:
    """Build and send the combined Bakong + Dropmail token info message."""

    lines = ["🔑 <b>Token Info</b>\n"]

    # ── Bakong API Token ──────────────────────────────────────────────────────
    lines.append("━━━ 🏦 Bakong ━━━")
    token = BAKONG_API_TOKEN
    if not token:
        lines.append("❌ មិនទាន់មាន Bakong Token ទេ។")
    else:
        exp_dt, days_left = _decode_jwt_expiry(token)
        lines.append(f"Token: <code>{html.escape(token[:10])}…</code>")
        if exp_dt:
            lines.append(f"📅 Expire: <b>{exp_dt.strftime('%Y-%m-%d %H:%M UTC')}</b>")
            lines.append(f"⏳ ស្ថានភាព: {_days_status(days_left)}")
        else:
            lines.append("📅 Expire: <b>មិនអាចបំបែក JWT បាន</b>")

    # ── Dropmail API Token ────────────────────────────────────────────────────
    lines.append("\n━━━ 📧 Dropmail ━━━")
    if not DROPMAIL_API_TOKEN:
        lines.append("❌ មិនទាន់មាន Dropmail Token ទេ។")
    else:
        dm_masked = DROPMAIL_API_TOKEN[:6] + "…" + DROPMAIL_API_TOKEN[-4:]
        lines.append(f"Token: <code>{html.escape(dm_masked)}</code>")
        if DROPMAIL_TOKEN_EXPIRY:
            try:
                exp_dt2 = datetime.strptime(DROPMAIL_TOKEN_EXPIRY, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                dm_days = (exp_dt2 - datetime.now(tz=timezone.utc)).days
                lines.append(f"📅 Expire: <b>{DROPMAIL_TOKEN_EXPIRY}</b>")
                lines.append(f"⏳ ស្ថានភាព: {_days_status(dm_days)}")
            except Exception:
                lines.append(f"📅 Expire: <b>{html.escape(DROPMAIL_TOKEN_EXPIRY)}</b>")
                lines.append("⏳ ស្ថានភាព: ✅ Active")
        else:
            lines.append("📅 Expire: <b>មិន​ទាន់​កំណត់</b> — ចុច ✏️ ប្តូរ Token ដើម្បីកំណត់")

    await send_msg(chat_id, "\n".join(lines), reply_markup=reply_markup)


async def _bakong_show_token_info(chat_id: int):
    await _send_combined_token_info(chat_id, BAKONG_SUBMENU_KB)


async def _show_maintenance_inline(chat_id):
    status = "🔴 បិទ" if MAINTENANCE_MODE else "🟢 បើក"
    await send_msg(chat_id, f"🛠 <b>ស្ថានភាព Bot បច្ចុប្បន្ន៖</b> {status}",
                   reply_markup=MAINTENANCE_SUBMENU_KB)


async def _dispatch_admin_button(client, message, user_id, chat_id, btn):
    global MAINTENANCE_MODE, CHANNEL_ID
    tok = _current_client.set(client)
    try:
        if btn == BTN_BACK_SETTINGS:
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            await send_admin_settings_menu(chat_id)
        elif btn == BTN_ADD_ACCOUNT:
            async with _data_lock:
                user_sessions[user_id] = {"state": "waiting_for_accounts"}
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(
                chat_id,
                "<b>បញ្ចូលគូប៉ុងសម្រាប់លក់</b>",
                reply_markup=ADD_ACCOUNT_KB)
        elif btn == BTN_DELETE_TYPE:
            await _show_delete_type_menu_inline(chat_id, user_id)
        elif btn == BTN_STOCK:
            await _export_stock_inline(chat_id)
        elif btn == BTN_USERS:
            await _show_users_list_inline(chat_id)
        elif btn == BTN_BUYERS:
            await _export_buyers_report_inline(chat_id)
        elif btn == BTN_PAYMENT:
            await _show_payment_inline(chat_id)
        elif btn == BTN_BAKONG:
            await _show_bakong_inline(chat_id)
        elif btn == BTN_BAKONG_TOKEN_INFO:
            await _bakong_show_token_info(chat_id)
        elif btn == BTN_CHANNEL:
            await _show_channel_inline(chat_id)
        elif btn == BTN_ADMINS:
            await _show_admins_inline(chat_id)
        elif btn == BTN_MAINTENANCE:
            await _show_maintenance_inline(chat_id)
        elif btn == BTN_BROADCAST:
            await _prompt_admin_input(
                chat_id, user_id, "broadcast",
                "📢 សូមផ្ញើ​សារ​ដែល​ចង់​ផ្សាយ​ទៅ​អ្នក​ប្រើ​ប្រាស់​ទាំង​អស់៖")
        elif btn == BTN_PAYMENT_EDIT:
            await _prompt_admin_input(chat_id, user_id, "payment",
                                      "💳 សូមផ្ញើ <b>ឈ្មោះ Payment</b> ថ្មី:")
        elif btn == BTN_BAKONG_API_EDIT:
            await _prompt_admin_input(chat_id, user_id, "bakong_api",
                                      "🔑 សូមផ្ញើ <b>Bakong Token</b> ថ្មី:")
        elif btn == BTN_CHANNEL_EDIT:
            await _prompt_admin_input(chat_id, user_id, "channel",
                                      "📢 សូមផ្ញើ <b>Channel ID</b> ថ្មី (ឧ. <code>-1001234567890</code>):")
        elif btn == BTN_CHANNEL_CLEAR:
            CHANNEL_ID = ""
            await run_sync(_set_setting, "TELEGRAM_CHANNEL_ID", "")
            await send_msg(chat_id, "✅ បានលុប Channel ID", reply_markup=ADMIN_SETTINGS_KB)
        elif btn == BTN_ADMIN_ADD:
            await _prompt_admin_input(chat_id, user_id, "admin_add",
                                      "➕ សូមផ្ញើ <b>Telegram User ID</b> ដែលចង់បន្ថែម:")
        elif btn == BTN_ADMIN_REMOVE:
            await _prompt_admin_input(chat_id, user_id, "admin_remove",
                                      "➖ សូមផ្ញើ <b>Telegram User ID</b> ដែលចង់ដក:")
        elif btn == BTN_MAINT_ON:
            MAINTENANCE_MODE = True
            await run_sync(_set_setting, "MAINTENANCE_MODE", "true")
            await send_msg(chat_id, "🔴 បានបិទ Bot", reply_markup=ADMIN_SETTINGS_KB)
        elif btn == BTN_MAINT_OFF:
            MAINTENANCE_MODE = False
            await run_sync(_set_setting, "MAINTENANCE_MODE", "false")
            await send_msg(chat_id, "🟢 បានបើក Bot", reply_markup=ADMIN_SETTINGS_KB)
        elif btn == BTN_EMAIL_MGMT:
            if not DROPMAIL_API_TOKEN:
                await send_msg(chat_id,
                    "⚠️ <b>DROPMAIL_API_TOKEN</b> មិនទាន់កំណត់។\n\n"
                    "ចុច <b>✏️ ប្តូរ Dropmail Token</b> ដើម្បីកំណត់ token ។",
                    reply_markup=EMAIL_SUBMENU_KB)
            else:
                await send_msg(chat_id,
                    "📧 <b>ការគ្រប់គ្រងអ៊ីម៉ែល</b>\n\nជ្រើសរើសប្រតិបត្តិការ៖",
                    reply_markup=EMAIL_SUBMENU_KB)
        elif btn == BTN_EMAIL_NEW:
            await _email_handle_new(chat_id, user_id)
        elif btn == BTN_EMAIL_LIST:
            await _email_handle_list(chat_id, user_id)
        elif btn == BTN_EMAIL_DELETE:
            await _email_handle_delete_picker(chat_id, user_id)
        elif btn == BTN_EMAIL_TOKEN_EDIT:
            await _prompt_admin_input(
                chat_id, user_id, "dropmail_token",
                "🔑 សូមផ្ញើ <b>Dropmail API Token</b> ថ្មី:\n\n"
                "<i>⚠️ Token នឹងត្រូវបានលុបចោលស្វ័យប្រវត្តិ — ផ្ញើដោយប្រុងប្រយ័ត្ន!</i>")
        elif btn == BTN_EMAIL_TOKEN_INFO:
            await _email_show_token_info(chat_id)
    finally:
        _current_client.reset(tok)


async def _handle_admin_settings_input(chat_id, user_id, message_id, key, text):
    global PAYMENT_NAME, BAKONG_TOKEN, BAKONG_RELAY_TOKEN, BAKONG_API_TOKEN, khqr_client, CHANNEL_ID, EXTRA_ADMIN_IDS, DROPMAIL_API_TOKEN, DROPMAIL_TOKEN_EXPIRY, _DROPMAIL_URL
    raw = (text or "").strip()
    cancel_words = {"បោះបង់", "🚫 បោះបង់"}
    if raw in cancel_words or raw == BTN_BACK_SETTINGS:
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_admin_settings_menu(chat_id)
        return True

    if key == "payment":
        if not raw:
            await send_msg(chat_id, "សូមផ្ញើឈ្មោះ Payment ថ្មី (ឬចុច 🚫 បោះបង់)")
            return True
        PAYMENT_NAME = raw
        await run_sync(_set_setting, "PAYMENT_NAME", PAYMENT_NAME)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(chat_id,
                       f"✅ បានប្តូរឈ្មោះ Payment ទៅជា <b>{html.escape(PAYMENT_NAME)}</b>",
                       reply_markup=_main_kb(user_id))
        return True

    if key in ("bakong", "bakong_api"):
        if not raw:
            await send_msg(chat_id, "សូមផ្ញើ Bakong token ថ្មី (ឬចុច 🚫 បោះបង់)")
            return True
        try:
            KHQR(raw)
        except Exception as e:
            await send_msg(chat_id, f"❌ Token មិនត្រឹមត្រូវ៖ <code>{html.escape(str(e))}</code>")
            return True
        BAKONG_API_TOKEN = raw
        await run_sync(_set_setting, "BAKONG_API_TOKEN", raw)
        label = "Bakong Token"
        BAKONG_TOKEN = BAKONG_API_TOKEN
        try:
            khqr_client = KHQR(BAKONG_TOKEN)
        except Exception:
            pass
        asyncio.create_task(delete_msg(chat_id, message_id))
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(chat_id,
                       f"✅ បានប្តូរ <b>{label}</b> (Prefix: <code>{html.escape(raw[:10])}…</code>)",
                       reply_markup=_main_kb(user_id))
        return True

    if key == "channel":
        if not raw:
            await send_msg(chat_id, "សូមផ្ញើ Channel ID ថ្មី ឬ <code>off</code> ដើម្បីបិទ")
            return True
        if raw.lower() in ("off", "none", "clear", "delete", "remove"):
            CHANNEL_ID = ""
            await run_sync(_set_setting, "TELEGRAM_CHANNEL_ID", "")
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(chat_id, "✅ បានលុប Channel ID", reply_markup=_main_kb(user_id))
            return True
        CHANNEL_ID = raw
        await run_sync(_set_setting, "TELEGRAM_CHANNEL_ID", raw)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(chat_id,
                       f"✅ បានកំណត់ Channel ID ទៅជា <code>{html.escape(raw)}</code>",
                       reply_markup=_main_kb(user_id))
        return True

    if key in ("admin_add", "admin_remove"):
        action = "add" if key == "admin_add" else "remove"
        try:
            target_id = int(raw)
        except ValueError:
            await send_msg(chat_id, "❌ user_id ត្រូវតែជាលេខ (ឬចុច 🚫 បោះបង់)")
            return True
        if target_id == ADMIN_ID:
            await send_msg(chat_id, "ℹ️ Admin បឋមមិនអាចលុប/បន្ថែមបានទេ។",
                           reply_markup=_main_kb(user_id))
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            return True
        if action == "add":
            EXTRA_ADMIN_IDS.add(target_id)
            msg = f"✅ បានបន្ថែម <code>{target_id}</code> ជា admin"
        else:
            EXTRA_ADMIN_IDS.discard(target_id)
            msg = f"✅ បានដក <code>{target_id}</code> ចេញពី admin"
        await run_sync(_set_setting, "EXTRA_ADMIN_IDS", json.dumps(sorted(EXTRA_ADMIN_IDS)))
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(chat_id, msg, reply_markup=_main_kb(user_id))
        return True

    if key == "dropmail_token":
        if not raw:
            await send_msg(chat_id, "🔑 សូមផ្ញើ <b>Dropmail API Token</b> ថ្មី (ឬចុច 🚫 បោះបង់)")
            return True
        DROPMAIL_API_TOKEN = raw
        _DROPMAIL_URL = f"https://dropmail.me/api/graphql/{raw}"
        await run_sync(_set_setting, "DROPMAIL_API_TOKEN", raw)
        asyncio.create_task(delete_msg(chat_id, message_id))
        async with _data_lock:
            user_sessions[user_id] = {"state": "admin_input:dropmail_expiry"}
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(
            chat_id,
            f"✅ បានប្តូរ <b>Dropmail API Token</b>\n"
            f"Prefix: <code>{html.escape(raw[:8])}…</code>\n\n"
            f"📅 សូមផ្ញើ <b>ថ្ងៃផុតកំណត់</b> (YYYY-MM-DD)\n"
            f"ឧ. <code>2026-12-31</code>\n"
            f"ឬចុច <b>🚫 បោះបង់</b> ដើម្បីរំលង",
            reply_markup=CANCEL_INPUT_KB)
        return True

    if key == "dropmail_expiry":
        if not raw:
            await send_msg(chat_id, "📅 សូមផ្ញើ​ថ្ងៃ​ផុត​កំណត់ (YYYY-MM-DD) ឬចុច 🚫 បោះបង់")
            return True
        try:
            datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            await send_msg(chat_id, "❌ ទម្រង់ថ្ងៃ​មិន​ត្រឹម​ត្រូវ។ សូម​ប្រើ​ទម្រង់ <code>YYYY-MM-DD</code> (ឧ. <code>2026-12-31</code>)")
            return True
        DROPMAIL_TOKEN_EXPIRY = raw
        await run_sync(_set_setting, "DROPMAIL_TOKEN_EXPIRY", raw)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(
            chat_id,
            f"✅ បានកំណត់ <b>Dropmail Token Expire</b>: <code>{html.escape(raw)}</code>",
            reply_markup=EMAIL_SUBMENU_KB)
        return True

    if key == "broadcast":
        if not message_id:
            await send_msg(chat_id, "សូមផ្ញើ​សារ​ដែល​ចង់​ផ្សាយ (ឬចុច 🚫 បោះបង់)")
            return True
        is_text_only = bool(raw)
        async with _data_lock:
            user_sessions[user_id] = {
                "state": "broadcast_confirm",
                "broadcast_message_id": message_id,
                "broadcast_chat_id": chat_id,
                "broadcast_use_copy": is_text_only,
            }
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(
            chat_id,
            "❓ <b>តើ​អ្នក​ប្រាកដ​ជា​ចង់​ផ្សាយ​សារ​ខាង​លើ​នេះ​ទៅ​អ្នក​ប្រើ​ប្រាស់​ទាំង​អស់​មែន​ទេ?</b>\n\n"
            "ចុច <b>✅ បញ្ជាក់ផ្សាយ</b> ឬ <b>🚫 បោះបង់ការផ្សាយ</b>",
            reply_markup=BROADCAST_CONFIRM_KB)
        return True

    return False


async def _run_broadcast(admin_chat_id, source_message_id, use_copy=False):
    try:
        r = await run_sync(_neon_query, "SELECT user_id FROM bot_known_users")
        rows = r.get("rows", []) or []
        total, sent, failed, blocked = len(rows), 0, 0, 0
        for row in rows:
            uid = row.get("user_id")
            if not uid:
                continue
            try:
                if use_copy:
                    result = await copy_msg(uid, admin_chat_id, source_message_id)
                else:
                    result = await forward_msg(uid, admin_chat_id, source_message_id)
                if result:
                    sent += 1
                else:
                    failed += 1
            except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
                blocked += 1
            except Exception as e:
                failed += 1
                logger.warning(f"Broadcast to {uid} error: {e}")
            await asyncio.sleep(0.05)
        summary = (
            "📢 <b>ផ្សាយ​សារ​បាន​ចប់</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👥 សរុប:         {total}\n"
            f"✅ ផ្ញើ​ជោគជ័យ:   {sent}\n"
            f"⛔ បាន​ប្លុក/លុប:  {blocked}\n"
            f"❌ បរាជ័យ:        {failed}"
        )
        await send_msg(admin_chat_id, summary, reply_markup=ADMIN_SETTINGS_KB)
    except Exception as e:
        logger.error(f"Broadcast crashed: {e}")
        await send_msg(admin_chat_id, f"❌ Broadcast error: <code>{html.escape(str(e))}</code>")


# ── 16. Channel post handler ──────────────────────────────────────────────────
def _parse_verification_message(text):
    email_match = re.search(r"[\w.+%-]+@[\w.-]+\.[A-Za-z]{2,}", text or "")
    code_match  = re.search(r"(?<!\d)\d{4,8}(?!\d)", text or "")
    if not email_match or not code_match:
        return None, None
    return email_match.group(0).strip().lower(), code_match.group(0)


async def handle_channel_post(message):
    chat_id    = message.chat.id
    message_id = message.id
    if not CHANNEL_ID or str(chat_id) != str(CHANNEL_ID):
        return
    text = message.text or message.caption or ""
    email, code = _parse_verification_message(text)
    if email and code:
        buyers = await run_sync(_find_all_buyers_by_email, email)
        formatted = (
            "📩 <b>លេខកូដផ្ទៀងផ្ទាត់ E-GetS</b>\n\n"
            f"{html.escape(email)}\n\n<code>{html.escape(code)}</code>")
        delivered_to = []
        for bid in buyers:
            sent = await send_msg(bid, formatted, reply_markup=False)
            if sent:
                await delete_msg_later(bid, sent.id, 60)
                delivered_to.append(bid)
        if not delivered_to:
            sent = await send_msg(ADMIN_ID, formatted)
            if sent:
                await delete_msg_later(ADMIN_ID, sent.id, 60)
        return
    copied = await copy_msg(ADMIN_ID, chat_id, message_id)
    if copied:
        return
    if text:
        await send_msg(ADMIN_ID, text)


# ── 17. Custom Pyrogram filters ───────────────────────────────────────────────
def _make_admin_filter():
    async def func(_, __, message):
        uid = message.from_user.id if message.from_user else None
        return bool(uid and is_admin(uid))
    return filters.create(func, "AdminFilter")


def _make_maintenance_block_filter():
    """Passes (returns True) when maintenance is ON and user is NOT admin."""
    async def func(_, __, message):
        if not MAINTENANCE_MODE:
            return False
        uid = message.from_user.id if message.from_user else None
        return not is_admin(uid)
    return filters.create(func, "MaintenanceBlockFilter")


def _make_has_admin_input_session_filter():
    async def func(_, __, message):
        uid = message.from_user.id if message.from_user else None
        if not uid or not is_admin(uid):
            return False
        sess = user_sessions.get(uid)
        return bool(sess and str(sess.get("state", "")).startswith("admin_input:"))
    return filters.create(func, "HasAdminInputSessionFilter")


def _make_has_admin_state_filter(state_name):
    async def func(_, __, message):
        uid = message.from_user.id if message.from_user else None
        if not uid or not is_admin(uid):
            return False
        sess = user_sessions.get(uid)
        return bool(sess and sess.get("state") == state_name)
    return filters.create(func, f"AdminState_{state_name}")


def _make_admin_button_filter():
    async def func(_, __, message):
        uid = message.from_user.id if message.from_user else None
        if not uid or not is_admin(uid):
            return False
        return bool(message.text and message.text.strip() in ADMIN_BUTTON_LABELS)
    return filters.create(func, "AdminButtonFilter")


def _make_payment_pending_filter():
    async def func(_, __, message):
        uid = message.from_user.id if message.from_user else None
        if not uid:
            return False
        sess = user_sessions.get(uid)
        return bool(sess and sess.get("state") == "payment_pending")
    return filters.create(func, "PaymentPendingFilter")


admin_filter              = _make_admin_filter()
maintenance_block_filter  = _make_maintenance_block_filter()
has_admin_input_filter    = _make_has_admin_input_session_filter()
admin_button_filter       = _make_admin_button_filter()
payment_pending_filter    = _make_payment_pending_filter()
delete_type_select_filter  = _make_has_admin_state_filter("delete_type_select")
delete_type_confirm_filter = _make_has_admin_state_filter("delete_type_confirm")
broadcast_confirm_filter   = _make_has_admin_state_filter("broadcast_confirm")
email_delete_picker_filter = _make_has_admin_state_filter("email_delete_picker")


# ── 18. Handlers — Priority via group parameter (lower = higher priority) ─────

# ─── group -10: Channel posts ──────────────────────────────────────────────────
@app.on_message(filters.channel, group=-10)
async def on_channel_post(client, message):
    await handle_channel_post(message)
    message.stop_propagation()


# ─── group -5: Maintenance mode blocker ───────────────────────────────────────
@app.on_message(filters.private & maintenance_block_filter, group=-5)
async def on_maintenance(client, message):
    await send_msg(message.chat.id,
                   "🔧 <b>Bot កំពុង Update សូមរង់ចាំមួយភ្លែត...</b>")
    message.stop_propagation()


# ─── group 0: /start and /cancel commands ─────────────────────────────────────
@app.on_message(filters.private & filters.command("start"), group=0)
async def on_start(client, message):
    user = message.from_user
    asyncio.create_task(
        notify_admin_new_user(user.id, user.first_name, user.last_name, user.username))
    async with get_user_lock(user.id):
        if await _has_active_purchase(user.id):
            await _notify_must_finish_order(message.chat.id)
            message.stop_propagation()
            return
        await _reset_user_session(user.id)
        logger.info(f"User {user.id} triggered account selection")
        await show_account_selection(message.chat.id)
    message.stop_propagation()


@app.on_message(filters.private & filters.command("cancel"), group=0)
async def on_cancel(client, message):
    user_id  = message.from_user.id
    chat_id  = message.chat.id
    async with get_user_lock(user_id):
        session = user_sessions.get(user_id) or await run_sync(_get_pending_payment, user_id)
        if not session or session.get("state") not in ("waiting_for_quantity", "payment_pending"):
            await show_account_selection(chat_id)
            message.stop_propagation()
            return
        for key in ("photo_message_id", "qr_message_id", "dot_message_id"):
            mid = session.get(key)
            if mid:
                asyncio.create_task(delete_msg(chat_id, mid))
        await _reset_user_session(user_id)
        await show_account_selection(chat_id)
    message.stop_propagation()


# ─── group 1: Admin ⚙️ button ─────────────────────────────────────────────────
@app.on_message(
    filters.private & admin_filter
    & filters.text & filters.regex(f"^{re.escape(ADMIN_SETTINGS_BTN)}$"),
    group=1)
async def on_admin_settings_btn(client, message):
    user_id = message.from_user.id
    async with _data_lock:
        sess = user_sessions.get(user_id, {})
        if str(sess.get("state", "")).startswith("admin_input:"):
            user_sessions.pop(user_id, None)
    asyncio.create_task(run_sync(_save_sessions))
    await send_admin_settings_menu(message.chat.id)
    message.stop_propagation()


# ─── group 2: Admin pending input (payment, bakong, channel, admin, broadcast) ─
@app.on_message(filters.private & has_admin_input_filter, group=2)
async def on_admin_input(client, message):
    user_id    = message.from_user.id
    chat_id    = message.chat.id
    message_id = message.id
    text       = message.text or ""
    async with get_user_lock(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        state = str(sess.get("state", ""))
        if state.startswith("admin_input:"):
            key = state.split(":", 1)[1]
            if await _handle_admin_settings_input(chat_id, user_id, message_id, key, text):
                message.stop_propagation()


# ─── group 3: Admin delete_type_select state ──────────────────────────────────
@app.on_message(filters.private & delete_type_select_filter, group=3)
async def on_delete_type_select(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text    = (message.text or "").strip()
    async with get_user_lock(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        labels = sess.get("labels", {}) or {}
        if text == BTN_BACK_SETTINGS:
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            await send_admin_settings_menu(chat_id)
            message.stop_propagation()
            return
        type_name = labels.get(text)
        if type_name and type_name in accounts_data.get("account_types", {}):
            async with _data_lock:
                count = len(accounts_data["account_types"].get(type_name, []))
                price = accounts_data.get("prices", {}).get(type_name, 0)
                user_sessions[user_id] = {"state": "delete_type_confirm", "type_name": type_name}
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(
                chat_id,
                f"⚠️ <b>តើអ្នកពិតជាចង់លុបប្រភេទ គូប៉ុង នេះមែនទេ?</b>\n\n"
                f"<blockquote>🔹 ប្រភេទ: {html.escape(type_name)}\n"
                f"🔹 ចំនួន: {count}\n🔹 តម្លៃ: ${price}</blockquote>",
                reply_markup=ReplyKeyboardMarkup([
                    [KeyboardButton(BTN_DELETE_CONFIRM)],
                    [KeyboardButton(BTN_DELETE_CANCEL)],
                ], resize_keyboard=True, is_persistent=True))
            message.stop_propagation()


# ─── group 3: Admin delete_type_confirm state ─────────────────────────────────
@app.on_message(filters.private & delete_type_confirm_filter, group=3)
async def on_delete_type_confirm(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text    = (message.text or "").strip()
    async with get_user_lock(user_id):
        async with _data_lock:
            type_name = user_sessions.get(user_id, {}).get("type_name")
        if text == BTN_DELETE_CONFIRM:
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            if not type_name or type_name not in accounts_data.get("account_types", {}):
                await send_msg(chat_id, "⚠️ <b>ប្រភេទនេះមិនមានទៀតហើយ!</b>",
                               reply_markup=ADMIN_SETTINGS_KB)
                message.stop_propagation()
                return
            async with _data_lock:
                count = len(accounts_data["account_types"].pop(type_name, []))
                accounts_data.get("prices", {}).pop(type_name, None)
                accounts_data["accounts"] = [
                    a for a in accounts_data.get("accounts", []) if a.get("type") != type_name]
            asyncio.create_task(run_sync(_save_data))
            await send_msg(chat_id,
                           f"✅ <b>បានលុបប្រភេទ <code>{html.escape(type_name)}</code> ចំនួន {count} records!</b>",
                           reply_markup=ADMIN_SETTINGS_KB)
            logger.info(f"Admin {user_id} deleted type '{type_name}' ({count} records)")
            message.stop_propagation()
        elif text == BTN_DELETE_CANCEL:
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(chat_id, "🚫 <b>បានបោះបង់ការលុប</b>", reply_markup=ADMIN_SETTINGS_KB)
            message.stop_propagation()


# ─── group 3: Admin broadcast_confirm state ───────────────────────────────────
@app.on_message(filters.private & broadcast_confirm_filter, group=3)
async def on_broadcast_confirm(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text    = (message.text or "").strip()
    async with get_user_lock(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        if text == BTN_BROADCAST_CONFIRM:
            bcast_msg_id  = sess.get("broadcast_message_id")
            bcast_chat_id = sess.get("broadcast_chat_id") or chat_id
            use_copy      = bool(sess.get("broadcast_use_copy"))
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            if not bcast_msg_id:
                await send_msg(chat_id, "⚠️ មិន​ឃើញ​សារ​ដែល​ចង់​ផ្សាយ​ទេ",
                               reply_markup=ADMIN_SETTINGS_KB)
                message.stop_propagation()
                return
            await send_msg(chat_id, "📢 កំពុង​ផ្សាយ​សារ ... សូមរង់ចាំ",
                           reply_markup=ADMIN_SETTINGS_KB)
            asyncio.create_task(_run_broadcast(bcast_chat_id, bcast_msg_id, use_copy))
            message.stop_propagation()
        elif text == BTN_BROADCAST_CANCEL:
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(chat_id, "🚫 <b>បាន​បោះបង់​ការ​ផ្សាយ</b>", reply_markup=ADMIN_SETTINGS_KB)
            message.stop_propagation()


# ─── group 3: Admin email_delete_picker state ─────────────────────────────────
@app.on_message(filters.private & email_delete_picker_filter, group=3)
async def on_email_delete_picker(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text    = (message.text or "").strip()
    async with get_user_lock(user_id):
        if text == BTN_BACK_SETTINGS:
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(chat_id, "📧 <b>ការគ្រប់គ្រងអ៊ីម៉ែល</b>\n\nជ្រើសរើសប្រតិបត្តិការ៖",
                           reply_markup=EMAIL_SUBMENU_KB)
            message.stop_propagation()
            return
        # Try to match tapped email address
        entry = await run_sync(_email_history_get_by_email, user_id, text)
        if not entry:
            await send_msg(chat_id, "❌ មិនឃើញអ៊ីម៉ែលនេះទេ។", reply_markup=EMAIL_SUBMENU_KB)
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            message.stop_propagation()
            return
        address_id = entry.get("address_id", "")
        entry_id   = entry.get("id")
        if address_id:
            await run_sync(_dropmail_delete_address, address_id)
        if entry_id:
            await run_sync(_email_history_delete, entry_id)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(chat_id,
                       f"✅ <b>លុបអ៊ីម៉ែលបានសម្រេច។</b>\n<code>{html.escape(text)}</code>",
                       reply_markup=EMAIL_SUBMENU_KB)
    message.stop_propagation()


# ─── Email sub-menu helpers ───────────────────────────────────────────────────
async def _email_handle_new(chat_id: int, user_id: int):
    if not DROPMAIL_API_TOKEN:
        await send_msg(chat_id, "❌ DROPMAIL_API_TOKEN មិនទាន់កំណត់។", reply_markup=EMAIL_SUBMENU_KB)
        return

    # ── Check token expiry before creating ──────────────────────────────────
    info = await run_sync(_dropmail_check_token_info)
    expires_val = info.get("expires") or "N/A"
    remaining_val = info.get("remaining")

    # Build a token-status footer line
    if not info.get("valid"):
        err = info.get("error", "")
        err_line = f"\n⚠️ <code>{html.escape(err[:80])}</code>" if err else ""
        await send_msg(
            chat_id,
            f"❌ <b>Dropmail Token មិនត្រឹមត្រូវ ឬផុតកំណត់!</b>\n"
            f"Token: <code>{DROPMAIL_API_TOKEN[:6]}…{DROPMAIL_API_TOKEN[-4:]}</code>"
            f"{err_line}\n\n"
            f"ចុច <b>✏️ ប្តូរ Dropmail Token</b> ដើម្បីធ្វើបច្ចុប្បន្នភាព។",
            reply_markup=EMAIL_SUBMENU_KB)
        return

    # Parse expiry string to compute days left (format varies: ISO or "N/A")
    days_left = None
    exp_display = expires_val
    if expires_val and expires_val != "N/A":
        try:
            from datetime import datetime, timezone
            # Try ISO 8601 parse
            exp_dt = datetime.fromisoformat(expires_val.replace("Z", "+00:00"))
            days_left = (exp_dt - datetime.now(tz=timezone.utc)).days
            exp_display = exp_dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    if days_left is not None and days_left < 0:
        await send_msg(
            chat_id,
            f"❌ <b>Dropmail Token ផុតកំណត់រួចហើយ!</b>\n"
            f"📅 Expire: <b>{exp_display}</b> ({abs(days_left)} ថ្ងៃមុន)\n\n"
            f"ចុច <b>✏️ ប្តូរ Dropmail Token</b> ដើម្បីធ្វើបច្ចុប្បន្នភាព។",
            reply_markup=EMAIL_SUBMENU_KB)
        return

    if days_left is not None and days_left <= 7:
        token_status = f"⚠️ Token នឹងផុតក្នុង <b>{days_left} ថ្ងៃ</b> ({exp_display}) — សូមធ្វើបច្ចុប្បន្នភាព!"
    elif days_left is not None:
        rem_str = f" | 📊 Requests: {remaining_val}" if remaining_val is not None else ""
        token_status = f"✅ Token ត្រឹមត្រូវ — នៅសល់ <b>{days_left} ថ្ងៃ</b> ({exp_display}){rem_str}"
    else:
        rem_str = f" | 📊 Requests: {remaining_val}" if remaining_val is not None else ""
        token_status = f"✅ Token ត្រឹមត្រូវ{rem_str}"
    # ────────────────────────────────────────────────────────────────────────

    try:
        result = await run_sync(_dropmail_create_session)
    except Exception as e:
        await send_msg(chat_id, f"❌ បង្កើតមិនបានទេ: <code>{html.escape(str(e))}</code>",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    if not result or not result.get("email"):
        await send_msg(chat_id, "❌ មិនអាចបង្កើត session បានទេ។ សូមព្យាយាមម្ដងទៀត។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    await run_sync(_email_history_add, user_id, result["email"],
                   result.get("session_id", ""), result.get("address_id", ""),
                   result.get("restore_key", ""))
    await send_msg(chat_id,
                   f"✅ <b>អ៊ីម៉ែលថ្មីបានបង្កើត!</b>\n\n"
                   f"📧 <code>{result['email']}</code>\n\n"
                   f"👆 ចុចលើអ៊ីម៉ែលដើម្បីចម្លង។ Bot នឹងជូនដំណឹងភ្លាមៗពីសំបុត្រថ្មី។\n\n"
                   f"🔑 {token_status}",
                   reply_markup=EMAIL_SUBMENU_KB)


async def _email_handle_inbox(chat_id: int, user_id: int):
    entries = await run_sync(_email_history_entries, user_id)
    if not entries:
        await send_msg(chat_id,
                       "📭 មិនទាន់មានអ៊ីម៉ែលទេ។ ចុច <b>✉️ អ៊ីម៉ែលថ្មី</b> ដើម្បីបង្កើត។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    entry = entries[0]
    session_id = entry.get("dropmail_session_id")
    if not session_id:
        await send_msg(chat_id, "❌ Session ID ត្រូវបានបាត់។ សូមបង្កើតអ៊ីម៉ែលថ្មី។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    await send_msg(chat_id, "⏳ កំពុងពិនិត្យប្រអប់…", reply_markup=EMAIL_SUBMENU_KB)
    try:
        mails = await run_sync(_dropmail_get_mails, session_id, None)
    except Exception as e:
        await send_msg(chat_id, f"❌ កំហុសក្នុងការពិនិត្យ: <code>{html.escape(str(e))}</code>",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    email_addr = entry.get("email_address", "?")
    if mails is None:
        await send_msg(chat_id,
                       f"⚠️ Session ផុតកំណត់។\n📧 <code>{email_addr}</code>\n\n"
                       f"Bot នឹងស្តារវិញដោយស្វ័យប្រវត្តិ។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    if not mails:
        await send_msg(chat_id,
                       f"📭 <b>ប្រអប់ទទេ</b>\n\n📧 <code>{email_addr}</code>\n\n"
                       f"មិនទាន់មានអ៊ីម៉ែលចូលទេ។ Bot នឹងជូនដំណឹងភ្លាមៗ។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    text = f"📬 <b>ប្រអប់ — {len(mails)} សំបុត្រ</b>\n📧 <code>{email_addr}</code>\n\n"
    for i, mail in enumerate(mails[-5:], 1):
        subject   = mail.get("headerSubject") or "(គ្មានប្រធានបទ)"
        from_addr = mail.get("fromAddr") or "unknown"
        body      = (mail.get("text") or "").strip()
        preview   = body[:200] + "…" if len(body) > 200 else body
        text += (
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>#{i} {html.escape(subject)}</b>\n"
            f"From: <code>{html.escape(from_addr)}</code>\n"
            f"{html.escape(preview) if preview else '<i>(ទទេ)</i>'}\n\n"
        )
    await send_msg(chat_id, text, reply_markup=EMAIL_SUBMENU_KB)


async def _email_handle_list(chat_id: int, user_id: int):
    emails = await run_sync(_email_history_list, user_id)
    if not emails:
        await send_msg(chat_id,
                       "📭 មិនទាន់មានអ៊ីម៉ែលទេ។ ចុច <b>✉️ អ៊ីម៉ែលថ្មី</b> ដើម្បីបង្កើត។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    lines = "\n".join(f"{i+1}. <code>{em}</code>" for i, em in enumerate(emails))
    await send_msg(chat_id,
                   f"📧 <b>បញ្ជីអ៊ីម៉ែល ({len(emails)})</b>\n\n{lines}",
                   reply_markup=EMAIL_SUBMENU_KB)


async def _email_show_token_info(chat_id: int):
    await _send_combined_token_info(chat_id, EMAIL_SUBMENU_KB)


async def _email_handle_delete_picker(chat_id: int, user_id: int):
    entries = await run_sync(_email_history_entries, user_id)
    if not entries:
        await send_msg(chat_id, "📭 មិនទាន់មានអ៊ីម៉ែលទេ។", reply_markup=EMAIL_SUBMENU_KB)
        return
    async with _data_lock:
        user_sessions[user_id] = {"state": "email_delete_picker"}
    asyncio.create_task(run_sync(_save_sessions))
    rows = [[KeyboardButton(e['email_address'])] for e in entries]
    rows.append([KeyboardButton(BTN_BACK_SETTINGS)])
    kb = ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)
    await send_msg(chat_id, "🗑 <b>ជ្រើសរើសអ៊ីម៉ែលដែលចង់លុប៖</b>", reply_markup=kb)


# ─── group 4: Admin keyboard button labels ────────────────────────────────────
@app.on_message(filters.private & admin_button_filter, group=4)
async def on_admin_button(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    btn     = (message.text or "").strip()
    async with get_user_lock(user_id):
        await _dispatch_admin_button(client, message, user_id, chat_id, btn)
    message.stop_propagation()


# ─── group 5: payment_pending message (anyone) ────────────────────────────────
@app.on_message(filters.private & payment_pending_filter, group=5)
async def on_payment_pending_msg(client, message):
    await _notify_must_finish_order(message.chat.id)
    message.stop_propagation()


# ─── group 6: Admin account-management session states ─────────────────────────
@app.on_message(filters.private & admin_filter, group=6)
async def on_admin_session_message(client, message):
    global accounts_data
    user_id    = message.from_user.id
    chat_id    = message.chat.id
    message_id = message.id
    text       = message.text or ""

    async with get_user_lock(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id)
        if not sess:
            await show_account_selection(chat_id)
            message.stop_propagation()
            return

        state = sess.get("state", "")

        if state == "waiting_for_accounts":
            email_pat = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
            accounts  = []
            for line in text.strip().split("\n"):
                em = line.strip()
                if em and email_pat.match(em):
                    accounts.append({"email": em})
            async with _data_lock:
                all_existing = {
                    a.get("email", "").lower()
                    for accs in accounts_data.get("account_types", {}).values()
                    for a in accs if a.get("email")
                }
            seen, deduped, intra_dupes = set(), [], []
            for a in accounts:
                k = a.get("email", "").lower()
                if k in seen:
                    intra_dupes.append(a["email"])
                else:
                    seen.add(k)
                    deduped.append(a)
            stock_dupes = [a["email"] for a in deduped if a.get("email", "").lower() in all_existing]
            new_accounts = [a for a in deduped if a.get("email", "").lower() not in all_existing]
            if new_accounts:
                warnings = []
                if intra_dupes:
                    warnings.append(f"⚠️ *អ៊ីមែលដដែល (រំលង)៖*\n```\n{chr(10).join(intra_dupes)}\n```")
                if stock_dupes:
                    warnings.append(f"⚠️ *អ៊ីមែលមានស្រាប់ (រំលង)៖*\n```\n{chr(10).join(stock_dupes)}\n```")
                if warnings:
                    await send_msg(chat_id, "\n\n".join(warnings), parse_mode=ParseMode.MARKDOWN)
                async with _data_lock:
                    sess["accounts"] = new_accounts
                    sess["state"]    = "waiting_for_account_type"
                    existing_types = list(accounts_data.get("account_types", {}).keys())
                asyncio.create_task(run_sync(_save_sessions))
                type_rows = [[KeyboardButton(t)] for t in existing_types]
                type_rows.append([KeyboardButton(BTN_BACK_SETTINGS)])
                type_kb = ReplyKeyboardMarkup(type_rows, resize_keyboard=True, is_persistent=True)
                await send_msg(chat_id,
                               f"<b>បានបញ្ចូល គូប៉ុង ចំនួន {len(new_accounts)}\n\nសូមជ្រើសរើស ឬបញ្ចូលប្រភេទ គូប៉ុង៖</b>",
                               reply_markup=type_kb)
            elif accounts:
                all_d = intra_dupes + stock_dupes
                await send_msg(chat_id,
                               "<b>មិនអាចបញ្ចូលបាន</b>",
                               reply_markup=ADD_ACCOUNT_KB)
            else:
                await send_msg(chat_id,
                               "<b>អ៊ីមែលមិនត្រឹមត្រូវតាមទម្រង់</b>",
                               reply_markup=ADD_ACCOUNT_KB)
            message.stop_propagation()
            return

        if state == "waiting_for_account_type":
            account_type_input = text.strip()
            async with _data_lock:
                existing_price = accounts_data.get("prices", {}).get(account_type_input)
                sess["account_type"] = account_type_input
                sess["state"]        = "waiting_for_price"
            asyncio.create_task(run_sync(_save_sessions))
            if existing_price is not None:
                await send_msg(
                    chat_id,
                    f"<b>ប្រភេទ <code>{account_type_input}</code> មានស្រាប់ ដែលមានតម្លៃ {existing_price}$\n\nតម្លៃត្រូវតែដូចគ្នា ({existing_price}$) ដើម្បីបន្ថែម គូប៉ុង</b>",
                    reply_markup=ADD_ACCOUNT_KB)
            else:
                await send_msg(chat_id,
                               f"<b>សូមដាក់តម្លៃក្នុងប្រភេទ គូប៉ុង {account_type_input}</b>",
                               reply_markup=ADD_ACCOUNT_KB)
            message.stop_propagation()
            return

        if state == "waiting_for_price":
            try:
                price = float(text.strip().replace("$", ""))
                account_type = sess["account_type"]
                accs_to_add  = sess["accounts"]
                async with _data_lock:
                    existing_price = accounts_data.get("prices", {}).get(account_type)
                    all_existing   = {
                        a.get("email", "").lower()
                        for pool in accounts_data.get("account_types", {}).values()
                        for a in pool if a.get("email")
                    }
                if existing_price is not None and round(existing_price, 4) != round(price, 4):
                    await send_msg(
                        chat_id,
                        f"❌ <b>មិនអាចបញ្ចូលបាន!</b>\n\nប្រភេទ <code>{account_type}</code> មានតម្លៃ <b>{existing_price}$</b> ស្រាប់។\nតម្លៃ <b>{price}$</b> មិនដូចគ្នា។ សូមប្រើ <b>{existing_price}$</b>",
                        reply_markup=ADD_ACCOUNT_KB)
                    message.stop_propagation()
                    return
                seen, deduped = set(), []
                for a in accs_to_add:
                    k = a.get("email", "").lower()
                    if k not in seen:
                        seen.add(k)
                        deduped.append(a)
                dup_emails  = [a["email"] for a in deduped if a.get("email", "").lower() in all_existing]
                new_accounts = [a for a in deduped if a.get("email", "").lower() not in all_existing]
                if dup_emails and not new_accounts:
                    await send_msg(chat_id,
                                   f"❌ *មិនអាចបញ្ចូលបាន!*\n\nEmail ទាំងអស់មានស្រាប់:\n```\n{chr(10).join(dup_emails)}\n```",
                                   parse_mode=ParseMode.MARKDOWN)
                    message.stop_propagation()
                    return
                if dup_emails:
                    await send_msg(chat_id,
                                   f"⚠️ *Email ខាងក្រោមមានស្រាប់ ហើយត្រូវបានរំលង:*\n```\n{chr(10).join(dup_emails)}\n```",
                                   parse_mode=ParseMode.MARKDOWN)
                async with _data_lock:
                    accounts_data["accounts"].extend(new_accounts)
                    if account_type in accounts_data["account_types"]:
                        accounts_data["account_types"][account_type].extend(new_accounts)
                    else:
                        accounts_data["account_types"][account_type] = new_accounts
                    accounts_data["prices"][account_type] = price
                    user_sessions.pop(user_id, None)
                asyncio.create_task(run_sync(_save_data))
                asyncio.create_task(run_sync(_save_sessions))
                await send_msg(
                    chat_id,
                    f"*✅ បានបញ្ចូល គូប៉ុង ដោយជោគជ័យ*\n\n"
                    f"```\n🔹 ចំនួន: {len(new_accounts)}\n🔹 ប្រភេទ: {account_type}\n🔹 តម្លៃ: {price}$\n```",
                    parse_mode=ParseMode.MARKDOWN)
                logger.info(f"Admin {user_id} added {len(new_accounts)} accounts of type {account_type} @ ${price}")
                await send_admin_settings_menu(chat_id)
            except ValueError:
                await send_msg(chat_id, "តម្លៃមិនត្រឹមត្រូវ។ សូមបញ្ចូលតម្លៃជាលេខ (ឧ: 5.99)")
            message.stop_propagation()
            return

        # Unrecognized admin message — clear session + show selection
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await show_account_selection(chat_id)
        message.stop_propagation()


# ─── group 7: Non-admin fallback ──────────────────────────────────────────────
@app.on_message(filters.private & ~admin_filter, group=7)
async def on_buyer_message(client, message):
    user = message.from_user
    asyncio.create_task(
        notify_admin_new_user(user.id, user.first_name, user.last_name, user.username))
    async with get_user_lock(user.id):
        await show_account_selection(message.chat.id)


# ─── Callback query handler ───────────────────────────────────────────────────
@app.on_callback_query(group=0)
async def on_callback_query(client, callback_query):
    user    = callback_query.from_user
    user_id = user.id
    chat_id = callback_query.message.chat.id
    data    = callback_query.data or ""
    logger.info(f"Callback from {user.first_name} (ID:{user_id}): {data}")

    asyncio.create_task(
        notify_admin_new_user(user_id, user.first_name, user.last_name, user.username))

    async with get_user_lock(user_id):
        await _handle_callback_locked(callback_query, user, user_id, chat_id, data)


async def _handle_callback_locked(cq, user, user_id, chat_id, data):
    try:
        # ── Buy account type ──────────────────────────────────────────────────
        if data.startswith("buy:") or data.startswith("buy_"):
            at = (_account_type_from_callback_id(data[4:]) if data.startswith("buy:")
                  else data.replace("buy_", ""))
            if not at:
                await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ។", show_alert=True)
                return
            if await _has_active_purchase(user_id):
                await cq.answer("សូមបញ្ចប់ការទិញបច្ចុប្បន្នជាមុនសិន", show_alert=True)
                return
            await cq.answer()
            async with _data_lock:
                count = len(accounts_data.get("account_types", {}).get(at, []))
                price = accounts_data.get("prices", {}).get(at, 0)
            if count <= 0:
                await send_msg(chat_id, f"សុំទោស! គូប៉ុង {at} អស់ស្តុក។")
                return
            await _reset_user_session(user_id, save=False)
            async with _data_lock:
                count = len(accounts_data["account_types"].get(at, []))
                user_sessions[user_id] = {
                    "state": "waiting_for_quantity", "account_type": at,
                    "price": price, "available_count": count, "started_at": time.time(),
                }
            asyncio.create_task(run_sync(_save_sessions))
            type_cb_id = _type_callback_id(at)
            qty_buttons = [
                InlineKeyboardButton(str(n), callback_data=f"qty:{type_cb_id}:{n}")
                for n in range(1, count + 1)
            ]
            rows_inline = [qty_buttons[i:i+5] for i in range(0, len(qty_buttons), 5)]
            rows_inline.append([InlineKeyboardButton("🚫 បោះបង់", callback_data="cancel_buy")])
            await send_msg(chat_id, "<b>សូមជ្រើសរើសចំនួនដែលចង់ទិញ៖</b>",
                           reply_markup=InlineKeyboardMarkup(rows_inline))
            asyncio.create_task(delete_msg(chat_id, cq.message.id))
            return

        # ── Out of stock ──────────────────────────────────────────────────────
        if data.startswith("out_of_stock"):
            await cq.answer()
            at = (_account_type_from_callback_id(data[13:]) if data.startswith("out_of_stock:")
                  else data.replace("out_of_stock_", "")) or "នេះ"
            await send_msg(chat_id, f"<i>សូមអភ័យទោស គូប៉ុង {at} អស់ពីស្តុក 🪤</i>",
                           parse_mode=ParseMode.HTML)
            return

        # ── Admin delete type: step 1 ─────────────────────────────────────────
        if data.startswith("dts:") and is_admin(user_id):
            type_name = _account_type_from_callback_id(data[4:]) or data[4:]
            if type_name not in accounts_data.get("account_types", {}):
                await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ!", show_alert=True)
                return
            await cq.answer()
            async with _data_lock:
                count = len(accounts_data["account_types"].get(type_name, []))
                price = accounts_data.get("prices", {}).get(type_name, 0)
            confirm_cb = f"dtc:{_type_callback_id(type_name)}"
            await send_msg(
                chat_id,
                f"⚠️ <b>តើអ្នកពិតជាចង់លុបប្រភេទ គូប៉ុង នេះមែនទេ?</b>\n\n"
                f"<blockquote>🔹 ប្រភេទ: {type_name}\n🔹 ចំនួន: {count}\n🔹 តម្លៃ: ${price}</blockquote>",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ បញ្ជាក់លុប", callback_data=confirm_cb),
                    InlineKeyboardButton("🚫 បោះបង់", callback_data="dtcancel"),
                ]]))
            return

        # ── Admin delete type: step 2 ─────────────────────────────────────────
        if data.startswith("dtc:") and is_admin(user_id):
            type_name = _account_type_from_callback_id(data[4:]) or data[4:]
            if type_name not in accounts_data.get("account_types", {}):
                await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ!", show_alert=True)
                return
            await cq.answer()
            async with _data_lock:
                count = len(accounts_data["account_types"].pop(type_name, []))
                accounts_data.get("prices", {}).pop(type_name, None)
                accounts_data["accounts"] = [
                    a for a in accounts_data.get("accounts", []) if a.get("type") != type_name]
            asyncio.create_task(run_sync(_save_data))
            asyncio.create_task(delete_msg(chat_id, cq.message.id))
            await send_msg(chat_id,
                           f"✅ <b>បានលុប <code>{type_name}</code> ចំនួន {count} records!</b>")
            logger.info(f"Admin {user_id} deleted type '{type_name}'")
            return

        if data == "dtcancel" and is_admin(user_id):
            await cq.answer()
            asyncio.create_task(delete_msg(chat_id, cq.message.id))
            await send_msg(chat_id, "🚫 <b>បានបោះបង់ការលុប</b>")
            return

        # ── Cancel buy (quantity selection) ───────────────────────────────────
        if data == "cancel_buy":
            await cq.answer()
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            asyncio.create_task(delete_msg(chat_id, cq.message.id))
            await show_account_selection(chat_id)
            return

        # ── Quantity selected ─────────────────────────────────────────────────
        if data.startswith("qty:"):
            parts = data.split(":")
            target_type, quantity = None, None
            try:
                if len(parts) == 3:
                    target_type = _account_type_from_callback_id(parts[1])
                    quantity    = int(parts[2])
                elif len(parts) == 2:
                    quantity = int(parts[1])
            except ValueError:
                pass
            if not quantity or quantity < 1:
                await cq.answer()
                return
            if await _has_active_purchase(user_id):
                await cq.answer("សូមបញ្ចប់ការទិញបច្ចុប្បន្នជាមុនសិន ឬចុច /cancel", show_alert=True)
                return
            async with _data_lock:
                session = user_sessions.get(user_id)
            if target_type and (not session or session.get("account_type") != target_type
                                or session.get("state") != "waiting_for_quantity"):
                if target_type not in accounts_data.get("account_types", {}):
                    await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ។", show_alert=True)
                    return
                await _reset_user_session(user_id, save=False)
                async with _data_lock:
                    available = len(accounts_data["account_types"].get(target_type, []))
                    price     = accounts_data.get("prices", {}).get(target_type, 0)
                if available <= 0:
                    await cq.answer(f"សូមអភ័យទោស គូប៉ុង {target_type} អស់ពីស្តុក 🪤", show_alert=True)
                    return
                async with _data_lock:
                    user_sessions[user_id] = {
                        "state": "waiting_for_quantity", "account_type": target_type,
                        "price": price, "available_count": available, "started_at": time.time(),
                    }
                    session = user_sessions[user_id]
            elif not session or session.get("state") != "waiting_for_quantity":
                await cq.answer()
                return
            if quantity > session["available_count"]:
                await cq.answer(f"សុំទោស! មានត្រឹមតែ {session['available_count']} នៅក្នុងស្តុក", show_alert=True)
                return
            async with _data_lock:
                session["quantity"]    = quantity
                session["total_price"] = quantity * session["price"]
            asyncio.create_task(delete_msg(chat_id, cq.message.id))
            await _start_payment_for_session(chat_id, user_id, session, callback_query=cq)
            return

        # ── Check payment ─────────────────────────────────────────────────────
        if data == "check_payment":
            async with _data_lock:
                session = user_sessions.get(user_id)
            if not session or session.get("state") != "payment_pending":
                session = await run_sync(_get_pending_payment, user_id)
            if not session:
                await cq.answer()
                return
            md5 = session.get("md5_hash")
            if not md5:
                await cq.answer("មានបញ្ហា។ សូមចាប់ផ្តើមម្តងទៀត។", show_alert=True)
                return
            is_paid, payment_data = await run_sync(_check_payment_status, md5)
            if is_paid:
                await cq.answer("✅ បានទទួលការបង់ប្រាក់!")
                user_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                await deliver_accounts(chat_id, user_id, session,
                                       payment_data=payment_data, user_name=user_name)
                asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                asyncio.create_task(run_sync(_save_sessions))
            else:
                await cq.answer(
                    "⏳ មិនទាន់បានទទួលការបង់ប្រាក់។\nសូមបង់ប្រាក់ហើយចុចពិនិត្យម្ដងទៀត។",
                    show_alert=True)
            return

        # ── Copy OTP code ─────────────────────────────────────────────────────
        if data.startswith("copy_otp:"):
            code = data.split(":", 1)[1]
            await cq.answer(code, show_alert=True)
            return

        # ── Cancel purchase ───────────────────────────────────────────────────
        if data == "cancel_purchase":
            async with _data_lock:
                session = user_sessions.get(user_id)
            if not session:
                session = await run_sync(_get_pending_payment, user_id)
            md5 = session.get("md5_hash") if session else None
            if md5:
                try:
                    is_paid, payment_data = await run_sync(_check_payment_status, md5)
                except Exception:
                    is_paid, payment_data = False, None
                if is_paid:
                    await cq.answer("✅ បានទទួលការបង់ប្រាក់!")
                    user_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                    await deliver_accounts(chat_id, user_id, session,
                                           payment_data=payment_data, user_name=user_name)
                    asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                    asyncio.create_task(run_sync(_save_sessions))
                    return
            await cq.answer()
            for key in ("photo_message_id", "qr_message_id", "dot_message_id"):
                mid = session.get(key) if session else None
                if mid:
                    asyncio.create_task(delete_msg(chat_id, mid))
            if session:
                await _release_reserved_accounts(session)
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            asyncio.create_task(run_sync(_delete_pending_payment, user_id))
            await show_account_selection(chat_id)
            return


    except Exception as e:
        logger.error(f"Callback handler error for user {user_id}: {e}")


# ── 19. Background periodic sweeper ──────────────────────────────────────────
async def _check_active_pending_payments():
    """Check all non-expired pending payments against Bakong API and deliver accounts if paid.
    This handles cases where the bot restarted and lost the in-memory QR expiry polling tasks."""
    try:
        r = await run_sync(
            _neon_query,
            "SELECT user_id, chat_id, account_type, quantity, total_price, md5_hash, reserved_accounts "
            "FROM bot_pending_payments "
            "WHERE created_at + ($1 || ' seconds')::interval >= NOW()",
            [str(PAYMENT_TIMEOUT_SECONDS)])
        rows = r.get("rows", []) or []
    except Exception as e:
        logger.warning(f"Failed to query active pending payments: {e}")
        return

    for row in rows:
        try:
            user_id = int(row["user_id"])
            async with _data_lock:
                active_session = user_sessions.get(user_id)
            if active_session and active_session.get("state") == "payment_pending":
                continue

            md5 = row.get("md5_hash")
            if not md5:
                continue

            is_paid, payment_data = await run_sync(_check_payment_status, md5)
            if not is_paid:
                continue

            reserved = row.get("reserved_accounts") or []
            if isinstance(reserved, str):
                try:
                    reserved = json.loads(reserved)
                except Exception:
                    reserved = []

            session = {
                "state": "payment_pending",
                "account_type": row.get("account_type"),
                "quantity": int(row.get("quantity") or 1),
                "total_price": float(row.get("total_price") or 0),
                "md5_hash": md5,
                "reserved_accounts": reserved,
            }
            chat_id = int(row.get("chat_id") or user_id)
            logger.info(f"Sweeper detected paid payment for user {user_id}, delivering accounts")
            await deliver_accounts(chat_id, user_id, session, payment_data=payment_data)
            asyncio.create_task(run_sync(_delete_pending_payment, user_id))
            asyncio.create_task(run_sync(_save_sessions))
        except Exception as e:
            logger.warning(f"Sweeper failed to process payment row {row}: {e}")


async def _pending_payment_sweeper(interval: int = 60):
    while True:
        await asyncio.sleep(interval)
        try:
            await _check_active_pending_payments()
        except Exception as e:
            logger.warning(f"Active payment check failed: {e}")
        try:
            await run_sync(_cleanup_expired_pending_payments)
        except Exception as e:
            logger.warning(f"Sweeper iteration failed: {e}")


async def _email_poller(interval: int = 10):
    """Background task: polls all email_history entries for new mail and notifies the admin."""
    while True:
        try:
            await asyncio.sleep(interval)
            if not DROPMAIL_API_TOKEN:
                continue
            entries = await run_sync(_email_history_all_entries)
            for entry in entries:
                entry_id    = entry.get("id")
                user_id     = int(entry.get("telegram_user_id") or 0)
                email_addr  = entry.get("email_address", "")
                session_id  = entry.get("dropmail_session_id")
                restore_key = entry.get("restore_key")
                last_mail_id = entry.get("last_mail_id")
                if not session_id:
                    continue
                try:
                    mails = await run_sync(_dropmail_get_mails, session_id, last_mail_id)
                except Exception as e:
                    logger.debug(f"[email_poller] poll error [{email_addr}]: {e}")
                    continue
                if mails is None:
                    if not restore_key:
                        continue
                    try:
                        restored = await run_sync(_dropmail_restore_session, email_addr, restore_key)
                        if restored and restored.get("session_id"):
                            await run_sync(_email_history_update_session, entry_id,
                                           restored["session_id"],
                                           restored.get("address_id", ""),
                                           restored.get("restore_key", ""))
                            logger.info(f"[email_poller] Restored [{email_addr}] → {restored['session_id']}")
                    except Exception as e:
                        logger.debug(f"[email_poller] restore error [{email_addr}]: {e}")
                    continue
                if not mails:
                    continue
                newest_id = None
                for mail in mails:
                    mail_id   = mail.get("id")
                    if last_mail_id and mail_id == last_mail_id:
                        continue
                    subject   = mail.get("headerSubject") or "(គ្មានប្រធានបទ)"
                    from_addr = mail.get("fromAddr") or "unknown"
                    to_addr   = mail.get("toAddr") or email_addr
                    body      = (mail.get("text") or "").strip()
                    preview = body[:1200] + "\n…" if len(body) > 1200 else body
                    text = (
                        f"📬 <b>អ៊ីម៉ែលថ្មីចូលមកដល់!</b>\n\n"
                        f"📨 ប្រធានបទ: <b>{html.escape(subject)}</b>\n"
                        f"📧 ពី: <code>{html.escape(from_addr)}</code>\n"
                        f"📥 ទៅ: <code>{html.escape(to_addr)}</code>\n\n"
                        f"{html.escape(preview) if preview else '<i>(ទទេ)</i>'}"
                    )
                    try:
                        target = int(CHANNEL_ID) if CHANNEL_ID else user_id
                        await send_msg(target, text)
                    except Exception as e:
                        logger.warning(f"[email_poller] notify failed: {e}")
                    newest_id = mail_id
                if newest_id:
                    await run_sync(_email_history_update_last_mail, entry_id, newest_id)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[email_poller] outer error: {e}")


async def _resume_scheduled_deletions():
    try:
        r = await run_sync(
            _neon_query,
            "SELECT chat_id, message_id, "
            "GREATEST(0, EXTRACT(EPOCH FROM (delete_at - NOW())))::int AS remaining "
            "FROM bot_scheduled_deletions")
        rows = r.get("rows", []) or []
        for row in rows:
            try:
                cid = int(row["chat_id"])
                mid = int(row["message_id"])
                rem = int(row.get("remaining") or 0)
                asyncio.create_task(delete_msg_later(cid, mid, rem))
            except Exception as e:
                logger.warning(f"Bad scheduled deletion row {row}: {e}")
        if rows:
            logger.info(f"Resumed {len(rows)} scheduled deletion(s)")
    except Exception as e:
        logger.error(f"Failed to resume scheduled deletions: {e}")


# ── 20. Startup sequence ──────────────────────────────────────────────────────
async def _on_startup():
    global accounts_data, PAYMENT_NAME, MAINTENANCE_MODE, CHANNEL_ID
    global BAKONG_TOKEN, BAKONG_RELAY_TOKEN, BAKONG_API_TOKEN, khqr_client, EXTRA_ADMIN_IDS
    global DROPMAIL_API_TOKEN, DROPMAIL_TOKEN_EXPIRY, _DROPMAIL_URL

    await run_sync(_init_db)

    # Restore settings from DB (cache-backed)
    _sv = await run_sync(_get_setting, "PAYMENT_NAME")
    if _sv:
        PAYMENT_NAME = _sv
        logger.info(f"Loaded PAYMENT_NAME from DB: {PAYMENT_NAME}")

    _sv = await run_sync(_get_setting, "MAINTENANCE_MODE")
    if _sv is not None:
        MAINTENANCE_MODE = str(_sv).lower() == "true"
        logger.info(f"Loaded MAINTENANCE_MODE: {MAINTENANCE_MODE}")

    _sv = await run_sync(_get_setting, "EXTRA_ADMIN_IDS")
    if _sv:
        try:
            EXTRA_ADMIN_IDS = set(int(x) for x in json.loads(_sv))
            logger.info(f"Loaded {len(EXTRA_ADMIN_IDS)} extra admin(s)")
        except Exception:
            pass

    _sv_relay = await run_sync(_get_setting, "BAKONG_RELAY_TOKEN")
    if _sv_relay:
        BAKONG_RELAY_TOKEN = _sv_relay
        logger.info(f"Loaded BAKONG_RELAY_TOKEN from DB: {BAKONG_RELAY_TOKEN[:10]}...")

    _sv_api = await run_sync(_get_setting, "BAKONG_API_TOKEN")
    if _sv_api:
        BAKONG_API_TOKEN = _sv_api
        logger.info(f"Loaded BAKONG_API_TOKEN from DB: {BAKONG_API_TOKEN[:10]}...")

    _sv_legacy = await run_sync(_get_setting, "BAKONG_TOKEN")
    if _sv_legacy and not _sv_relay and not _sv_api:
        if _sv_legacy.startswith("rbk"):
            BAKONG_RELAY_TOKEN = _sv_legacy
        else:
            BAKONG_API_TOKEN = _sv_legacy

    BAKONG_TOKEN = BAKONG_RELAY_TOKEN if BAKONG_RELAY_TOKEN else BAKONG_API_TOKEN
    if BAKONG_TOKEN:
        try:
            khqr_client = KHQR(BAKONG_TOKEN)
            logger.info(f"Active BAKONG_TOKEN: {'relay' if BAKONG_TOKEN.startswith('rbk') else 'bakong'} ({BAKONG_TOKEN[:10]}...)")
        except Exception as e:
            logger.error(f"Failed to rebuild KHQR client: {e}")

    _sv = await run_sync(_get_setting, "TELEGRAM_CHANNEL_ID")
    if _sv:
        CHANNEL_ID = _sv.strip()
        logger.info(f"Loaded TELEGRAM_CHANNEL_ID: {CHANNEL_ID}")

    _sv = await run_sync(_get_setting, "DROPMAIL_API_TOKEN")
    if _sv:
        DROPMAIL_API_TOKEN = _sv
        _DROPMAIL_URL = f"https://dropmail.me/api/graphql/{DROPMAIL_API_TOKEN}"
        logger.info(f"Loaded DROPMAIL_API_TOKEN from DB: {DROPMAIL_API_TOKEN[:6]}…")

    _sv = await run_sync(_get_setting, "DROPMAIL_TOKEN_EXPIRY")
    if _sv:
        DROPMAIL_TOKEN_EXPIRY = _sv
        logger.info(f"Loaded DROPMAIL_TOKEN_EXPIRY from DB: {DROPMAIL_TOKEN_EXPIRY}")

    # Load data and sessions into memory
    data = await run_sync(_load_data)
    accounts_data.update(data)
    await run_sync(_load_sessions)

    # Resume background tasks
    await _resume_scheduled_deletions()
    await run_sync(_cleanup_expired_pending_payments)
    asyncio.create_task(_pending_payment_sweeper(60))
    logger.info("Pending-payment sweeper started (every 60s)")
    asyncio.create_task(_email_poller(10))
    logger.info("Email poller started (every 10s)")

    me = await app.get_me()
    logger.info(f"Bot connected: @{me.username}")

    # Drain any pending Bot API HTTP queue so Pyrogram MTProto can receive
    # new updates cleanly (stale queued updates block MTProto delivery).
    await run_sync(_drain_bot_api_queue)
    logger.info("Bot is now listening for updates (Pyrogram MTProto)...")



# ── 22. Main ──────────────────────────────────────────────────────────────────
async def _run():
    await app.start()
    try:
        await _on_startup()
        await idle()
    finally:
        await app.stop()


if __name__ == "__main__":
    app.run(_run())
