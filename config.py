import sqlite3
from typing import Dict, Tuple, List, Optional, Set

import asyncio
import telethon.events
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config
from telethon import TelegramClient
from pathlib import Path

# Python 3.14+ no longer creates a default event loop automatically.
# Telethon expects an event loop to exist at import/initialization time.
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Конфиг
API_ID: int = int(config("API_ID"))
API_HASH: str = config("API_HASH")
BOT_TOKEN: str = config("BOT_TOKEN")
ADMIN_ID_LIST: List[int] = list(map(int, map(str.strip, config("ADMIN_ID_LIST").split(","))))  # <-- Вставить ID разрешенных телеграмм аккаунтов через запятую

bot: TelegramClient = TelegramClient(
    "bot",
    API_ID,
    API_HASH,
    use_ipv6=False,
    connection_retries=5,
    retry_delay=2,
    timeout=15,
)

# Telethon 1.39 doesn't accept parse_mode in constructor; set default here.
bot.parse_mode = "html"

# Always store DB next to this file to avoid CWD-related mismatches
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "sessions.db"
conn: sqlite3.Connection = sqlite3.connect(str(DB_PATH), timeout=30.0)

# Аннотирование
New_Message = telethon.events.NewMessage
Query = telethon.events.CallbackQuery
callback_query = Query.Event
callback_message = New_Message.Event
__Dict_int_str = Dict[int, str]
__Dict_all_str = Dict[str, str]
__Dict_int_dict = Dict[int, dict]


phone_waiting: Dict[int, bool] = {}  # Список пользователей ожидающие подтверждения телефона

code_waiting: __Dict_int_str = {}
phone_code_hash_waiting: __Dict_int_str = {}
code_requested_at: Dict[int, float] = {}
qr_login_waiting: Dict[int, object] = {}
broadcast_all_text: __Dict_int_str = {}
user_states: __Dict_int_str = {}

password_waiting: __Dict_int_dict = {}
broadcast_all_state: __Dict_int_dict = {}
broadcast_solo_state: __Dict_int_dict = {}
broadcast_all_state_account: __Dict_int_dict = {}
user_sessions: __Dict_int_dict = {}

user_sessions_deleting: Dict[int, __Dict_all_str] = {}
user_sessions_phone: Dict[Tuple[int, int], __Dict_all_str] = {}

user_clients: Dict[int, TelegramClient] = {}
scheduler: AsyncIOScheduler = AsyncIOScheduler()

# State: adding a group to a specific account
add_group_to_account_state: Dict[int, Dict[str, int]] = {}

# Словарь для отслеживания обработанных callback-запросов
processed_callbacks: Dict[str, bool] = {}

# Навигация админа для reply-клавиатуры «Назад» / «Главное меню» (токены: H, L, A:uid, G:uid, I:uid:gid)
admin_nav_stack: Dict[int, List[str]] = {}
# Кому уже отправили постоянную reply-клавиатуру навигации
admin_reply_kb_installed: Set[int] = set()


async def safe_callback_answer(event: callback_query, text: str = "") -> None:
    """
    Безопасно отвечает на callback query, обрабатывая возможные ошибки.
    """
    try:
        await event.answer(text)
    except Exception as e:
        # Игнорируем ошибки QueryIdInvalid и другие ошибки ответа на callback
        from loguru import logger
        logger.debug(f"Ошибка при ответе на callback: {e}")
        pass


def cleanup_processed_callbacks() -> None:
    """
    Очищает словарь processed_callbacks для предотвращения утечек памяти.
    Вызывается периодически через scheduler.
    """
    processed_callbacks.clear()
