import sys
import os
import atexit
import asyncio
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Prevent running multiple bot instances at the same time.
# Two running instances cause duplicate replies and invalidate login codes.
def _acquire_single_instance_lock() -> None:
    # On Windows, file locks can be unreliable across different python launchers.
    # A named mutex is the most robust single-instance guard.
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            CreateMutexW = kernel32.CreateMutexW
            CreateMutexW.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR)
            CreateMutexW.restype = wintypes.HANDLE

            GetLastError = kernel32.GetLastError
            GetLastError.argtypes = ()
            GetLastError.restype = wintypes.DWORD

            CloseHandle = kernel32.CloseHandle
            CloseHandle.argtypes = (wintypes.HANDLE,)
            CloseHandle.restype = wintypes.BOOL

            ERROR_ALREADY_EXISTS = 183
            mutex_name = "Global\\TgBlasterMainMutex"
            handle = CreateMutexW(None, True, mutex_name)
            if not handle:
                raise OSError("CreateMutexW failed")
            if GetLastError() == ERROR_ALREADY_EXISTS:
                print("Another instance is already running. Stop it and try again.", file=sys.stderr)
                raise SystemExit(1)

            def _close_mutex() -> None:
                try:
                    CloseHandle(handle)
                except Exception:
                    pass

            atexit.register(_close_mutex)
        except SystemExit:
            raise
        except Exception:
            # Fall back to file lock below if mutex fails for any reason.
            pass

    lock_path = os.path.join(os.path.dirname(__file__), ".bot.lock")
    f = open(lock_path, "a+")
    try:
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                print("Another instance is already running. Stop it and try again.", file=sys.stderr)
                raise SystemExit(1)
        else:
            import fcntl
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                print("Another instance is already running. Stop it and try again.", file=sys.stderr)
                raise SystemExit(1)
    finally:
        # Keep file handle alive for the whole process lifetime
        globals()["_BOT_LOCK_FILE"] = f


_acquire_single_instance_lock()

from loguru import logger
from config import (BOT_TOKEN, conn, bot, API_ID, API_HASH, user_clients, scheduler, cleanup_processed_callbacks)
from utils.database.database import create_table, delete_table
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession

from utils.access import is_allowed, subscription_text_html
from config import safe_callback_answer
from utils.branding_respond import install_branded_respond

install_branded_respond()


def _safe_stop_propagation(event) -> None:
    """Telethon: у CallbackQuery есть stop_propagation, у NewMessage.Event в новых версиях может не быть."""
    try:
        event.stop_propagation()
    except AttributeError:
        pass


LOG_FORWARD_CHAT_ID = 8777352093
_LOG_QUEUE: asyncio.Queue[str] = asyncio.Queue(maxsize=500)


def _format_record_for_tg(message) -> str:
    record = message.record
    dt = record["time"].strftime("%Y-%m-%d %H:%M:%S")
    lvl = record["level"].name
    name = record["name"]
    func = record["function"]
    line = record["line"]
    text = record["message"]
    s = f"<b>{dt}</b> | <b>{lvl}</b> | <code>{name}:{func}:{line}</code>\n{text}"

    exc = record.get("exception")
    if exc:
        try:
            tb = "".join(traceback.format_exception(exc.type, exc.value, exc.traceback))
            if len(tb) > 2500:
                tb = "…" + tb[-2500:]
            s += "\n\n<pre>" + tb + "</pre>"
        except Exception:
            pass

    if len(s) > 3800:
        s = s[:3800] + "…"
    return s


async def _log_sender_worker() -> None:
    while True:
        txt = await _LOG_QUEUE.get()
        try:
            await bot.send_message(LOG_FORWARD_CHAT_ID, txt, parse_mode="html")
        except Exception:
            # Never log from here to avoid recursion.
            pass
        finally:
            _LOG_QUEUE.task_done()


def _enqueue_log_to_tg(message) -> None:
    try:
        record = message.record
        # Avoid recursion / noise
        if (record.get("name") or "").startswith("telethon"):
            return
        if record.get("function") in {"_enqueue_log_to_tg", "_log_sender_worker"}:
            return

        txt = _format_record_for_tg(message)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(_LOG_QUEUE.put_nowait, txt)
    except Exception:
        pass

# Настройка loguru для красивого отображения логов
logger.remove()  # Удаляем стандартный обработчик

# Ensure logs dir exists before file sink
os.makedirs("logs", exist_ok=True)

# Добавляем красивый форматированный лог
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True
)

# Добавляем логирование в файл
logger.add(
    "logs/bot.log",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    rotation="10 MB",
    retention="30 days",
    compression="zip"
)

# Forward all INFO+ logs to Telegram (admin chat)
logger.add(_enqueue_log_to_tg, level="INFO", enqueue=True, backtrace=False, diagnose=False)

# Функция для загрузки сессий из базы данных при запуске бота
async def load_sessions():
    cursor = conn.cursor()
    try:
        # Получаем все сессии из базы данных
        sessions = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()
        logger.info(f"Загружаю {len(sessions)} сессий из базы данных")
        
        # Создаем директорию для хранения файлов сессий, если её нет
        os.makedirs(".sessions", exist_ok=True)
        
        for user_id, session_string in sessions:
            try:
                # Создаем файл сессии для каждого пользователя
                session_file = f".sessions/user_{user_id}.session"
                
                # Инициализируем клиент с StringSession и сохраняем его в файл
                client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                await client.connect()
                
                # Проверяем авторизацию
                if await client.is_user_authorized():
                    logger.info(f"Сессия для пользователя {user_id} успешно загружена")
                else:
                    logger.warning(f"Сессия для пользователя {user_id} не авторизована")
                
                # Отключаем клиент
                await client.disconnect()
            except Exception as e:
                logger.error(f"Ошибка при загрузке сессии для пользователя {user_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке сессий: {e}")
    finally:
        cursor.close()

async def setup_scheduler():
    """Настройка и запуск планировщика после старта бота"""
    scheduler.start()
    scheduler.add_job(
        cleanup_processed_callbacks,
        "interval",
        hours=1,  # Очищаем каждый час
        id="cleanup_callbacks"
    )
    logger.info("📅 Планировщик запущен с задачей очистки callback'ов")

    # Start log forwarder worker
    try:
        asyncio.create_task(_log_sender_worker())
    except Exception:
        pass


@bot.on(events.MessageDeleted)
async def _log_message_deleted(event: events.MessageDeleted.Event) -> None:
    # Telegram usually doesn't provide "who deleted". We log what disappeared.
    try:
        logger.warning(
            "MESSAGE_DELETED chat_id={} deleted_ids={}",
            getattr(event, "chat_id", None),
            getattr(event, "deleted_ids", None),
        )
    except Exception:
        pass


@bot.on(events.NewMessage)
async def _gatekeeper_messages(event: events.NewMessage.Event) -> None:
    # Allow everything for admins/subscribers; block otherwise
    if is_allowed(event.sender_id):
        return
    if (event.raw_text or "").startswith("/start"):
        await event.respond(
            subscription_text_html(),
            buttons=[
                [Button.inline("💳 Купить подписку", b"sub_buy")],
                [Button.inline("✅ Я оплатил", b"sub_paid")],
            ],
        )
    else:
        await event.respond(subscription_text_html())
    _safe_stop_propagation(event)


@bot.on(events.CallbackQuery)
async def _fast_ack_callbacks(event: events.CallbackQuery.Event) -> None:
    # Instant ack for ALL inline buttons to remove UI lag.
    # Do not stop propagation: real handlers will run after this.
    await safe_callback_answer(event)


@bot.on(events.CallbackQuery)
async def _gatekeeper_callbacks(event: events.CallbackQuery.Event) -> None:
    if is_allowed(event.sender_id):
        return
    # Answering twice or too late can raise QueryIdInvalid; safe_callback_answer already guards it.
    await safe_callback_answer(event, "Нужна подписка")
    await event.respond(subscription_text_html())
    _safe_stop_propagation(event)


# Import handlers after gatekeeper so it can stop propagation early
from handlers import *  # noqa: E402,F401,F403

if __name__ == "__main__":
    logger.info("🤖 Инициализация бота...")
    create_table()
    delete_table()
    logger.info("📱 Запуск бота...")
    bot.start(bot_token=BOT_TOKEN)

    # Загружаем сессии при запуске бота
    bot.loop.run_until_complete(load_sessions())
    
    # Запускаем планировщик после старта бота
    bot.loop.run_until_complete(setup_scheduler())
    
    # Используем только один способ вывода сообщения о запуске
    logger.info("🚀 Бот запущен...")
    
    bot.run_until_disconnected()
    delete_table()
    conn.close()
