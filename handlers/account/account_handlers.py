from loguru import logger

import asyncio
import time
from io import BytesIO
import datetime
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeExpiredError, PhoneCodeInvalidError
from telethon.sessions import StringSession
from telethon import Button
from telethon.tl.functions.auth import SendCodeRequest, SignInRequest, ResendCodeRequest

from config import (callback_query, callback_message, phone_waiting, code_waiting, password_waiting, user_clients,
                    API_ID,
                    API_HASH, broadcast_all_state, user_states, New_Message, Query, bot, conn,
                    phone_code_hash_waiting, code_requested_at, qr_login_waiting)
from utils.reply_nav import maybe_install_reply_nav_keyboard


@bot.on(Query(data=b"add_account"))
async def add_account(event: callback_query) -> None:
    """
    Добавляет аккаунт
    """
    logger.info("Выбрана кнопка добавления аккаунта")
    await event.respond(
        "Выберите способ входа:\n\n"
        "✅ <b>QR-вход (рекомендуется)</b> — без кодов, Telegram не блокирует.\n"
        "⚠️ <b>Вход по коду</b> — Telegram часто блокирует, если код отправлять в чат.\n",
        buttons=[
            [Button.inline("🔐 Войти по QR", b"login_qr")],
            [Button.inline("📲 Войти по коду (номер)", b"login_code")],
        ],
    )
    await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=b"login_code"))
async def login_code(event: callback_query) -> None:
    user_id: int = event.sender_id
    await event.respond(
        "⚠️ Telegram блокирует вход, если код отправлять в чат (даже своему боту).\n\n"
        "Чтобы добавить аккаунт <b>без QR и без блокировки</b>, добавляй его локально через консоль:\n\n"
        "<code>python tools/add_account_cli.py</code>\n\n"
        "После добавления — вернись в бота и нажми «Мои аккаунты».",
    )
    await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=b"login_qr"))
async def login_qr(event: callback_query) -> None:
    user_id = event.sender_id

    # Avoid parallel QR sessions per user
    existing = qr_login_waiting.get(user_id)
    if existing is not None:
        await event.respond("QR уже выдан. Отсканируйте его в Telegram (Настройки → Устройства → Подключить устройство).")
        return

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    try:
        qr = await client.qr_login()
        qr_login_waiting[user_id] = {"client": client, "qr": qr}

        # Build QR image if qrcode is available; otherwise send URL.
        qr_url = qr.url
        try:
            import qrcode  # type: ignore

            img = qrcode.make(qr_url)
            buf = BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            await event.respond(
                "🔐 Отсканируйте QR в Telegram:\n"
                "Настройки → Устройства → Подключить устройство.\n\n"
                "У вас ~2 минуты.",
                file=buf,
            )
        except Exception:
            await event.respond(
                "🔐 QR-вход:\n\n"
                "У меня нет библиотеки для генерации картинки QR, поэтому даю ссылку.\n"
                "Откройте её на другом устройстве/в браузере и отсканируйте в Telegram:\n\n"
                f"`{qr_url}`"
            )

        # Wait until user scans/accepts.
        try:
            await asyncio.wait_for(qr.wait(), timeout=120)
        except asyncio.TimeoutError:
            await event.respond("⌛ QR истёк. Нажмите «Войти по QR» ещё раз.")
            return

        # Authorized → persist session
        session_string = client.session.save()
        me = await client.get_me()
        cursor = conn.cursor()
        try:
            if not cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (me.id,)).fetchall():
                cursor.execute(
                    "INSERT INTO sessions (user_id, session_string, first_name, username, phone, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        me.id,
                        session_string,
                        getattr(me, "first_name", None),
                        getattr(me, "username", None),
                        getattr(me, "phone", None),
                        datetime.datetime.now().isoformat(),
                    ),
                )
                conn.commit()
                await event.respond("✅ Аккаунт добавлен через QR!")
            else:
                await event.respond("❌ Такой аккаунт уже есть")
        finally:
            cursor.close()
    except Exception as e:
        logger.error(f"QR login error: {e}")
        await event.respond(f"⚠ Ошибка QR-входа: {e}")
    finally:
        qr_login_waiting.pop(user_id, None)
        try:
            await client.disconnect()
        except Exception:
            pass


@bot.on(New_Message(func=lambda e: e.sender_id in phone_waiting and e.text.startswith("+") and e.text[1:].isdigit()))
async def send_code_for_phone(event: callback_message) -> None:
    """
    Отправляет код на телефон
    """
    user_id: int = event.sender_id
    phone_number: str = event.text.strip()
    logger.info(f"Отправляю {user_id} на телефон {phone_number} код подтверждения")

    # Cooldown to prevent invalidating codes by repeated requests.
    now = time.time()
    last = code_requested_at.get(user_id)
    if last and (now - last) < 60:
        await event.respond(
            "⏳ Код уже запрошен недавно.\n\n"
            "Подождите ~60 секунд (частые запросы делают старые коды недействительными), "
            "или нажмите кнопку ниже для повторной отправки."
        , buttons=[[Button.inline("🔁 Отправить код снова", b"resend_code")]])
        return

    user_clients[user_id] = TelegramClient(StringSession(), API_ID, API_HASH)
    await user_clients[user_id].connect()

    await event.respond("⏳ Отправляю код подтверждения...")

    try:
        sent = await user_clients[user_id].send_code_request(phone_number)
        code_requested_at[user_id] = now
        # Explicitly store the hash to avoid mismatches if Telethon cache is lost.
        phone_code_hash_waiting[user_id] = sent.phone_code_hash
        sent_type = getattr(sent, "type", None)
        sent_type_name = type(sent_type).__name__ if sent_type is not None else "Unknown"
        sent_timeout = getattr(sent, "timeout", None)
        logger.debug(
            f"send_code_request sent_type={sent_type_name} timeout={sent_timeout} hash={sent.phone_code_hash} user_id={user_id}"
        )
        code_waiting[user_id] = phone_number
        del phone_waiting[user_id]
        await event.respond(
            "✅ Код отправлен!\n\n"
            "⏰ Код действует 5-10 минут\n"
            "📱 Код придёт в чат «Telegram» (в приложении) или по SMS.\n"
            "Если не пришёл — подождите 60 секунд и нажмите «Отправить код снова»."
            ,
            buttons=[[Button.inline("🔁 Отправить код снова", b"resend_code")]]
        )
        logger.info(f"Код отправлен")
    except Exception as e:
        if isinstance(e, (SendCodeRequest, FloodWaitError)):
            sec_time = int(str(e).split()[3])
            message = (f"⚠ Телеграмм забанил за быстрые запросы. "
                       f"Подождите {(a := sec_time // 3600)} Часов {(b := ((sec_time - a * 3600) // 60))}"
                       f" Минут {sec_time - a * 3600 - b * 60} Секунд")
            await event.respond(message)
            logger.error(message)
        else:
            phone_waiting.pop(user_id, None)
            user_clients.pop(user_id, None)
            logger.error(f"⚠ Произошла ошибка: {e}")
            await event.respond(f"⚠ Произошла ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")


@bot.on(Query(data=b"resend_code"))
async def resend_code(event: callback_query) -> None:
    user_id = event.sender_id
    phone_number = code_waiting.get(user_id)
    phone_code_hash = phone_code_hash_waiting.get(user_id)
    client = user_clients.get(user_id)

    if not phone_number or not phone_code_hash or not client:
        await event.respond("Нет активного запроса кода. Нажмите «Добавить аккаунт» и введите номер заново.")
        return

    now = time.time()
    last = code_requested_at.get(user_id)
    if last and (now - last) < 60:
        await event.respond("Подождите немного перед повторной отправкой (≈60 сек).")
        return

    try:
        if not client.is_connected():
            await client.connect()
        sent = await client(ResendCodeRequest(phone_number=phone_number, phone_code_hash=phone_code_hash))
        # Update hash after resend
        new_hash = getattr(sent, "phone_code_hash", None) or phone_code_hash
        phone_code_hash_waiting[user_id] = new_hash
        code_requested_at[user_id] = now
        await event.respond("✅ Код отправлен повторно. Введите самый последний код.")
    except Exception as e:
        logger.error(f"Ошибка при resend_code: {e}")
        await event.respond(f"⚠ Не удалось отправить код повторно: {e}")


@bot.on(New_Message(
    func=lambda e: e.sender_id in code_waiting and e.text.isdigit() and e.sender_id not in broadcast_all_state))
async def get_code(event: callback_message) -> None:
    """
    Проверяет код от пользователя
    """
    code = event.text.strip()
    user_id = event.sender_id
    phone_number = code_waiting[user_id]
    phone_code_hash = phone_code_hash_waiting.get(user_id)
    cursor = conn.cursor()
    try:
        # Ensure client is connected before sign-in attempt
        if not user_clients[user_id].is_connected():
            await user_clients[user_id].connect()

        if not phone_code_hash:
            raise RuntimeError("Missing phone_code_hash for this login attempt. Request a new code.")

        # Use explicit MTProto request to avoid any cached/mismatched hash issues.
        logger.debug(f"sign_in for {user_id} with hash: {phone_code_hash}")
        await user_clients[user_id](SignInRequest(
            phone_number=phone_number,
            phone_code_hash=phone_code_hash,
            phone_code=code,
        ))
        session_string = user_clients[user_id].session.save()
        me = await user_clients[user_id].get_me()
        if not cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (me.id, )).fetchall():
            cursor.execute(
                "INSERT INTO sessions (user_id, session_string, first_name, username, phone, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    me.id,
                    session_string,
                    getattr(me, "first_name", None),
                    getattr(me, "username", None),
                    getattr(me, "phone", None),
                    datetime.datetime.now().isoformat(),
                ),
            )
            conn.commit()
            await event.respond("✅ Авторизация прошла успешно!")
        else:
            await event.respond("❌ Такой аккаунт уже есть")
        del code_waiting[user_id]
        phone_code_hash_waiting.pop(user_id, None)
        del user_clients[user_id]
    except SessionPasswordNeededError:
        password_waiting[user_id] = {"waiting": True, "last_message_id": event.message.id}
        await event.respond("⚠ Этот аккаунт защищен паролем. Отправьте пароль:")
    except PhoneCodeExpiredError as e:
        logger.error(f"Код истек: {e}")
        del code_waiting[user_id]
        phone_code_hash_waiting.pop(user_id, None)
        user_clients.pop(user_id, None)
        await event.respond(
            "⏰ Код подтверждения не принят (Telegram вернул: «код истёк»).\n\n"
            "Обычно это происходит, если был запрошен новый код и старый стал недействительным.\n"
            "Нажмите 'Добавить аккаунт' и введите самый последний код из Telegram."
        )
    except PhoneCodeInvalidError as e:
        logger.error(f"Неверный код: {e}")
        await event.respond(f"❌ Неверный код подтверждения\n\n"
                          f"Проверьте код в SMS и введите его еще раз.\n"
                          f"Если проблема повторяется, нажмите 'Добавить аккаунт' для нового кода.")
    except Exception as e:
        del code_waiting[user_id]
        phone_code_hash_waiting.pop(user_id, None)
        user_clients.pop(user_id, None)
        logger.error(f"Ошибка: {e}, Неверный код")
        await event.respond(f"❌ Неверный код или ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")
    finally:
        cursor.close()


@bot.on(New_Message(func=lambda
        e: e.sender_id in password_waiting and e.sender_id not in user_states and e.sender_id not in broadcast_all_state))
async def get_password(event: callback_message) -> None:
    user_id = event.sender_id
    if password_waiting[user_id]["waiting"] and event.message.id > password_waiting[user_id]["last_message_id"]:
        password = event.text.strip()
        cursor = conn.cursor()
        try:
            await user_clients[user_id].sign_in(password=password)
            me = await user_clients[user_id].get_me()
            session_string = user_clients[user_id].session.save()

            cursor.execute(
                "INSERT INTO sessions (user_id, session_string, first_name, username, phone, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    me.id,
                    session_string,
                    getattr(me, "first_name", None),
                    getattr(me, "username", None),
                    getattr(me, "phone", None),
                    datetime.datetime.now().isoformat(),
                ),
            )
            conn.commit()

            del password_waiting[user_id]
            del user_clients[user_id]
            await event.respond("✅ Авторизация с паролем прошла успешно!")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при вводе пароля: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")
        finally:
            cursor.close()
