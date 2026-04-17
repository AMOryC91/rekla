import asyncio
import datetime
import html
from loguru import logger
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
from telethon.tl.custom import Button

from config import callback_query, API_ID, API_HASH, scheduler, Query, bot, conn, New_Message, \
    broadcast_solo_state, callback_message
from utils.telegram import gid_key, get_entity_by_id, create_broadcast_data, deliver_broadcast, event_message_has_image
from utils.logging import log_message_event, log_user_action
from utils.text import normalize_telegram_html, event_to_broadcast_payload
from utils.access import can_manage_session
from utils.reply_nav import maybe_install_reply_nav_keyboard


async def send_broadcast_message(user_id: int, group_id: int, text: str, session_string: str, photo_url: Optional[str] = None, max_retries: int = 10) -> None:
    """
    Отправляет сообщение рассылки в группу с обработкой ошибок и повторными попытками.
    
    Args:
        user_id: ID пользователя (владельца аккаунта)
        group_id: ID группы для отправки
        text: Текст сообщения
        session_string: Строка сессии Telethon
        photo_url: Опциональный путь к фото для отправки
        max_retries: Максимальное количество попыток отправки
    """
    retry_count = 0
    job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
    
    while retry_count < max_retries:
        try:
            async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                with conn:
                    cursor = conn.cursor()
                    # Получаем актуальный текст рассылки и фото из базы данных
                    cursor.execute("""SELECT broadcast_text, photo_url, broadcast_entities FROM broadcasts 
                                    WHERE group_id = ? AND user_id = ?""",
                                   (gid_key(group_id), user_id))
                    current_data = cursor.fetchone()
                    txt = current_data[0] if current_data and current_data[0] else text
                    entities_json = current_data[2] if current_data and len(current_data) > 2 else None
                    if not entities_json:
                        txt = normalize_telegram_html(txt)
                    photo_url_from_db = current_data[1] if current_data and len(current_data) > 1 else None
                    photo_to_send = photo_url_from_db if photo_url_from_db else photo_url
                    
                    # Получаем информацию о группе
                    try:
                        group_entity = await get_entity_by_id(client, group_id)

                        if not group_entity:
                            group_row = cursor.execute(
                                "SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?",
                                (user_id, group_id),
                            ).fetchone()

                            if group_row and group_row[0]:
                                group_username = group_row[0]
                                if group_username.startswith("@"):
                                    group_entity = await client.get_entity(group_username)
                                else:
                                    try:
                                        group_id_int = int(group_username)
                                        group_entity = await get_entity_by_id(client, group_id_int)
                                    except ValueError:
                                        group_entity = await client.get_entity(group_username)

                        if not group_entity:
                            raise ValueError(f"Не удалось определить чат для group_id={group_id}")

                        await deliver_broadcast(
                            client,
                            group_entity,
                            txt,
                            photo_to_send,
                            entities_json,
                            None,
                            None,
                        )
                        logger.debug(f"Отправлено в {getattr(group_entity, 'title', 'группу')}")
                        
                        # Записываем в историю отправок
                        cursor.execute("""INSERT INTO send_history 
                                        (user_id, group_id, group_name, sent_at, message_text) 
                                        VALUES (?, ?, ?, ?, ?)""",
                                       (user_id, group_id, getattr(group_entity, 'title', ''),
                                        datetime.datetime.now().isoformat(), txt))
                        
                        # Обновляем статус рассылки
                        cursor.execute("""UPDATE broadcasts 
                                        SET error_reason = NULL 
                                        WHERE user_id = ? AND group_id = ?""",
                                       (user_id, gid_key(group_id)))
                        conn.commit()
                        return  # Успешно отправлено, выходим из функции
                        
                    except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                        error_msg = f"Нет прав писать в группу: {e}"
                        logger.error(error_msg)
                        cursor.execute("""UPDATE broadcasts 
                                        SET is_active = ?, error_reason = ? 
                                        WHERE user_id = ? AND group_id = ?""",
                                       (False, error_msg, user_id, gid_key(group_id)))
                        conn.commit()
                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)
                        return  # Нет смысла повторять, выходим из функции
                        
                    except Exception as entity_error:
                        # Проверяем, не связана ли ошибка с невозможностью найти entity
                        if "Cannot find any entity corresponding to" in str(entity_error):
                            logger.error(f"Не удалось найти группу: {entity_error}")
                            error_msg = f"Не удалось найти группу: {entity_error}"
                            cursor.execute("""UPDATE broadcasts 
                                            SET is_active = ?, error_reason = ? 
                                            WHERE user_id = ? AND group_id = ?""",
                                           (False, error_msg, user_id, gid_key(group_id)))
                            conn.commit()
                            if scheduler.get_job(job_id):
                                scheduler.remove_job(job_id)
                            return  # Нет смысла повторять, выходим из функции
                        else:
                            raise entity_error  # Другие ошибки пробрасываем дальше
        
        except (FloodWaitError, SlowModeWaitError) as e:
            wait_time = e.seconds
            logger.warning(f"{type(e).__name__}: ожидание {wait_time} сек.")
            await asyncio.sleep(wait_time + 10)
            retry_count += 1
        except Exception as e:
            logger.error(f"Ошибка при отправке: {type(e).__name__}: {e}")
            retry_count += 1
            await asyncio.sleep(5)
    
    logger.warning(f"Не удалось отправить сообщение после {max_retries} попыток")
    with conn:
        cursor = conn.cursor()
        error_msg = f"Не удалось отправить после {max_retries} попыток"
        cursor.execute("""UPDATE broadcasts 
                        SET is_active = ?, error_reason = ? 
                        WHERE user_id = ? AND group_id = ?""",
                       (False, error_msg, user_id, gid_key(group_id)))
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)


async def _solo_start_broadcast(event, st: dict, photo_path: Optional[str]) -> bool:
    """Сохраняет рассылку в БД и ставит задачу планировщика для одной группы. Снимает solo-state при успехе или фатальной ошибке."""
    job_id = f"broadcast_{st['user_id']}_{gid_key(st['group_id'])}"
    create_broadcast_data(
        st["user_id"],
        st["group_id"],
        st["text"],
        st["interval"],
        photo_path,
        st.get("entities_json"),
        None,
        None,
    )
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (st["user_id"],)).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти сессию для этого аккаунта.")
        broadcast_solo_state.pop(event.sender_id, None)
        cursor.close()
        return False
    session_string = row[0]
    cursor.close()
    trigger = IntervalTrigger(minutes=st["interval"])
    next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)
    scheduler.add_job(
        send_broadcast_message,
        trigger,
        args=[st["user_id"], st["group_id"], st["text"], session_string, photo_path],
        id=job_id,
        next_run_time=next_run,
        replace_existing=True,
    )
    if not scheduler.running:
        scheduler.start()
    broadcast_solo_state.pop(event.sender_id, None)
    return True


@bot.on(Query(data=lambda d: d.decode() == "bc_solo_msg"))
async def solo_wizard_back_message(event: callback_query) -> None:
    uid = event.sender_id
    if uid not in broadcast_solo_state:
        await event.answer("Мастер не активен. Начните из меню группы.", alert=True)
        return
    st = broadcast_solo_state[uid]
    if st.get("step") != "interval":
        await event.answer("Назад с этого шага недоступен.", alert=True)
        return
    st["step"] = "text"
    st.pop("embedded_photo_path", None)
    await event.answer()
    await event.respond(
        "📝 Отправьте новое сообщение для рассылки (текст, форматирование, фото с подписью):"
    )


@bot.on(Query(data=lambda d: d.decode() == "bc_solo_int"))
async def solo_wizard_back_interval(event: callback_query) -> None:
    uid = event.sender_id
    if uid not in broadcast_solo_state:
        await event.answer("Мастер не активен.", alert=True)
        return
    st = broadcast_solo_state[uid]
    if st.get("step") != "photo_choice":
        await event.answer("Назад с этого шага недоступен.", alert=True)
        return
    st["step"] = "interval"
    st.pop("interval", None)
    await event.answer()
    back_btn = [[Button.inline("◀️ Изменить сообщение", b"bc_solo_msg")]]
    await event.respond(
        "⏲️ Снова введите интервал в <b>минутах</b> (одно число):",
        buttons=back_btn,
    )


@bot.on(Query(data=lambda d: d.decode().startswith("BroadcastTextInterval_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    data = event.data.decode()
    prefix = "BroadcastTextInterval_"
    if not data.startswith(prefix):
        await event.respond("⚠ Неверный формат callback.")
        return
    rest = data[len(prefix):]
    user_id_str, group_id_str = rest.rsplit("_", 1)
    user_id, group_id = int(user_id_str), int(group_id_str)
    if not can_manage_session(admin_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    broadcast_solo_state[admin_id] = {"user_id": user_id, "mode": "same", "step": "text", "group_id": group_id}
    await event.respond(
        "📝 <b>Сообщение для рассылки</b> (одна группа)\n\n"
        "Отправьте боту <b>текст</b>, <b>форматирование</b>, при необходимости <b>фото с подписью</b> "
        "или пересланное сообщение — в целевой чат уйдёт <b>копия</b> (от имени аккаунта рассылки).\n\n"
        "<i>Можно нажать «Назад» на следующих шагах, чтобы изменить сообщение или интервал.</i>"
    )


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_solo_state))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_solo_state[event.sender_id]
    tid = st.get("user_id")
    if tid is None or not can_manage_session(event.sender_id, int(tid)):
        broadcast_solo_state.pop(event.sender_id, None)
        await event.respond("⚠ Нет доступа к этому аккаунту.")
        return
    log_message_event(event, "обработка диалога индивидуальной рассылки")
    if st.get("step") == "photo_choice":
        await event.respond("⚠ Сначала нажмите кнопку под вопросом про фото (или /start).")
        return
    # шаг 1 — сообщение рассылки (текст / пересланное / фото+подпись)
    if st["step"] == "text":
        text, ent_json = event_to_broadcast_payload(event)
        if not (text or "").strip() and not event_message_has_image(event):
            await event.respond(
                "⚠ Нужен текст, подпись к медиа или изображение. Отправьте сообщение ещё раз."
            )
            return
        st["text"] = text
        st["entities_json"] = ent_json
        st.pop("embedded_photo_path", None)
        if event_message_has_image(event):
            try:
                st["embedded_photo_path"] = await event.download_media()
            except Exception as e:
                logger.error(f"download_media (solo text): {e}")
                await event.respond("⚠ Не удалось сохранить изображение. Попробуйте без фото или отправьте снова.")
                return
        st["step"] = "interval"
        back_btn = [[Button.inline("◀️ Изменить сообщение", b"bc_solo_msg")]]
        await event.respond(
            "⏲️ Введите интервал в <b>минутах</b> (одно число, например <code>30</code>):",
            buttons=back_btn,
        )
        return

    # шаг 2 - получили интервал
    if st["step"] == "interval":
        try:
            min_time = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return

        st["interval"] = min_time
        if st.get("embedded_photo_path"):
            if await _solo_start_broadcast(event, st, st["embedded_photo_path"]):
                await event.respond(
                    f"✅ Рассылка запущена: каждые {st['interval']} мин (текст и изображение из вашего сообщения)."
                )
            return
        st["step"] = "photo_choice"
        buttons = [
            [Button.inline("✅ Да, другое фото к тексту", b"photo_yes")],
            [Button.inline("📸 Только изображение (без текста)", b"photo_only")],
            [Button.inline("❌ Нет, как в сообщении выше", b"photo_no")],
            [Button.inline("◀️ К интервалу", b"bc_solo_int")],
        ]
        await event.respond(
            "📸 Нужно ли <b>отдельное</b> фото сверх того, что уже в сообщении?\n"
            "(Если вы уже отправили фото с текстом — нажмите «Нет».)",
            buttons=buttons,
        )
        return
        
    # шаг 3 - получили фото (если пользователь выбрал "Да" или "Только изображение")
    if st["step"] == "photo" or st["step"] == "photo_only":
        if event.photo or event_message_has_image(event):
            try:
                txt, ent_json = event_to_broadcast_payload(event)
                st["text"] = txt
                st["entities_json"] = ent_json
                photo_path = await event.download_media()
                if await _solo_start_broadcast(event, st, photo_path):
                    message_type = "только фото" if st["step"] == "photo_only" else "текст + фото"
                    await event.respond(f"✅ Рассылка запущена: каждые {st['interval']} мин ({message_type}).")
            except Exception as e:
                logger.error(f"Ошибка при обработке фото: {e}")
                await event.respond("⚠ Произошла ошибка при обработке фото. Попробуйте еще раз или выберите рассылку без фото.")
        else:
            await event.respond("⚠ Пожалуйста, отправьте фото или выберите рассылку без фото (/start).")
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes"))
async def photo_yes_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    st["step"] = "photo"
    
    await event.respond(
        "📤 Отправьте изображение — оно будет отправляться вместе с текстом из прошлого шага:"
    )


@bot.on(Query(data=lambda d: d.decode() == "photo_only"))
async def photo_only_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    st["step"] = "photo_only"
    st["text"] = ""
    st["entities_json"] = None
    st.pop("embedded_photo_path", None)

    await event.respond("📤 Отправьте изображение (без текста или с подписью — подпись пойдёт в рассылку):")


@bot.on(Query(data=lambda d: d.decode() == "photo_no"))
async def photo_no_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    if await _solo_start_broadcast(event, st, st.get("embedded_photo_path")):
        await event.respond(f"✅ Рассылка запущена: каждые {st['interval']} мин.")


@bot.on(Query(data=lambda data: data.decode().startswith("StartResumeBroadcast_")))
async def start_resume_broadcast(event: callback_query) -> None:
    data = event.data.decode()
    prefix = "StartResumeBroadcast_"
    if not data.startswith(prefix):
        await event.respond("⚠ Неверный формат callback.")
        return
    rest = data[len(prefix):]
    try:
        user_id_str, group_id_str = rest.rsplit("_", 1)
        user_id, group_id = int(user_id_str), int(group_id_str)
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении данных: {e}")
        return

    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return

    cursor = conn.cursor()
    job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
    existing_job = scheduler.get_job(job_id)

    if existing_job:
        await event.respond("⚠ Рассылка уже активна для этой группы.")
        cursor.close()
        return

    # Получаем данные рассылки из базы
    cursor.execute("""
                SELECT broadcast_text, interval_minutes, photo_url, broadcast_entities
                FROM broadcasts 
                WHERE user_id = ? AND group_id = ?
            """, (user_id, gid_key(group_id)))
    row = cursor.fetchone()

    if not row:
        # Если данных нет, предлагаем настроить рассылку
        await event.respond("⚠ Рассылка еще не настроена для этой группы. Пожалуйста, настройте текст и интервал рассылки.")
        cursor.close()
        return
    
    broadcast_text = row[0]
    interval_minutes = row[1]
    photo_url = row[2] if len(row) > 2 else None
    broadcast_entities = row[3] if len(row) > 3 else None
    has_body = (broadcast_text and str(broadcast_text).strip()) or photo_url
    if not has_body or not interval_minutes or interval_minutes <= 0:
        await event.respond("⚠ Пожалуйста, убедитесь, что текст (или фото) рассылки и корректный интервал установлены.")
        cursor.close()
        return
    
    # Получаем сессию пользователя
    session_string_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?",
                                        (user_id,)).fetchone()
    if not session_string_row:
        await event.respond("⚠ Ошибка: не найден session_string для аккаунта.")
        cursor.close()
        return
    
    session_string = session_string_row[0]
    
    # Проверяем, существует ли запись о группе
    group_row = cursor.execute("SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?", 
                              (user_id, group_id)).fetchone()
    if not group_row:
        await event.respond(f"⚠ Группа не найдена в базе данных для user_id={user_id}, group_id={group_id}.")
        cursor.close()
        return

    # Активируем рассылку в базе данных
    cursor.execute("""
        UPDATE broadcasts 
        SET is_active = ?, error_reason = NULL
        WHERE user_id = ? AND group_id = ?
    """, (True, user_id, gid_key(group_id)))
    
    # Если запись не существует, создаем новую
    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO broadcasts (user_id, group_id, broadcast_text, interval_minutes, is_active, error_reason, photo_url, broadcast_entities, broadcast_fwd_bot_id, broadcast_fwd_msg_ids)
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
        """, (user_id, gid_key(group_id), broadcast_text, interval_minutes, True, photo_url, broadcast_entities, None, None))
        
    conn.commit()
    
    # Создаем задачу в планировщике
    trigger = IntervalTrigger(minutes=interval_minutes)
    next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
    
    # Добавляем задачу в планировщик
    scheduler.add_job(
        send_broadcast_message,
        trigger,
        args=[user_id, group_id, broadcast_text, session_string, photo_url],
        id=job_id,
        next_run_time=next_run,
        replace_existing=True
    )
    
    # Запускаем планировщик, если он еще не запущен
    if not scheduler.running:
        scheduler.start()
    
    await event.respond(f"✅ Рассылка успешно запущена! Первое сообщение будет отправлено через 10 секунд, затем каждые {interval_minutes} минут.")
    cursor.close()


@bot.on(Query(data=lambda data: data.decode().startswith("StopAccountBroadcast_")))
async def stop_broadcast(event: callback_query) -> None:
    try:
        data = event.data.decode()
        prefix = "StopAccountBroadcast_"
        if not data.startswith(prefix):
            await event.respond("⚠ Неверный формат callback.")
            await maybe_install_reply_nav_keyboard(event)
            return
        rest = data[len(prefix):]
        user_id_str, group_id_str = rest.rsplit("_", 1)
        user_id, group_id = int(user_id_str), int(group_id_str)
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        await maybe_install_reply_nav_keyboard(event)
        return

    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        await maybe_install_reply_nav_keyboard(event)
        return

    cursor = conn.cursor()
    
    # Проверяем наличие сессии
    session_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not session_row:
        await event.respond("⚠ Ошибка: не найдена сессия для этого аккаунта.")
        cursor.close()
        await maybe_install_reply_nav_keyboard(event)
        return

    session_string = session_row[0]
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)
    
    # Проверяем наличие группы
    group_row = cursor.execute("SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?", 
                             (user_id, group_id)).fetchone()
    if not group_row:
        await event.respond("⚠ Ошибка: не найдена группа.")
        cursor.close()
        await maybe_install_reply_nav_keyboard(event)
        return

    group_username = group_row[0]
    
    try:
        await client.connect()
        
        # Пытаемся получить entity группы
        try:
            # Проверяем, является ли username числом (ID группы) или именем пользователя
            if group_username.startswith('@'):
                # Это username группы
                group = await client.get_entity(group_username)
            else:
                # Пробуем получить entity по ID
                try:
                    group_id_int = int(group_username)
                    group = await get_entity_by_id(client, group_id_int)
                    if not group:
                        await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                        await client.disconnect()
                        cursor.close()
                        return
                except ValueError:
                    # Если не можем преобразовать в число, пробуем использовать как есть
                    group = await client.get_entity(group_username)
        except Exception as entity_error:
            logger.error(f"Ошибка при получении entity для группы {group_username}: {entity_error}")
            
            # Пробуем получить entity другим способом
            if "Cannot find any entity corresponding to" in str(entity_error):
                try:
                    # Преобразуем username в ID, если это возможно
                    try:
                        group_id_int = int(group_username)
                        group = await get_entity_by_id(client, group_id_int)
                        if not group:
                            # Если не удалось получить entity, останавливаем задачу без информации о группе
                            job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
                            job = scheduler.get_job(job_id)
                            
                            if job:
                                job.remove()
                                cursor.execute("UPDATE broadcasts SET is_active = ?, error_reason = ? WHERE user_id = ? AND group_id = ?",
                                               (False, "Администратор остановил рассылку", user_id, gid_key(group_id)))
                                conn.commit()
                                await event.respond(f"⛔ Рассылка в группу с ID {group_id} остановлена.")
                                await client.disconnect()
                                cursor.close()
                                return
                    except ValueError:
                        # Если username не является числом, пробуем другие методы
                        return
                    except Exception as alt_error:
                        logger.error(f"[DEBUG] Ошибка при альтернативном получении Entity: {alt_error}")
                        return
                except Exception as alt_error:
                    logger.error(f"[DEBUG] Ошибка при альтернативном получении Entity: {alt_error}")
                    return
            else:
                await event.respond(f"⚠ Ошибка при получении информации о группе: {str(entity_error)}")
                await client.disconnect()
                cursor.close()
                return
        
        # Останавливаем задачу
        job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
        job = scheduler.get_job(job_id)
        
        if job:
            job.remove()
            cursor.execute("UPDATE broadcasts SET is_active = ?, error_reason = ? WHERE user_id = ? AND group_id = ?",
                           (False, "Администратор остановил рассылку", user_id, gid_key(group_id)))
            conn.commit()
            title_esc = html.escape(str(getattr(group, "title", group_username)))
            await event.respond(f"⛔ Рассылка в группу <b>{title_esc}</b> остановлена.")
        else:
            title_esc = html.escape(str(getattr(group, "title", group_username)))
            await event.respond(f"⚠ Рассылка в группу <b>{title_esc}</b> не была запущена.")
    except Exception as e:
        logger.error(f"Ошибка при остановке рассылки: {e}")
        
        # Если произошла неожиданная ошибка, все равно пытаемся остановить задачу
        try:
            job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
            job = scheduler.get_job(job_id)
            
            if job:
                job.remove()
                cursor.execute("UPDATE broadcasts SET is_active = ?, error_reason = ? WHERE user_id = ? AND group_id = ?",
                               (False, "Администратор остановил рассылку", user_id, gid_key(group_id)))
                conn.commit()
                await event.respond(f"⛔ Рассылка в группу с ID {group_id} остановлена (с ошибкой: {str(e)}).")
            else:
                await event.respond(f"⚠ Рассылка в группу с ID {group_id} не была запущена.")
        except Exception as stop_error:
            await event.respond(f"⚠ Критическая ошибка при остановке рассылки: {str(stop_error)}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            await maybe_install_reply_nav_keyboard(event)
        except Exception:
            pass
