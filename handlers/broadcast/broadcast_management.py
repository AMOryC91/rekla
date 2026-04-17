import asyncio
import datetime
from loguru import logger
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import Button, TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat

from config import callback_query, callback_message, broadcast_all_state, API_ID, API_HASH, scheduler, Query, bot, conn, \
    New_Message
from utils.telegram import gid_key, create_broadcast_data, get_active_broadcast_groups, get_entity_by_id, deliver_broadcast, event_message_has_image
from utils.logging import log_message_event, log_user_action
from utils.text import normalize_telegram_html, event_to_broadcast_payload, strip_html_tags, ellipsize
from utils.access import can_manage_session
from utils.reply_nav import maybe_install_reply_nav_keyboard


@bot.on(Query(data=lambda d: d.decode().startswith("broadcastAll_")))
async def broadcast_all_menu(event: callback_query) -> None:
    admin_id = event.sender_id
    target_user_id = int(str(event.data.decode()).split("_")[1])
    if not can_manage_session(admin_id, target_user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    # запоминаем аккаунт, с которого шлём
    broadcast_all_state[admin_id] = {"user_id": target_user_id}

    keyboard = [
        [Button.inline("⏲️ Один интервал на все чаты", f"sameIntervalAll_{target_user_id}")],
        [Button.inline("🎲 Случайный интервал (мин–макс)", f"diffIntervalAll_{target_user_id}")],
        [Button.inline("◀️ К группам", f"groups_{target_user_id}".encode())],
    ]
    await event.respond(
        "<b>📨 Рассылка по всем чатам аккаунта</b>\n\n"
        "Выберите режим интервала, затем пришлите текст рассылки и настройте фото при необходимости.\n"
        "Сообщения уходят в группы <b>как копия</b> (от имени этого аккаунта).",
        buttons=keyboard,
    )
    await maybe_install_reply_nav_keyboard(event)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("sameIntervalAll_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    if not can_manage_session(admin_id, uid):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "same", "step": "text"}
    await event.respond(
        "📝 <b>Сообщение для рассылки</b> по <b>всем</b> группам этого аккаунта.\n\n"
        "Отправьте боту текст, форматирование и при необходимости фото с подписью — в группы уйдёт "
        "<b>копия</b> от имени аккаунта рассылки.\n\n"
        "<i>На шагах интервала и фото есть кнопки «Назад».</i>"
    )


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diffIntervalAll_")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    if not can_manage_session(admin_id, uid):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "diff", "step": "text"}
    await event.respond(
        "📝 <b>Сообщение для рассылки</b> — отправьте боту, затем укажите диапазон интервалов.\n"
        "В группы уходит <b>копия</b> сообщения (не пересылка)."
    )


@bot.on(Query(data=lambda d: d.decode() == "bc_all_msg"))
async def broadcast_all_wizard_back_message(event: callback_query) -> None:
    uid = event.sender_id
    if uid not in broadcast_all_state:
        await event.answer("Мастер не активен.", alert=True)
        return
    st = broadcast_all_state[uid]
    if st.get("step") == "interval" and st.get("mode") == "same":
        st["step"] = "text"
        st.pop("embedded_photo_path", None)
        await event.answer()
        await event.respond("📝 Отправьте новое сообщение для рассылки:")
        return
    if st.get("mode") == "diff" and st.get("step") == "min":
        st["step"] = "text"
        st.pop("embedded_photo_path", None)
        st.pop("min", None)
        await event.answer()
        await event.respond("📝 Отправьте новое сообщение для рассылки:")
        return
    if st.get("mode") == "diff" and st.get("step") == "max":
        st["step"] = "min"
        st.pop("max_m", None)
        await event.answer()
        await event.respond(
            "🔢 Снова введите минимальный интервал (мин):",
            buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_all_msg")]],
        )
        return
    await event.answer("Назад недоступен.", alert=True)


@bot.on(Query(data=lambda d: d.decode() == "bc_all_int"))
async def broadcast_all_wizard_back_interval(event: callback_query) -> None:
    uid = event.sender_id
    if uid not in broadcast_all_state:
        await event.answer("Мастер не активен.", alert=True)
        return
    st = broadcast_all_state[uid]
    if st.get("step") != "photo_choice":
        await event.answer("Назад недоступен.", alert=True)
        return
    if st.get("mode") == "same":
        st["step"] = "interval"
        st.pop("min_time", None)
        await event.answer()
        await event.respond(
            "⏲️ Снова введите интервал (минуты, одно число):",
            buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_all_msg")]],
        )
        return
    if st.get("mode") == "diff":
        st["step"] = "max"
        st.pop("max_m", None)
        await event.answer()
        await event.respond(
            "🔢 Снова введите максимальный интервал (мин):",
            buttons=[[Button.inline("◀️ К мин. интервалу", b"bc_all_msg")]],
        )
        return
    await event.answer("Назад недоступен.", alert=True)


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_all_state[event.sender_id]
    tid = st.get("user_id")
    if tid is None or not can_manage_session(event.sender_id, int(tid)):
        broadcast_all_state.pop(event.sender_id, None)
        await event.respond("⚠ Нет доступа к этому аккаунту. Начните с карточки аккаунта.")
        return
    log_message_event(event, "обработка диалога рассылки")
    if st.get("step") == "photo_choice":
        await event.respond("⚠ Сначала нажмите кнопку под вопросом про фото (или /start).")
        return
    # шаг 1 — сообщение рассылки
    if st["step"] == "text":
        text, ent_json = event_to_broadcast_payload(event)
        if not (text or "").strip() and not event_message_has_image(event):
            await event.respond("⚠ Нужен текст, подпись к медиа или изображение.")
            return
        st["text"] = text
        st["entities_json"] = ent_json
        st.pop("embedded_photo_path", None)
        if event_message_has_image(event):
            try:
                st["embedded_photo_path"] = await event.download_media()
            except Exception as e:
                logger.error(f"download_media (broadcast_all text): {e}")
                await event.respond("⚠ Не удалось сохранить изображение. Попробуйте снова.")
                return
        if st["mode"] == "same":
            st["step"] = "interval"
            await event.respond(
                "⏲️ Введите интервал (в минутах, одно число):",
                buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_all_msg")]],
            )
        else:
            st["step"] = "min"
            await event.respond(
                "🔢 Минимальный интервал (мин):",
                buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_all_msg")]],
            )
        return

    # одинаковый интервал
    if st["mode"] == "same" and st["step"] == "interval":
        try:
            min_time = int(event.text)
        except ValueError:
            await event.respond("⚠ Некорректное число. Попробуйте ещё раз.")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return

        st["min_time"] = min_time
        if st.get("embedded_photo_path"):
            await schedule_account_broadcast(
                int(st["user_id"]),
                st["text"],
                st["min_time"],
                None,
                st["embedded_photo_path"],
                st.get("entities_json"),
                None,
                None,
            )
            await event.respond(
                f"✅ Запущено: каждые {st['min_time']} мин (текст и изображение из вашего сообщения)."
            )
            broadcast_all_state.pop(event.sender_id, None)
            return
        st["step"] = "photo_choice"
        buttons = [
            [Button.inline("✅ Да, другое фото к тексту", b"photo_yes_all")],
            [Button.inline("📸 Только изображение", b"photo_only_all")],
            [Button.inline("❌ Нет, как в сообщении выше", b"photo_no_all")],
            [Button.inline("◀️ К интервалу", b"bc_all_int")],
        ]
        await event.respond(
            "📸 Нужно ли отдельное фото сверх сообщения?\n(Если фото уже в сообщении — «Нет».)",
            buttons=buttons,
        )
        return
    
    # шаг для получения фото (если выбрали "Да" или "Только изображение")
    if st["step"] == "photo" or st["step"] == "photo_only":
        if event.photo or event_message_has_image(event):
            try:
                txt, ent_json = event_to_broadcast_payload(event)
                st["text"] = txt
                st["entities_json"] = ent_json
                photo_path = await event.download_media()
                if st["mode"] == "same":
                    await schedule_account_broadcast(
                        int(st["user_id"]),
                        st["text"],
                        st["min_time"],
                        None,
                        photo_path,
                        st.get("entities_json"),
                        None,
                        None,
                    )
                    message_type = "только фото" if st["step"] == "photo_only" else "текст + фото"
                    await event.respond(f"✅ Запущено: каждые {st['min_time']} мин ({message_type}).")
                else:
                    await schedule_account_broadcast(
                        int(st["user_id"]),
                        st["text"],
                        st["min"],
                        st["max_m"],
                        photo_path,
                        st.get("entities_json"),
                        None,
                        None,
                    )
                    message_type = "только фото" if st["step"] == "photo_only" else "текст + фото"
                    await event.respond(
                        f"✅ Запущено: случайно каждые {st['min']}-{st['max_m']} мин ({message_type})."
                    )
                broadcast_all_state.pop(event.sender_id, None)
            except Exception as e:
                logger.error(f"Ошибка при обработке фото: {e}")
                await event.respond("⚠ Произошла ошибка при обработке фото. Попробуйте еще раз или выберите рассылку без фото.")
        else:
            await event.respond("⚠ Пожалуйста, отправьте фото или выберите рассылку без фото (/start).")
        return

    # случайный интервал — шаг 2 (min)
    if st["mode"] == "diff" and st["step"] == "min":
        try:
            st["min"] = int(event.text)
        except ValueError:
            await event.respond("⚠ Некорректное число. Попробуйте ещё раз.")
            return
        if st["min"] <= 0:
            await event.respond("⚠ Минимальное число должно быть больше нуля.")
            return
        st["step"] = "max"
        await event.respond(
            "🔢 Максимальный интервал (мин):",
            buttons=[[Button.inline("◀️ К мин. интервалу", b"bc_all_msg")]],
        )
        return

    # случайный интервал — шаг 3 (max)
    if st["mode"] == "diff" and st["step"] == "max":
        try:
            max_m = int(event.text)
        except ValueError:
            await event.respond("⚠ Некорректное число. Попробуйте ещё раз.")
            return
        if max_m <= st["min"]:
            await event.respond("⚠ Максимальное число должно быть больше минимального числа.")
            return

        st["max_m"] = max_m
        if st.get("embedded_photo_path"):
            await schedule_account_broadcast(
                int(st["user_id"]),
                st["text"],
                st["min"],
                st["max_m"],
                st["embedded_photo_path"],
                st.get("entities_json"),
                None,
                None,
            )
            await event.respond(
                f"✅ Запущено: случайно каждые {st['min']}-{st['max_m']} мин (текст и изображение из сообщения)."
            )
            broadcast_all_state.pop(event.sender_id, None)
            return
        st["step"] = "photo_choice"
        buttons = [
            [Button.inline("✅ Да, другое фото к тексту", b"photo_yes_all")],
            [Button.inline("📸 Только изображение", b"photo_only_all")],
            [Button.inline("❌ Нет, как в сообщении выше", b"photo_no_all")],
            [Button.inline("◀️ К интервалу", b"bc_all_int")],
        ]
        await event.respond(
            "📸 Нужно ли отдельное фото сверх сообщения?\n(Если фото уже в сообщении — «Нет».)",
            buttons=buttons,
        )
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes_all"))
async def photo_yes_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    st["step"] = "photo"
    
    await event.respond("📤 Пожалуйста, отправьте фото, которое хотите прикрепить к сообщению:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only_all"))
async def photo_only_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    st["step"] = "photo_only"
    st["text"] = ""
    st["entities_json"] = None
    st.pop("embedded_photo_path", None)

    await event.respond("📤 Отправьте изображение (с подписью или без):")


@bot.on(Query(data=lambda d: d.decode() == "photo_no_all"))
async def photo_no_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    
    ent = st.get("entities_json")
    if st["mode"] == "same":
        await schedule_account_broadcast(
            int(st["user_id"]),
            st["text"],
            st["min_time"],
            None,
            st.get("embedded_photo_path"),
            ent,
            None,
            None,
        )
        await event.respond(f"✅ Запущено: каждые {st['min_time']} мин.")
    else:
        await schedule_account_broadcast(
            int(st["user_id"]),
            st["text"],
            st["min"],
            st["max_m"],
            st.get("embedded_photo_path"),
            ent,
            None,
            None,
        )
        await event.respond(f"✅ Запущено: случайно каждые {st['min']}-{st['max_m']} мин.")
    
    broadcast_all_state.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode().startswith("StopBroadcastAll_")))
async def stop_broadcast_all(event: callback_query) -> None:
    data = event.data.decode()
    try:
        user_id = int(data.split("_")[1])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        return

    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return

    cursor = conn.cursor()
    msg = ["⛔ **Остановленные рассылки**:\n\n"]
    
    # Получаем информацию о группах для отображения названий вместо ID
    groups_data = cursor.execute("""
        SELECT g.group_id, g.group_username, b.is_active 
        FROM groups g 
        LEFT JOIN broadcasts b ON g.group_id = b.group_id AND b.user_id = g.user_id
        WHERE g.user_id = ?
    """, (user_id,)).fetchall()
    
    # Проверяем, есть ли активные рассылки
    has_stopped = False
    
    # Получаем клиента для получения названий групп
    session_string = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not session_string:
        await event.respond("⚠ Не найдена сессия для этого аккаунта.")
        cursor.close()
        return
    
    client = TelegramClient(StringSession(session_string[0]), API_ID, API_HASH)
    await client.connect()
    
    try:
        for group_id, group_username, is_active in groups_data:
            # Спробуємо отримати інформацію про групу
            try:
                # Спробуємо отримати entity групи
                try:
                    # Перевіряємо, чи це username чи ID
                    if group_username.startswith('@'):
                        # Це username групи
                        entity = await client.get_entity(group_username)
                    else:
                        # Спробуємо отримати entity за ID
                        try:
                            group_id_int = int(group_username)
                            entity = await get_entity_by_id(client, group_id_int)
                            if not entity:
                                # Якщо не вдалося отримати entity, використовуємо тільки ID для відображення
                                display_name = f"Група з ID {group_id}"
                                
                                # Проверяем наличие задания в планировщике
                                job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                job = scheduler.get_job(job_id)
                                
                                # Проверяем также статус is_active в базе данных
                                is_active_in_db = cursor.execute(
                                    "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                    (user_id, gid_key(group_id))
                                ).fetchone()
                                
                                # Если есть задание в планировщике или активный статус в БД
                                if job or (is_active_in_db and is_active_in_db[0]):
                                    if job:
                                        scheduler.remove_job(job_id)
                                    
                                    # Обновляем статус в базе данных
                                    cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                  (False, user_id, gid_key(group_id)))
                                    conn.commit()
                                    
                                    msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                    has_stopped = True
                                
                                # Пропускаем дальнейшую обработку этой группы
                                continue
                        except ValueError:
                            # Якщо не можемо перетворити в число, спробуємо використати як є
                            entity = await client.get_entity(group_username)
                except Exception as entity_error:
                    # Якщо не вдалося отримати entity, спробуємо альтернативний метод
                    if "Cannot find any entity corresponding to" in str(entity_error):
                        try:
                            # Спробуємо отримати entity за ID
                            try:
                                group_id_int = int(group_username) if group_username.isdigit() else group_id
                                entity = await get_entity_by_id(client, group_id_int)
                                if not entity:
                                    # Якщо не вдалося отримати entity, використовуємо тільки ID для відображення
                                    display_name = f"Група з ID {group_id}"
                                    
                                    # Проверяем наличие задания в планировщике
                                    job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                    job = scheduler.get_job(job_id)
                                    
                                    # Проверяем также статус is_active в базе данных
                                    is_active_in_db = cursor.execute(
                                        "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                        (user_id, gid_key(group_id))
                                    ).fetchone()
                                    
                                    # Если есть задание в планировщике или активный статус в БД
                                    if job or (is_active_in_db and is_active_in_db[0]):
                                        if job:
                                            scheduler.remove_job(job_id)
                                        
                                        # Обновляем статус в базе данных
                                        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                      (False, user_id, gid_key(group_id)))
                                        conn.commit()
                                        
                                        msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                        has_stopped = True
                                    
                                    # Пропускаем дальнейшую обработку этой группы
                                    continue
                            except ValueError:
                                # Якщо не вдалося перетворити в число, просто зупиняємо задачі
                                display_name = f"Група з ID {group_id}"
                                
                                # Проверяем наличие задания в планировщике
                                job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                job = scheduler.get_job(job_id)
                                
                                # Проверяем также статус is_active в базе данных
                                is_active_in_db = cursor.execute(
                                    "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                    (user_id, gid_key(group_id))
                                ).fetchone()
                                
                                # Если есть задание в планировщике или активный статус в БД
                                if job or (is_active_in_db and is_active_in_db[0]):
                                    if job:
                                        scheduler.remove_job(job_id)
                                    
                                    # Обновляем статус в базе данных
                                    cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                  (False, user_id, gid_key(group_id)))
                                    conn.commit()
                                    
                                    msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                    has_stopped = True
                                
                                # Пропускаем дальнейшую обработку этой группы
                                continue
                        except Exception as alt_error:
                            logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                            
                            # Если все методы не сработали, останавливаем задачу без информации о группе
                            display_name = f"Група з ID {group_id}"
                            
                            # Проверяем наличие задания в планировщике
                            job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                            job = scheduler.get_job(job_id)
                            
                            # Проверяем также статус is_active в базе данных
                            is_active_in_db = cursor.execute(
                                "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                (user_id, gid_key(group_id))
                            ).fetchone()
                            
                            # Если есть задание в планировщике или активный статус в БД
                            if job or (is_active_in_db and is_active_in_db[0]):
                                if job:
                                    scheduler.remove_job(job_id)
                                
                                # Обновляем статус в базе данных
                                cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                              (False, user_id, gid_key(group_id)))
                                conn.commit()
                                
                                msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                has_stopped = True
                            
                            # Пропускаем дальнейшую обработку этой группы
                            continue
                    else:
                        logger.error(f"Ошибка при получении информации о группе: {str(entity_error)}")
                        continue
                
                # Пропускаємо канали-вітрини
                if isinstance(entity, Channel) and entity.broadcast and not entity.megagroup:
                    continue
                
                # Формуємо назву для відображення
                display_name = entity.title if hasattr(entity, 'title') else group_username
                
                # Проверяем наличие задания в планировщике
                job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                job = scheduler.get_job(job_id)
                
                # Проверяем также статус is_active в базе данных
                is_active_in_db = cursor.execute(
                    "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                    (user_id, gid_key(group_id))
                ).fetchone()
                
                # Если есть задание в планировщике или активный статус в БД
                if job or (is_active_in_db and is_active_in_db[0]):
                    if job:
                        scheduler.remove_job(job_id)
                    
                    # Обновляем статус в базе данных
                    cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                  (False, user_id, gid_key(group_id)))
                    conn.commit()
                    
                    msg.append(f"⛔ Рассылка в группу **{display_name}** остановлена.")
                    has_stopped = True
                
            except Exception as e:
                logger.error(f"Ошибка при обработке группы {group_id}: {e}")
                
                # В случае ошибки все равно пытаемся остановить задачу
                try:
                    job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                    job = scheduler.get_job(job_id)
                    
                    # Проверяем также статус is_active в базе данных
                    is_active_in_db = cursor.execute(
                        "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                        (user_id, gid_key(group_id))
                    ).fetchone()
                    
                    # Если есть задание в планировщике или активный статус в БД
                    if job or (is_active_in_db and is_active_in_db[0]):
                        if job:
                            scheduler.remove_job(job_id)
                        
                        # Обновляем статус в базе данных
                        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                      (False, user_id, gid_key(group_id)))
                        conn.commit()
                        
                        msg.append(f"⛔ Рассылка в группу с ID {group_id} остановлена.")
                        has_stopped = True
                except Exception as stop_error:
                    logger.error(f"Критическая ошибка при остановке рассылки: {stop_error}")
                    continue
    
    finally:
        await client.disconnect()
    
    # Если нет остановленных рассылок
    if not has_stopped:
        msg.append("Нет активных рассылок для остановки.")
    
    await event.respond("\n".join(msg))
    cursor.close()


async def schedule_account_broadcast(user_id: int,
                                     text: str,
                                     min_m: int,
                                     max_m: Union[int] = None,
                                     photo_url: Optional[str] = None,
                                     entities_json: Optional[str] = None,
                                     fwd_bot_id: Optional[int] = None,
                                     fwd_msg_ids_json: Optional[str] = None) -> None:
    """Ставит/обновляет jobs broadcastALL_<user>_<gid> только для чатов,
    куда аккаунт реально может писать."""
    # --- сессия ---
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    cursor.execute(
        """UPDATE broadcasts SET broadcast_text = ?, broadcast_entities = ?, broadcast_fwd_bot_id = ?, broadcast_fwd_msg_ids = ? WHERE user_id = ?""",
        (text, entities_json, fwd_bot_id, fwd_msg_ids_json, user_id),
    )
    if not row:
        return
    sess_str = row[0]

    client = TelegramClient(StringSession(sess_str), API_ID, API_HASH)
    await client.connect()
    
    # Сначала удаляем все существующие задания для этого пользователя
    # чтобы избежать дублирования сообщений
    for job in scheduler.get_jobs():
        if job.id.startswith(f"broadcastALL_{user_id}_"):
            scheduler.remove_job(job.id)
            logger.info(f"Удалено существующее задание {job.id}")

    # --- собираем «разрешённые» чаты/каналы ---
    groups = cursor.execute("""SELECT group_username, group_id FROM groups WHERE user_id = ?""", (user_id,)).fetchall()
    ok_entities: list[Channel | Chat] = []
    
    for group in groups:
        try:
            # Пробуем получить объект по username или ID
            group_username = group[0]
            group_id = group[1]
            
            # Проверяем, является ли group_username числом (ID группы) или именем пользователя
            if group_username.startswith('@'):
                # Это username группы
                try:
                    ent = await client.get_entity(group_username)
                except Exception as e:
                    logger.error(f"Не удалось получить entity по username {group_username}: {e}")
                    continue
            else:
                # Пробуем получить entity по ID
                try:
                    group_id_int = int(group_username)
                    ent = await get_entity_by_id(client, group_id_int)
                    if not ent:
                        logger.error(f"Не удалось получить entity для ID {group_username}")
                        continue
                except ValueError:
                    # Если не можем преобразовать в число, пробуем использовать как есть
                    try:
                        ent = await client.get_entity(group_username)
                    except Exception as e:
                        logger.error(f"Не удалось получить entity для {group_username}: {e}")
                        continue
            
            if not isinstance(ent, (Channel, Chat)):
                logger.info(f"пропускаем задачу {ent} так как данный чат Личный диалог или бот")
                continue
            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                logger.info(f"пропускаем задачу {ent} так как данный чат витрина-канал")
                continue
            ok_entities.append(ent)
        except Exception as error:
            logger.warning(f"Не смог проверить: {error}")
            continue

    if not ok_entities:
        logger.info(f"Нету задач выходим")
        return

    sec_run = (((max_m - min_m) / len(ok_entities)) if max_m else min_m)
    current_time = sec_run
    for ent in ok_entities:
        logger.debug(ent)
        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
        interval = (((max_m - min_m) / len(ok_entities)) if max_m else min_m)
        create_broadcast_data(
            user_id, gid_key(ent.id), text, interval, photo_url, entities_json, fwd_bot_id, fwd_msg_ids_json
        )
        if scheduler.get_job(job_id):
            logger.info(f"Удаляем задачу")
            scheduler.remove_job(job_id)

        async def send_message(
                ss: str = sess_str,
                entity: Union[Channel, Chat] = ent,
                jobs_id: str = job_id,
                start_text: str = text,
                start_photo_url: Optional[str] = photo_url,
                max_retries: int = 10
        ) -> None:
            """Отправляет сообщение с обработкой ошибок и повторными попытками."""
            retry_count = 0
            cursor = None

            while retry_count < max_retries:
                try:
                    async with TelegramClient(StringSession(ss), API_ID, API_HASH) as client:
                        cursor = conn.cursor()

                        # Получаем актуальный текст рассылки и фото из базы данных
                        cursor.execute("""SELECT broadcast_text, photo_url, broadcast_entities FROM broadcasts 
                                        WHERE group_id = ? AND user_id = ?""",
                                       (gid_key(entity.id), user_id))
                        current_data = cursor.fetchone()
                        txt = current_data[0] if current_data and current_data[0] else start_text
                        ent_json = current_data[2] if current_data and len(current_data) > 2 else None
                        if not ent_json:
                            txt = normalize_telegram_html(txt)
                        photo_url_from_db = current_data[1] if current_data and len(current_data) > 1 else None
                        photo_to_send = photo_url_from_db if photo_url_from_db else start_photo_url

                        try:
                            txt_preview = ellipsize(strip_html_tags(txt), 200)
                            logger.info(
                                "SEND attempt user_id={} chat_id={} chat_title={!r} job_id={} retry={}/{} photo={} text_preview={!r}",
                                user_id,
                                getattr(entity, "id", None),
                                getattr(entity, "title", None),
                                jobs_id,
                                retry_count + 1,
                                max_retries,
                                bool(photo_to_send),
                                txt_preview,
                            )
                            res = await deliver_broadcast(
                                client, entity, txt, photo_to_send, ent_json, None, None
                            )
                            msg_id = None
                            if isinstance(res, list) and res:
                                msg_id = getattr(res[-1], "id", None)
                            else:
                                msg_id = getattr(res, "id", None)
                            logger.info(
                                "SENT user_id={} chat_id={} msg_id={}",
                                user_id,
                                getattr(entity, "id", None),
                                msg_id,
                            )
                        except Exception as entity_error:
                            if "Cannot find any entity corresponding to" in str(entity_error):
                                logger.info(f"Пробуем получить entity другим способом для {entity.id}")
                                new_entity = await get_entity_by_id(client, entity.id)
                                if new_entity:
                                    await deliver_broadcast(
                                        client, new_entity, txt, photo_to_send, ent_json, None, None
                                    )
                                    logger.info(f"Отправлено через альтернативный метод в {new_entity.title}")
                                    entity = new_entity
                                else:
                                    raise entity_error
                            else:
                                raise entity_error

                        cursor.execute("""INSERT INTO send_history 
                                        (user_id, group_id, group_name, sent_at, message_text) 
                                        VALUES (?, ?, ?, ?, ?)""",
                                       (user_id, entity.id, getattr(entity, 'title', ''),
                                        datetime.datetime.now().isoformat(), txt))
                except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                    logger.error(f"Нет прав писать в {entity.title}: {e}")
                    break
                except (FloodWaitError, SlowModeWaitError) as e:
                    wait_time = e.seconds
                    logger.warning(f"{type(e).__name__}: ожидание {wait_time} сек.")
                    await asyncio.sleep(wait_time + 10)
                    retry_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при отправке в {entity.title}: {type(e).__name__}: {e}")
                    retry_count += 1
                    await asyncio.sleep(5)
                else:
                    return

            logger.warning(f"Не удалось отправить в {entity.title} после {max_retries} попыток")
            with conn:
                cursor = conn.cursor()
                cursor.execute("""UPDATE broadcasts 
                                SET is_active = ? 
                                WHERE user_id = ? AND group_id = ?""",
                               (False, user_id, gid_key(entity.id)))
                if scheduler.get_job(jobs_id):
                    scheduler.remove_job(jobs_id)

        base = (min_m + max_m) // 2 if max_m else min_m
        jitter = (max_m - min_m) * 60 // 2 if max_m else 0
        trigger = IntervalTrigger(minutes=base, jitter=jitter)
        next_run = datetime.datetime.now() + datetime.timedelta(minutes=current_time)
        logger.info(f"Добавляем задачу отправить сообщения в {ent.title} в {next_run.isoformat()}")
        scheduler.print_jobs()
        scheduler.add_job(
            send_message,
            trigger,
            id=job_id,
            next_run_time=next_run,
            replace_existing=True,
        )
        logger.info(f"Создано новое задание {job_id} для группы {ent.title} (@{getattr(ent, 'username', 'без username')})")
        current_time += sec_run
    if not scheduler.running:
        logger.info("Запускаем все задачи")
        scheduler.start()

    await client.disconnect()
    cursor.close()
    
    if not ok_entities:
        return
