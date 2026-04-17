import asyncio
import datetime
from loguru import logger
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import Button, TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from config import callback_query, callback_message, API_ID, API_HASH, scheduler, Query, bot, conn, \
    New_Message, broadcast_all_state_account, ADMIN_ID_LIST
from utils.telegram import gid_key, create_broadcast_data, get_active_broadcast_groups, get_entity_by_id, send_broadcast_formatted, event_message_has_image
from utils.logging import log_message_event, log_user_action
from utils.text import normalize_telegram_html, event_to_broadcast_payload, strip_html_tags, ellipsize
from utils.access import is_admin
from utils.reply_nav import maybe_install_reply_nav_keyboard


@bot.on(Query(data=lambda d: d.decode().startswith("broadcast_All_account")))
async def broadcast_all_menu(event: callback_query) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Только для администратора.", alert=True)
        return
    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", f"same_IntervalAll_account")],
        [Button.inline("🎲 Разный интервал (25-35)", f"diff_IntervalAll_account")],
    ]
    await event.respond("<b>Рассылка по всем аккаунтам</b>\n\nВыберите режим:", buttons=keyboard)
    await maybe_install_reply_nav_keyboard(event)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("same_IntervalAll_account")))
async def same_interval_start(event: callback_query) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Только для администратора.", alert=True)
        return
    admin_id = event.sender_id
    broadcast_all_state_account[admin_id] = {"mode": "same", "step": "text"}
    await event.respond(
        "📝 <b>Сообщение для рассылки</b> по всем группам <b>всех</b> аккаунтов — пришлите боту.\n"
        "<i>Режим «все аккаунты»: в группы уходит копия текста/медиа (у каждого аккаунта свой чат с ботом).</i>"
    )


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diff_IntervalAll_account")))
async def diff_interval_start(event: callback_query) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Только для администратора.", alert=True)
        return
    admin_id = event.sender_id
    broadcast_all_state_account[admin_id] = {"mode": "diff", "step": "text"}
    await event.respond(
        "📝 <b>Сообщение для рассылки</b> — пришлите боту, затем спрошу интервал.\n"
        "<i>Режим «все аккаунты»: копия в группы (не пересылка).</i>"
    )


@bot.on(Query(data=lambda d: d.decode() == "bc_aam_home"))
async def all_accounts_wizard_home(event: callback_query) -> None:
    """Старые кнопки «В главное меню»; для админа открывает ту же панель, что и /start."""
    broadcast_all_state_account.pop(event.sender_id, None)
    await event.answer()
    if event.sender_id in ADMIN_ID_LIST:
        from handlers.admin.admin_handlers import deliver_admin_home

        await deliver_admin_home(event)
    else:
        await event.respond("Вы вышли из мастера. Отправьте команду <b>/start</b>.")


@bot.on(Query(data=lambda d: d.decode() == "bc_aam_msg"))
async def all_accounts_wizard_back_message(event: callback_query) -> None:
    uid = event.sender_id
    if uid not in broadcast_all_state_account:
        await event.answer("Мастер не активен.", alert=True)
        return
    st = broadcast_all_state_account[uid]
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
            buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_aam_msg")]],
        )
        return
    await event.answer("Назад недоступен.", alert=True)


@bot.on(Query(data=lambda d: d.decode() == "bc_aam_int"))
async def all_accounts_wizard_back_interval(event: callback_query) -> None:
    uid = event.sender_id
    if uid not in broadcast_all_state_account:
        await event.answer("Мастер не активен.", alert=True)
        return
    st = broadcast_all_state_account[uid]
    if st.get("step") != "photo_choice":
        await event.answer("Назад недоступен.", alert=True)
        return
    if st.get("mode") == "same":
        st["step"] = "interval"
        st.pop("min_time", None)
        await event.answer()
        await event.respond(
            "⏲️ Снова введите интервал (минуты, одно число):",
            buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_aam_msg")]],
        )
        return
    if st.get("mode") == "diff":
        st["step"] = "max"
        st.pop("max_m", None)
        await event.answer()
        await event.respond(
            "🔢 Снова введите максимальный интервал (мин):",
            buttons=[[Button.inline("◀️ К мин. интервалу", b"bc_aam_msg")]],
        )
        return
    await event.answer("Назад недоступен.", alert=True)


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state_account))
async def broadcast_all_dialog(event: callback_message) -> None:
    if not is_admin(event.sender_id):
        broadcast_all_state_account.pop(event.sender_id, None)
        await event.respond("⚠ Этот режим доступен только администратору.")
        return
    st = broadcast_all_state_account[event.sender_id]
    log_message_event(event, "обработка диалога рассылки по аккаунтам")
    if st.get("step") == "photo_choice":
        await event.respond("⚠ Сначала нажмите кнопку под вопросом про фото (или /start).")
        return
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
                logger.error(f"download_media (all_account text): {e}")
                await event.respond("⚠ Не удалось сохранить изображение. Отправьте сообщение без фото или повторите.")
                return
        if st["mode"] == "same":
            st["step"] = "interval"
            await event.respond(
                "⏲️ Введите интервал (минуты, одно число):",
                buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_aam_msg")]],
            )
        else:
            st["step"] = "min"
            await event.respond(
                "🔢 Минимальный интервал (мин):",
                buttons=[[Button.inline("◀️ Изменить сообщение", b"bc_aam_msg")]],
            )
        return

    if st["mode"] == "same" and st["step"] == "interval":
        try:
            min_time = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return

        st["min_time"] = min_time
        if st.get("embedded_photo_path"):
            await schedule_all_accounts_broadcast(
                st["text"], st["min_time"], None, st["embedded_photo_path"], st.get("entities_json")
            )
            await event.respond(
                f"✅ Запустил: каждые {st['min_time']} мин (фото и текст из вашего сообщения)."
            )
            broadcast_all_state_account.pop(event.sender_id, None)
            return

        st["step"] = "photo_choice"
        buttons = [
            [Button.inline("✅ Да, другое фото к тексту", b"photo_yes_all_account")],
            [Button.inline("📸 Только изображение", b"photo_only_all_account")],
            [Button.inline("❌ Нет, как в сообщении выше", b"photo_no_all_account")],
            [Button.inline("◀️ К интервалу", b"bc_aam_int")],
        ]
        await event.respond(
            "📸 Нужно ли отдельное фото сверх сообщения?\n(Если фото уже в сообщении — «Нет».)",
            buttons=buttons,
        )
        return
        
    if st["step"] == "photo" or st["step"] == "photo_only":
        if event.photo or event_message_has_image(event):
            try:
                photo = await event.download_media()
                if st["mode"] == "same":
                    await schedule_all_accounts_broadcast(
                        st["text"], st["min_time"], None, photo, st.get("entities_json")
                    )
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    await event.respond(f"✅ Запустил: каждые {st['min_time']} мин {message_type}.")
                else:
                    await schedule_all_accounts_broadcast(
                        st["text"], st["min"], st["max_m"], photo, st.get("entities_json")
                    )
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{st['max_m']} мин {message_type}.")
                broadcast_all_state_account.pop(event.sender_id, None)
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
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if st["min"] <= 0:
            await event.respond("⚠ Минимальное число должно быть больше нуля.")
            return
        st["step"] = "max"
        await event.respond(
            "🔢 Максимальный интервал (мин):",
            buttons=[[Button.inline("◀️ К мин. интервалу", b"bc_aam_msg")]],
        )
        return

    # случайный интервал — шаг 3 (max)
    if st["mode"] == "diff" and st["step"] == "max":
        try:
            max_m = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if max_m <= st["min"]:
            await event.respond("⚠ Максимальное число должно быть больше минимального числа.")
            return

        st["max_m"] = max_m
        if st.get("embedded_photo_path"):
            await schedule_all_accounts_broadcast(
                st["text"], st["min"], st["max_m"], st["embedded_photo_path"], st.get("entities_json")
            )
            await event.respond(
                f"✅ Запустил: случайно каждые {st['min']}-{st['max_m']} мин (фото и текст из вашего сообщения)."
            )
            broadcast_all_state_account.pop(event.sender_id, None)
            return

        st["step"] = "photo_choice"
        buttons = [
            [Button.inline("✅ Да, другое фото к тексту", b"photo_yes_all_account")],
            [Button.inline("📸 Только изображение", b"photo_only_all_account")],
            [Button.inline("❌ Нет, как в сообщении выше", b"photo_no_all_account")],
            [Button.inline("◀️ К интервалу", b"bc_aam_int")],
        ]
        await event.respond(
            "📸 Нужно ли отдельное фото сверх сообщения?\n(Если фото уже в сообщении — «Нет».)",
            buttons=buttons,
        )
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes_all_account"))
async def photo_yes_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state_account:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state_account[user_id]
    st["step"] = "photo"
    
    await event.respond("📤 Отправьте изображение для отправки вместе с текстом:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only_all_account"))
async def photo_only_all_account_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state_account:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state_account[user_id]
    st["step"] = "photo_only"
    st["text"] = ""
    st["entities_json"] = None
    st.pop("embedded_photo_path", None)

    await event.respond("📤 Отправьте изображение (с подписью или без):")


@bot.on(Query(data=lambda d: d.decode() == "photo_no_all_account"))
async def photo_no_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state_account:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state_account[user_id]
    
    ent = st.get("entities_json")
    if st["mode"] == "same":
        await schedule_all_accounts_broadcast(
            st["text"], st["min_time"], None, st.get("embedded_photo_path"), ent
        )
        await event.respond(f"✅ Запущено: каждые {st['min_time']} мин.")
    else:
        await schedule_all_accounts_broadcast(
            st["text"], st["min"], st["max_m"], st.get("embedded_photo_path"), ent
        )
        await event.respond(f"✅ Запущено: случайно каждые {st['min']}-{st['max_m']} мин.")
    
    broadcast_all_state_account.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode() == "Stop_Broadcast_All_account"))
async def stop_broadcast_all(event: callback_query) -> None:
    """Останавливает все активные рассылки для всех аккаунтов и групп"""
    if not is_admin(event.sender_id):
        await event.answer("Только для администратора.", alert=True)
        return
    msg_lines = ["⛔ **Остановленные рассылки**:\n\n"]
    processed_accounts = []

    with conn:
        cursor = conn.cursor()
        try:
            sessions = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()

            for user_id, session_string in sessions:
                # Проверяем, не обрабатывали ли мы уже этот аккаунт
                if user_id in processed_accounts:
                    continue
                processed_accounts.append(user_id)
                
                async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                    try:
                        await client.connect()
                        account = await client.get_me()
                        username = getattr(account, 'username', 'без username')
                        account_name = account.first_name if hasattr(account, 'first_name') and account.first_name else username
                        
                        # Получаем активные группы для этого аккаунта
                        active_groups = get_active_broadcast_groups(user_id)
                        
                        if not active_groups:
                            continue
                        
                        # Добавляем информацию об аккаунте только если есть активные группы
                        account_msg = [f"**Аккаунт {account_name}**:\n"]
                        has_stopped_jobs = False
                        
                        # Получаем информацию о группах для отображения названий вместо ID
                        group_info = {}
                        groups_data = cursor.execute("""
                            SELECT group_id, group_username FROM groups WHERE user_id = ?
                        """, (user_id,)).fetchall()
                        
                        for g_id, g_username in groups_data:
                            group_info[g_id] = g_username
                        
                        # Обрабатываем каждую активную группу
                        for group_id in active_groups:
                            # Пробуем получить информацию о группе
                            try:
                                group_username = group_info.get(group_id, str(group_id))
                                
                                # Пробуем получить entity группы
                                try:
                                    # Проверяем, это username или ID
                                    if group_username.startswith('@'):
                                        # Это username группы
                                        group_entity = await client.get_entity(group_username)
                                    else:
                                        # Спробуємо отримати entity за ID
                                        try:
                                            group_id_int = int(group_username)
                                            group_entity = await get_entity_by_id(client, group_id_int)
                                            if not group_entity:
                                                # Если не удалось получить entity, используем только ID для отображения
                                                display_name = f"Група з ID {group_id}"
                                                
                                                # Проверяем и останавливаем задачи
                                                job_stopped = False
                                                job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                                job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                                
                                                if scheduler.get_job(job_id_all):
                                                    scheduler.remove_job(job_id_all)
                                                    job_stopped = True
                                                
                                                if scheduler.get_job(job_id_solo):
                                                    scheduler.remove_job(job_id_solo)
                                                    job_stopped = True
                                                
                                                # Обновляем статус в базе данных
                                                cursor.execute(
                                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                    (False, user_id, gid_key(group_id)))
                                                
                                                # Добавляем сообщение о результате
                                                if job_stopped:
                                                    account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                    has_stopped_jobs = True
                                                
                                                # Пропускаем дальнейшую обработку этой группы
                                                continue
                                        except ValueError:
                                            # Если не можем преобразовать в число, попробуем использовать как есть
                                            group_entity = await client.get_entity(group_username)
                                except Exception as entity_error:
                                    # Якщо не вдалося отримати entity, спробуємо альтернативний метод
                                    if "Cannot find any entity corresponding to" in str(entity_error):
                                        try:
                                            # Спробуємо отримати entity за ID
                                            try:
                                                group_id_int = int(group_username) if group_username.isdigit() else group_id
                                                group_entity = await get_entity_by_id(client, group_id_int)
                                                if not group_entity:
                                                    # Если не удалось получить entity, используем только ID для отображения
                                                    display_name = f"Група з ID {group_id}"
                                                    
                                                    # Проверяем и останавливаем задачи
                                                    job_stopped = False
                                                    job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                                    job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                                    
                                                    if scheduler.get_job(job_id_all):
                                                        scheduler.remove_job(job_id_all)
                                                        job_stopped = True
                                                    
                                                    if scheduler.get_job(job_id_solo):
                                                        scheduler.remove_job(job_id_solo)
                                                        job_stopped = True
                                                    
                                                    # Обновляем статус в базе данных
                                                    cursor.execute(
                                                        "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                        (False, user_id, gid_key(group_id)))
                                                    
                                                    # Добавляем сообщение о результате
                                                    if job_stopped:
                                                        account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                        has_stopped_jobs = True
                                                    
                                                    # Пропускаем дальнейшую обработку этой группы
                                                    continue
                                            except ValueError:
                                                # Якщо не вдалося перетворити в число, просто зупиняємо задачі
                                                display_name = f"Група з ID {group_id}"
                                                
                                                # Проверяем и останавливаем задачи
                                                job_stopped = False
                                                job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                                job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                                
                                                if scheduler.get_job(job_id_all):
                                                    scheduler.remove_job(job_id_all)
                                                    job_stopped = True
                                                
                                                if scheduler.get_job(job_id_solo):
                                                    scheduler.remove_job(job_id_solo)
                                                    job_stopped = True
                                                
                                                # Обновляем статус в базе данных
                                                cursor.execute(
                                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                    (False, user_id, gid_key(group_id)))
                                                
                                                # Добавляем сообщение о результате
                                                if job_stopped:
                                                    account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                    has_stopped_jobs = True
                                                
                                                # Пропускаем дальнейшую обработку этой группы
                                                continue
                                        except Exception as alt_error:
                                            logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                                            
                                            # Если все методы не сработали, останавливаем задачу без информации о группе
                                            display_name = f"Група з ID {group_id}"
                                            
                                            # Проверяем и останавливаем задачи
                                            job_stopped = False
                                            job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                            job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                            
                                            if scheduler.get_job(job_id_all):
                                                scheduler.remove_job(job_id_all)
                                                job_stopped = True
                                            
                                            if scheduler.get_job(job_id_solo):
                                                scheduler.remove_job(job_id_solo)
                                                job_stopped = True
                                            
                                            # Обновляем статус в базе данных
                                            cursor.execute(
                                                "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                (False, user_id, gid_key(group_id)))
                                            
                                            # Добавляем сообщение о результате
                                            if job_stopped:
                                                account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                has_stopped_jobs = True
                                            
                                            # Пропускаем дальнейшую обработку этой группы
                                            continue
                                    else:
                                        logger.error(f"Ошибка при получении информации о группе: {str(entity_error)}")
                                        continue
                                
                                # Пропускаем каналы-витрины
                                if isinstance(group_entity, Channel) and group_entity.broadcast and not group_entity.megagroup:
                                    logger.info(f"Пропускаємо канал {group_username}")
                                    continue
                                
                                # Формируем название для отображения
                                display_name = group_entity.title if hasattr(group_entity, 'title') else group_username
                                
                                # Проверяем и останавливаем задачи
                                job_stopped = False
                                job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                
                                if scheduler.get_job(job_id_all):
                                    scheduler.remove_job(job_id_all)
                                    job_stopped = True
                                
                                if scheduler.get_job(job_id_solo):
                                    scheduler.remove_job(job_id_solo)
                                    job_stopped = True
                                
                                # Обновляем статус в базе данных
                                cursor.execute(
                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                    (False, user_id, gid_key(group_id)))
                                
                                # Добавляем сообщение о результате
                                if job_stopped:
                                    account_msg.append(f"⛔ Рассылка в группу **{display_name}** остановлена.\n")
                                    has_stopped_jobs = True
                                
                            except Exception as e:
                                logger.error(f"Ошибка при обработке группы {group_id}: {str(e)}")
                                
                                # В случае ошибки все равно пытаемся остановить задачу
                                try:
                                    job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                    job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                    job_stopped = False
                                    
                                    if scheduler.get_job(job_id_all):
                                        scheduler.remove_job(job_id_all)
                                        job_stopped = True
                                    
                                    if scheduler.get_job(job_id_solo):
                                        scheduler.remove_job(job_id_solo)
                                        job_stopped = True
                                    
                                    # Обновляем статус в базе данных
                                    cursor.execute(
                                        "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                        (False, user_id, gid_key(group_id)))
                                    
                                    if job_stopped:
                                        account_msg.append(f"⛔ Рассылка в группу с ID {group_id} остановлена.\n")
                                        has_stopped_jobs = True
                                except Exception as stop_error:
                                    logger.error(f"Критическая ошибка при остановке рассылки: {stop_error}")
                                    continue
                        
                        # Добавляем сообщение об аккаунте только если были остановлены задачи
                        if has_stopped_jobs:
                            msg_lines.extend(account_msg)
                        
                    except Exception as e:
                        logger.error(f"Ошибка при обработке аккаунта {user_id}: {str(e)}")
                        msg_lines.append(f"⚠ Ошибка при обработке аккаунта {user_id}\n")
            
            # Если нет сообщений об остановленных рассылках, добавляем информационное сообщение
            if len(msg_lines) == 1:  # Только заголовок
                msg_lines.append("Нет активных рассылок для остановки.")
            
            await event.respond("".join(msg_lines))

        finally:
            cursor.close()


async def schedule_all_accounts_broadcast(text: str,
                                          min_m: int,
                                          max_m: Optional[int] = None,
                                          photo_url: Optional[str] = None,
                                          entities_json: Optional[str] = None) -> None:
    """Планирует/обновляет задачи рассылки broadcastALL_<user>_<gid> только для чатов,
    куда пользователь действительно может писать."""

    with conn:
        cursor = conn.cursor()
        try:
            users = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()

            for user_id, session_string in users:
                cursor.execute(
                    """UPDATE broadcasts SET broadcast_text = ?, broadcast_entities = ?, broadcast_fwd_bot_id = NULL, broadcast_fwd_msg_ids = NULL WHERE user_id = ?""",
                    (text, entities_json, user_id),
                )

                async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                    await client.connect()

                    groups = cursor.execute("""SELECT group_username, group_id FROM groups 
                                            WHERE user_id = ?""", (user_id,)).fetchall()

                    ok_entities: list[Channel | Chat] = []
                    for group_username, group_id in groups:
                        try:
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

                            # Проверяем тип чата
                            if not isinstance(ent, (Channel, Chat)):
                                logger.info(f"Пропускаем {ent} - не чат/канал")
                                continue

                            # Пропускаем каналы-витрины
                            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                                logger.info(f"Пропускаем {ent} - витрина-канал")
                                continue

                            ok_entities.append(ent)
                        except Exception as error:
                            logger.warning(f"Не смог проверить {group_username}: {error}")
                            continue

                    if not ok_entities:
                        continue

                    total_entities = len(ok_entities)
                    sec_run = ((max_m - min_m) / total_entities) if max_m else min_m
                    current_time = sec_run

                    for ent in ok_entities:
                        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
                        interval = ((max_m - min_m) / total_entities) if max_m else min_m

                        create_broadcast_data(user_id, gid_key(ent.id), text, interval, photo_url, entities_json, None, None)

                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)

                        async def send_message(
                                ss: str = session_string,
                                entity: Union[Channel, Chat] = ent,
                                jobs_id: str = job_id,
                                start_text: str = text,
                                start_photo_url: Optional[str] = photo_url,
                                max_retries: int = 10
                        ) -> None:
                            """Отправляет сообщение с обработкой ошибок и повторными попытками."""
                            retry_count = 0

                            while retry_count < max_retries:
                                try:
                                    async with TelegramClient(StringSession(ss), API_ID, API_HASH) as client:
                                        with conn:
                                            cursor = conn.cursor()
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
                                                    "SEND all_account user_id={} chat_id={} job_id={} photo={} text_preview={!r}",
                                                    user_id,
                                                    getattr(entity, "id", None),
                                                    jobs_id,
                                                    bool(photo_to_send),
                                                    txt_preview,
                                                )
                                                if photo_to_send:
                                                    try:
                                                        await send_broadcast_formatted(
                                                            client, entity, txt, photo_to_send, ent_json
                                                        )
                                                        logger.debug(f"Отправлено сообщение с фото в {entity.title}")
                                                    except Exception as photo_error:
                                                        logger.error(f"Ошибка при отправке с фото: {photo_error}")
                                                        await send_broadcast_formatted(client, entity, txt, None, ent_json)
                                                        logger.debug(f"Отправлено сообщение без фото в {entity.title}")
                                                else:
                                                    await send_broadcast_formatted(client, entity, txt, None, ent_json)
                                                    logger.debug(f"Успешно отправлено в {entity.title}")
                                            except Exception as entity_error:
                                                if "Cannot find any entity corresponding to" in str(entity_error):
                                                    logger.info(f"Пробуем получить entity другим способом для {entity.id}")
                                                    new_entity = await get_entity_by_id(client, entity.id)
                                                    if new_entity:
                                                        if photo_to_send:
                                                            try:
                                                                await send_broadcast_formatted(
                                                                    client, new_entity, txt, photo_to_send, ent_json
                                                                )
                                                                logger.debug(f"Отправлено с фото (alt) в {new_entity.title}")
                                                            except Exception as alt_photo_error:
                                                                logger.error(f"Ошибка alt фото: {alt_photo_error}")
                                                                await send_broadcast_formatted(
                                                                    client, new_entity, txt, None, ent_json
                                                                )
                                                                logger.debug(f"Отправлено без фото (alt) в {new_entity.title}")
                                                        else:
                                                            await send_broadcast_formatted(
                                                                client, new_entity, txt, None, ent_json
                                                            )
                                                            logger.debug(f"Отправлено через альтернативный метод в {new_entity.title}")
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
                        jitter = (max_m - min_m) * 60 // 2 if max_m else min_m * 30
                        trigger = IntervalTrigger(minutes=base, jitter=jitter)
                        next_run = datetime.datetime.now() + datetime.timedelta(minutes=current_time)

                        logger.info(f"Добавляем задачу для {ent.title} на {next_run.isoformat()}")
                        scheduler.add_job(
                            send_message,
                            trigger,
                            id=job_id,
                            next_run_time=next_run,
                            replace_existing=True,
                        )
                        current_time += sec_run
        finally:
            cursor.close()

    if not scheduler.running:
        logger.info("Запускаем планировщик задач")
        scheduler.start()
