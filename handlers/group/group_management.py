from loguru import logger
from typing import List, Optional, Union

from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel, Chat, DialogFilter

from config import callback_query, API_ID, API_HASH, Query, bot, conn, processed_callbacks
from utils.telegram import broadcast_status_emoji, gid_key, get_entity_by_id
from utils.access import can_manage_session
from utils.reply_nav import maybe_install_reply_nav_keyboard, nav_append


async def send_groups_list_ui(event, user_id: int) -> None:
    """Список групп с живыми названиями из Telegram (callback или reply «Назад»)."""
    if not can_manage_session(event.sender_id, user_id):
        await event.respond("⚠ Нет доступа к этому аккаунту.")
        await maybe_install_reply_nav_keyboard(event)
        return

    cursor = conn.cursor()
    session_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()

    if not session_row:
        await event.respond("⚠ Ошибка: не найдена сессия для этого аккаунта.")
        cursor.close()
        await maybe_install_reply_nav_keyboard(event)
        return

    session_string = session_row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)

    try:
        await client.connect()

        cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))
        groups = cursor.fetchall()

        if not groups:
            await event.respond(
                "<b>📋 Группы</b>\n\nЧатов в базе нет. Добавьте их из карточки аккаунта.",
            )
            return

        group_list = []

        for group_id, group_username in groups:
            try:
                try:
                    ent = await client.get_entity(group_username)
                except Exception as entity_error:
                    if "Cannot find any entity corresponding to" in str(entity_error):
                        try:
                            try:
                                group_id_int = int(group_username)
                                ent = await get_entity_by_id(client, group_id_int)
                                if not ent:
                                    logger.error(f"Не удалось получить entity для группы {group_username}")
                                    continue
                            except ValueError:
                                logger.error(f"Не удалось получить entity для группы {group_username}")
                                continue
                        except Exception as alt_error:
                            logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                            continue
                    else:
                        logger.error(f"Ошибка при получении entity для группы {group_username}: {entity_error}")
                        continue

                status = broadcast_status_emoji(user_id, group_id)

                group_name = getattr(ent, "title", group_username)

                group_list.append((gid_key(group_id), group_name, status))
            except Exception as e:
                logger.error(f"Ошибка при обработке группы {group_username}: {e}")
                continue

        if group_list:
            buttons = []
            for gid, group_name, status in group_list:
                data = f"groupInfo_{user_id}_{gid}".encode()
                buttons.append([Button.inline(f"{status} {group_name}", data)])

            await event.respond(
                "<b>📋 Ваши группы</b>\n\n"
                "Статус слева: <b>🟢 Рассылка вкл.</b> или <b>⚪ Рассылка выкл.</b>\n"
                "Выберите чат, чтобы настроить текст, интервал и запуск.",
                buttons=buttons,
            )
        else:
            await event.respond(
                "⚠ Не удалось получить информацию о группах. Возможно, чаты удалены или недоступны.",
            )

    except Exception as e:
        logger.error(f"Ошибка при получении списка групп: {e}")
        await event.respond(f"⚠ Ошибка при получении списка групп: {str(e)}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
    await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=lambda d: d.decode().startswith("account_")))
async def account_menu(event: callback_query) -> None:
    """Обрабатывает нажатие кнопки "Назад" в списке групп и возвращает к меню аккаунта."""
    data = event.data.decode()
    parts = data.split("_")

    if len(parts) < 2:
        await event.respond("⚠ Ошибка: неверный формат данных")
        return

    if parts[1] == "info":
        if len(parts) < 3:
            await event.respond("⚠ Ошибка: неверный формат данных")
            return
        try:
            user_id = int(parts[2])
        except ValueError:
            await event.respond("⚠ Ошибка: неверный ID пользователя")
            return
    else:
        try:
            user_id = int(parts[1])
        except ValueError:
            await event.respond("⚠ Ошибка: неверный ID пользователя")
            return

    if not can_manage_session(event.sender_id, user_id):
        await event.respond("⚠ Нет доступа к этому аккаунту.")
        return

    callback_id = f"{event.sender_id}:{event.query.msg_id}"
    if callback_id in processed_callbacks:
        return
    processed_callbacks[callback_id] = True

    buttons = [
        [Button.inline("📋 Группы", f"groups_{user_id}".encode())],
        [
            Button.inline("🚀 Рассылка во все чаты", f"broadcastAll_{user_id}".encode()),
            Button.inline("⏹ Стоп", f"StopBroadcastAll_{user_id}".encode()),
        ],
    ]

    await event.respond(
        "<b>📱 Быстрые действия</b>\n\n"
        "Полная карточка аккаунта (имя, телефон, список чатов) — в "
        "<b>«Мои аккаунты»</b> → выберите аккаунт.\n"
        "Здесь — только переход к группам и массовой рассылке.",
        buttons=buttons,
    )


@bot.on(Query(data=b"my_groups"))
async def my_groups(event: callback_query) -> None:
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{event.query.msg_id}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    cursor = conn.cursor()
    cursor.execute("SELECT group_id, group_username FROM groups")
    groups = cursor.fetchall()
    cursor.close()
    message = "❌ У вас нет добавленных групп."
    buttons = []
    if groups:
        message = "📑 <b>Список добавленных групп:</b>\n"
        buttons.append([Button.inline("➕ Добавить все аккаунты в эти группы", b"add_all_accounts_to_groups")])
        buttons.append([Button.inline("❌ Удалить группу", b"delete_group")])
        for group in groups:
            message += f"{group[1]}\n"
    await event.respond(message, buttons=buttons)


@bot.on(Query(data=b"add_all_accounts_to_groups"))
async def add_all_accounts_to_groups(event: callback_query) -> None:
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{event.query.msg_id}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, session_string FROM sessions")
    accounts = cursor.fetchall()

    cursor.execute("SELECT group_id, group_username FROM groups")
    groups = cursor.fetchall()
    if not accounts:
        await event.respond("❌ Нет добавленных аккаунтов.")
        return

    if not groups:
        await event.respond("❌ Нет добавленных групп.")
        return

    for account in accounts:
        session = StringSession(account[1])
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
        try:
            for group in groups:
                try:
                    await client(JoinChannelRequest(group[1]))
                except Exception as e:
                    logger.error(f"Ошибка {e}")
                cursor.execute("""INSERT OR IGNORE INTO groups 
                                        (user_id, group_id, group_username) 
                                        VALUES (?, ?, ?)""", (account[0], group[0], group[1]))
                logger.info(f"Добавляем в базу данных группу ({account[0], group[0], group[1]})")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при добавлении аккаунта: {e}")
        finally:
            await client.disconnect()
    group_list = "\n".join([f"📌 {group[1]}" for group in groups])
    await event.respond(f"✅ Аккаунты успешно добавлены в следующие группы:\n{group_list}")
    conn.commit()
    cursor.close()


@bot.on(Query(data=lambda event: event.decode().startswith("add_all_groups_")))
async def add_all_groups_to_account(event: callback_query) -> None:
    data: str = event.data.decode()
    user_id = int(data.split("_")[3])
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return

    callback_id = f"{event.sender_id}:{event.query.msg_id}"
    if callback_id in processed_callbacks:
        return
    processed_callbacks[callback_id] = True

    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id, ))
    accounts = cursor.fetchall()
    if not accounts:
        await event.respond("❌ Нет добавленных аккаунтов.")
        return
    msg = ["✅ <b>Добавленные группы:</b>\n"]
    num = 1
    session = StringSession(accounts[0][0])
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()
    cursor.execute("DELETE FROM groups WHERE user_id = ?", (user_id,))
    conn.commit()
    
    # Создаем множества для отслеживания уникальных групп
    added_group_ids = set()
    added_group_names = set()  # Добавляем отслеживание по названиям
    
    # Сначала собираем все диалоги
    all_dialogs = await client.get_dialogs()
    
    # Сортируем их: сначала каналы (с username), потом группы, потом приватные группы
    sorted_dialogs = sorted(all_dialogs, key=lambda d: (
        not isinstance(d.entity, Channel),  # Сначала каналы
        not (isinstance(d.entity, Channel) and d.entity.username),  # Потом с username
        d.name  # Потом по названию
    ))
    
    for group in sorted_dialogs:
        ent = group.entity
        logger.info(f"Анализируем группу: {group.name}, тип: {type(ent)}")
        
        # Пропускаем, если это не группа или канал
        if not isinstance(ent, (Channel, Chat)):
            continue
            
        # Пропускаем, если это приватный чат или бот
        if hasattr(ent, 'bot') and ent.bot:
            continue
            
        # Пропускаем, если это канал-витрина (не мегагруппа)
        if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
            continue
            
        # Пропускаем, если эта группа уже была добавлена (по ID или названию)
        if ent.id in added_group_ids or group.name in added_group_names:
            logger.info(f"Пропускаем дубликат: {group.name}")
            continue
            
        # Добавляем ID и название к множествам отслеживания
        added_group_ids.add(ent.id)
        added_group_names.add(group.name)
        
        # Определяем username или ID для сохранения
        if isinstance(ent, Channel) and ent.username:
            group_username = f"@{ent.username}"
            cursor.execute(f"""INSERT INTO groups 
                            (group_id, group_username, user_id, group_title) 
                            VALUES (?, ?, ?, ?)""", (ent.id, group_username, user_id, group.name))
            msg.append(f"№{num} <b>{group.name}</b> - <code>{group_username}</code>")
        else:
            # Для групп без username используем ID
            # Сохраняем ID как строку для приватных групп
            group_id_str = str(ent.id)
            cursor.execute(f"""INSERT INTO groups 
                            (group_id, group_username, user_id, group_title) 
                            VALUES (?, ?, ?, ?)""", (ent.id, group_id_str, user_id, group.name))
            msg.append(f"№{num} <b>{group.name}</b> (приватная, ID: <code>{group_id_str}</code>)")
            
        conn.commit()
        num += 1
        
    conn.commit()
    cursor.close()
    await client.disconnect()
    await event.respond("\n".join(msg))
    await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=lambda d: d.decode().startswith("groups_")))
async def groups_list(event: callback_query) -> None:
    """Отображает список групп пользователя."""
    data = event.data.decode()
    user_id = int(data.split("_")[1])
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return

    callback_id = f"{event.sender_id}:{event.query.msg_id}"

    if callback_id in processed_callbacks:
        return

    processed_callbacks[callback_id] = True

    nav_append(event.sender_id, f"G:{user_id}")
    await send_groups_list_ui(event, user_id)
