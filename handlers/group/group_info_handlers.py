from loguru import logger
import os

from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

from config import callback_query, API_ID, API_HASH, Query, bot, conn
from utils.telegram import gid_key, broadcast_status_emoji, get_entity_by_id
from utils.text import strip_html_tags, ellipsize
from utils.access import can_manage_session
from utils.reply_nav import maybe_install_reply_nav_keyboard, nav_append


@bot.on(Query(data=lambda data: data.decode().startswith("listOfgroups_")))
async def handle_groups_list(event: callback_query) -> None:
    user_id = int(event.data.decode().split("_")[1])
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    nav_append(event.sender_id, f"G:{user_id}")
    cursor = conn.cursor()
    dialogs = cursor.execute(
        "SELECT group_id, group_title, group_username FROM groups WHERE user_id = ? ORDER BY group_title",
        (user_id,),
    )
    buttons = []
    for group_id, group_title, group_username in dialogs:
        title = group_title or group_username or f"ID {group_id}"
        buttons.append(
            [
                Button.inline(
                    f"{broadcast_status_emoji(user_id, int(group_id))} {title}",
                    f"groupInfo_{user_id}_{gid_key(group_id)}".encode(),
                )
            ]
        )

    cursor.close()
    if not buttons:
        await event.respond("<b>📋 Группы</b>\n\nУ этого аккаунта пока нет сохранённых чатов.")
        await maybe_install_reply_nav_keyboard(event)
        return

    await event.respond(
        "<b>📋 Группы аккаунта</b>\n\n"
        "Ниже — чаты из базы. Слева от названия — краткий статус рассылки для этой группы.\n"
        "Нажмите строку, чтобы открыть настройки чата.",
        buttons=buttons,
    )
    await maybe_install_reply_nav_keyboard(event)


async def send_group_info_ui(event, user_id: int, group_id: int) -> None:
    """Карточка группы (inline-действия; «Назад» — reply-клавиатура)."""
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
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)

    group_row = cursor.execute(
        "SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?",
        (user_id, group_id),
    ).fetchone()
    if not group_row:
        await event.respond("⚠ Ошибка: не найдена группа.")
        cursor.close()
        await maybe_install_reply_nav_keyboard(event)
        return

    group_username = group_row[0]

    try:
        await client.connect()

        try:
            ent = await client.get_entity(group_row[0])
        except Exception as entity_error:
            if "Cannot find any entity corresponding to" in str(entity_error):
                try:
                    try:
                        group_id_int = int(group_row[0])
                        ent = await get_entity_by_id(client, group_id_int)
                        if not ent:
                            await event.respond(
                                f"⚠ Ошибка: не удалось получить информацию о группе {group_username}."
                            )
                            return
                    except ValueError:
                        await event.respond(
                            f"⚠ Ошибка: не удалось получить информацию о группе {group_username}."
                        )
                        return
                except Exception as alt_error:
                    logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                    await event.respond(
                        f"⚠ Ошибка: не удалось получить информацию о группе {group_username}."
                    )
                    return
            else:
                await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                return

        broadcast_row = cursor.execute(
            """
            SELECT broadcast_text, interval_minutes, is_active, photo_url
            FROM broadcasts
            WHERE user_id = ? AND group_id = ?
        """,
            (user_id, gid_key(group_id)),
        ).fetchone()

        broadcast_text = broadcast_row[0] if broadcast_row and broadcast_row[0] else "Не установлен"
        interval = f"{broadcast_row[1]} мин." if broadcast_row and broadcast_row[1] else "Не установлен"
        status = broadcast_status_emoji(user_id, group_id)

        photo_url = broadcast_row[3] if broadcast_row and len(broadcast_row) > 3 and broadcast_row[3] else None
        photo_info = os.path.basename(photo_url) if photo_url else "Отсутствует"

        group_title = getattr(ent, "title", group_username)
        group_username_display = f"@{ent.username}" if hasattr(ent, "username") and ent.username else "Нет юзернейма"

        members_count = getattr(ent, "participants_count", None)
        if members_count is None:
            try:
                if isinstance(ent, Channel):
                    full_channel = await client(GetFullChannelRequest(ent))
                    members_count = getattr(full_channel.full_chat, "participants_count", "Неизвестно")
                elif isinstance(ent, Chat):
                    full_chat = await client(GetFullChatRequest(ent.id))
                    members_count = getattr(full_chat.full_chat, "participants_count", "Неизвестно")
                else:
                    members_count = "Неизвестно"
            except Exception as e:
                logger.error(f"Не удалось получить количество участников: {e}")
                members_count = "Неизвестно"

        if isinstance(ent, Channel):
            group_type = "Канал" if ent.broadcast else "Супергруппа"
        elif isinstance(ent, Chat):
            group_type = "Группа"
        else:
            group_type = "Неизвестный тип"

        bt_preview = "Не установлен"
        if broadcast_row and broadcast_row[0]:
            bt_preview = ellipsize(strip_html_tags(broadcast_row[0]), 220) or "Не установлен"

        info_text = (
            "<b>📊 Информация о группе</b>\n\n"
            f"👥 <b>Название:</b> {group_title}\n"
            f"🔖 <b>Юзернейм:</b> <code>{group_username_display}</code>\n"
            f"👤 <b>Участников:</b> <code>{members_count}</code>\n"
            f"📝 <b>Тип:</b> <code>{group_type}</code>\n"
            f"🆔 <b>ID:</b> <code>{group_id}</code>\n\n"
            f"📬 <b>Статус рассылки:</b> {status}\n"
            f"⏱ <b>Интервал:</b> <code>{interval}</code>\n"
            f"📝 <b>Текст:</b>\n<code>{bt_preview}</code>\n"
            f"🖼 <b>Фото:</b> <code>{photo_info}</code>"
        )

        buttons = [
            [Button.inline("📝 Настроить текст/интервал", f"BroadcastTextInterval_{user_id}_{group_id}".encode())],
            [Button.inline("▶️ Запустить/возобновить", f"StartResumeBroadcast_{user_id}_{group_id}".encode())],
            [Button.inline("⏹ Остановить", f"StopAccountBroadcast_{user_id}_{group_id}".encode())],
            [Button.inline("🗑 Удалить группу", f"DeleteGroup_{user_id}_{group_id}".encode())],
        ]

        await event.respond(info_text, buttons=buttons)

    except Exception as e:
        logger.error(f"Ошибка при получении информации о группе: {e}")
        await event.respond(f"⚠ Ошибка при получении информации о группе: {str(e)}")
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


@bot.on(Query(data=lambda d: d.decode().startswith("groupInfo_")))
async def group_info(event: callback_query) -> None:
    data = event.data.decode()
    prefix = "groupInfo_"
    if not data.startswith(prefix):
        return
    rest = data[len(prefix):]
    user_id_str, gid_str = rest.rsplit("_", 1)
    user_id, group_id = int(user_id_str), int(gid_str)
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    nav_append(event.sender_id, f"I:{user_id}:{group_id}")
    await send_group_info_ui(event, user_id, group_id)
