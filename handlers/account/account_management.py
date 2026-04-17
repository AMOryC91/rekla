import datetime
import html
import re
from loguru import logger

from telethon import Button, TelegramClient
from telethon.sessions import StringSession

from config import callback_query, API_ID, API_HASH, Query, bot, conn, processed_callbacks
from config import add_group_to_account_state, New_Message, callback_message
from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from utils.telegram import get_active_broadcast_groups, broadcast_status_emoji, get_entity_by_id
from utils.formatting import format_telegram_phone, user_display_name
from utils.access import is_admin, can_manage_session
from utils.reply_nav import maybe_install_reply_nav_keyboard, nav_append, nav_set_accounts_list


async def send_my_accounts_ui(event) -> None:
    """Список аккаунтов (и для callback, и для reply-навигации)."""
    try:
        cursor = conn.cursor()
        buttons = []
        accounts_found = False
        viewer = event.sender_id

        if is_admin(viewer):
            rows = cursor.execute(
                "SELECT user_id, first_name, username FROM sessions ORDER BY updated_at DESC NULLS LAST, user_id DESC"
            )
        else:
            rows = cursor.execute(
                "SELECT user_id, first_name, username FROM sessions WHERE user_id = ? ORDER BY updated_at DESC NULLS LAST",
                (viewer,),
            )

        for user_id, first_name, username in rows:
            accounts_found = True
            try:
                display = first_name or (f"@{username}" if username else None) or str(user_id)
                buttons.append([Button.inline(f"👤 {display}", f"account_info_{user_id}")])
            except Exception:
                buttons.append([Button.inline("⚠ Ошибка при загрузке аккаунта", f"error_{user_id}")])

        cursor.close()

        if not accounts_found:
            await event.respond(
                "<b>📱 Аккаунты</b>\n\n"
                "Пока нет ни одного привязанного аккаунта.\n"
                "Нажмите <b>«Добавить аккаунт»</b> в главном меню или выполните локально "
                "<code>python tools/add_account_cli.py</code>.",
            )
            await maybe_install_reply_nav_keyboard(event)
            return

        await event.respond(
            "<b>📱 Ваши аккаунты</b>\n\n"
            "Выберите строку ниже, чтобы открыть чаты, рассылки и настройки этого профиля.\n\n"
            "<i>Имя и телефон подтягиваются из Telegram при открытии карточки.</i>",
            buttons=buttons,
        )
        await maybe_install_reply_nav_keyboard(event)

    except Exception as e:
        logger.error(f"Error in send_my_accounts_ui: {e}")
        await event.respond("⚠ Произошла ошибка при получении списка аккаунтов")
        await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=b"my_accounts"))
async def my_accounts(event: callback_query) -> None:
    """Выводит список аккаунтов"""
    nav_set_accounts_list(event.sender_id)
    await send_my_accounts_ui(event)


async def send_account_card_ui(event, user_id: int) -> None:
    """Карточка аккаунта (callback или reply «Назад»)."""
    if not can_manage_session(event.sender_id, user_id):
        await event.respond("⚠ Нет доступа: вы можете управлять только своим аккаунтом.")
        await maybe_install_reply_nav_keyboard(event)
        return

    cursor = conn.cursor()
    row = cursor.execute(
        "SELECT first_name, username, phone, session_string FROM sessions WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        cursor.close()
        await maybe_install_reply_nav_keyboard(event)
        return

    try:
        first_name_db, username_db, phone_db, session_string = (
            row[0],
            row[1],
            row[2],
            row[3],
        )
        first_name, username, phone = first_name_db, username_db, phone_db
        if session_string and str(session_string).strip():
            client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
            try:
                await client.connect()
                me = await client.get_me()
                first_name = getattr(me, "first_name", None) or first_name_db
                username = getattr(me, "username", None) or username_db
                phone = getattr(me, "phone", None) or phone_db
                cursor.execute(
                    "UPDATE sessions SET first_name = ?, username = ?, phone = ?, updated_at = ? WHERE user_id = ?",
                    (
                        first_name,
                        username,
                        phone,
                        datetime.datetime.now().isoformat(),
                        user_id,
                    ),
                )
                conn.commit()
            except Exception as ex:
                logger.warning("Не удалось обновить профиль из Telegram для {}: {}", user_id, ex)
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        groups = cursor.execute(
            "SELECT group_id, group_title, group_username FROM groups WHERE user_id = ? ORDER BY group_title",
            (user_id,),
        )

        active_gids = get_active_broadcast_groups(user_id)
        lines = []

        for group_id, group_title, group_username in groups:
            title = group_title or group_username or f"ID {group_id}"
            title_esc = html.escape(str(title))
            lines.append(f"{broadcast_status_emoji(user_id, int(group_id))} {title_esc}")

        group_list = "\n".join(lines)
        if not group_list:
            group_list = "<i>Нет привязанных чатов. Добавьте группу вручную или нажмите «Из диалогов Telegram».</i>"

        mass_active = "🟢 включена" if active_gids else "⚪ выключена"
        profile_line = user_display_name(first_name, username, user_id)
        phone_line = format_telegram_phone(phone)

        buttons = [
            [Button.inline("📋 Группы", f"listOfgroups_{user_id}")],
            [
                Button.inline("🚀 Рассылка во все чаты", f"broadcastAll_{user_id}"),
                Button.inline("⏹ Стоп всем чатам", f"StopBroadcastAll_{user_id}"),
            ],
            [
                Button.inline("➕ Добавить группу", f"add_group_to_account_{user_id}"),
                Button.inline("🔄 Из диалогов Telegram", f"add_all_groups_{user_id}"),
            ],
            [Button.inline("🗑 Удалить аккаунт", f"delete_account_{user_id}")],
        ]

        await event.respond(
            "<b>📇 Аккаунт</b>\n\n"
            f"👤 <b>Профиль:</b> {profile_line}\n"
            f"📞 <b>Телефон:</b> {phone_line}\n"
            f"🆔 <b>User ID:</b> <code>{user_id}</code>\n\n"
            f"📣 <b>Массовая рассылка по всем чатам этого аккаунта:</b> {mass_active}\n\n"
            "<b>Чаты в базе</b>\n"
            f"{group_list}\n\n"
            "<i>Статус у строки: «🟢 Рассылка вкл.» — для этой группы запущена задача; "
            "«⚪ Рассылка выкл.» — задачи нет или она остановлена.</i>",
            buttons=buttons,
        )
    finally:
        cursor.close()
    await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=lambda data: data.decode().startswith("account_info_")))
async def handle_account_button(event: callback_query) -> None:
    user_id = int(event.data.decode().split("_")[2])
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return

    callback_id = f"{event.sender_id}:{event.query.msg_id}"

    if callback_id in processed_callbacks:
        return

    processed_callbacks[callback_id] = True

    nav_append(event.sender_id, f"A:{user_id}")
    await send_account_card_ui(event, user_id)


@bot.on(Query(data=lambda d: d.decode().startswith("add_group_to_account_")))
async def add_group_to_account_start(event: callback_query) -> None:
    user_id = int(event.data.decode().split("_")[4])
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    add_group_to_account_state[event.sender_id] = {"user_id": user_id}
    await event.respond(
        "<b>➕ Добавление группы</b>\n\n"
        "Пришлите <b>@username</b> супергруппы/канала или <b>числовой ID</b> (для приватных чатов).\n"
        "Бот попытается от имени этого аккаунта открыть чат и сохранить его в списке.",
    )
    await maybe_install_reply_nav_keyboard(event)


@bot.on(New_Message(func=lambda e: e.sender_id in add_group_to_account_state))
async def add_group_to_account_input(event: callback_message) -> None:
    st = add_group_to_account_state.get(event.sender_id)
    if not st:
        return
    user_id = st["user_id"]
    if not can_manage_session(event.sender_id, user_id):
        add_group_to_account_state.pop(event.sender_id, None)
        await event.respond("⚠ Нет доступа к этому аккаунту.")
        return
    add_group_to_account_state.pop(event.sender_id, None)
    group_identifier = (event.raw_text or "").strip()
    if not group_identifier:
        await event.respond("⚠ Пустой ввод. Попробуйте ещё раз.")
        return
    # Support @username and links like t.me/username, https://t.me/username.
    group_identifier = re.sub(r"^https?://", "", group_identifier, flags=re.IGNORECASE)
    if group_identifier.lower().startswith("t.me/"):
        group_identifier = group_identifier.split("/", 1)[1]
        group_identifier = group_identifier.split("?", 1)[0].strip()
        if group_identifier:
            group_identifier = f"@{group_identifier.lstrip('@')}"

    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        cursor.close()
        await event.respond("⚠ Не удалось найти сессию этого аккаунта.")
        return

    session_string = row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        try:
            if group_identifier.startswith("@"):
                entity = await client.get_entity(group_identifier)
                # Users cannot be target groups for broadcasts.
                if not getattr(entity, "title", None):
                    await event.respond(
                        "⚠ По этому username найден пользователь, а не группа/канал.\n"
                        "Укажите @username группы/канала или числовой id чата."
                    )
                    return
                group_id = int(getattr(entity, "id"))
                group_username = group_identifier
                group_title = getattr(entity, "title", group_identifier)
                try:
                    await client(JoinChannelRequest(entity))
                except (UserAlreadyParticipantError, Exception):
                    pass
            else:
                group_id = int(group_identifier)
                group_username = group_identifier
                group_title = group_identifier
        except Exception as e:
            msg = str(e)
            if "ResolveUsernameRequest" in msg or "The key is not registered in the system" in msg:
                await event.respond(
                    "⚠ Не удалось найти группу/канал по username.\n"
                    "Проверьте, что username существует и это именно группа/канал, а не пользователь."
                )
            else:
                await event.respond(f"⚠ Не удалось найти/распознать группу: {e}")
            return

        cursor.execute(
            "INSERT OR IGNORE INTO groups (group_id, group_username, user_id, group_title) VALUES (?, ?, ?, ?)",
            (group_id, group_username, user_id, group_title),
        )
        conn.commit()
        await event.respond("✅ Группа добавлена в этот аккаунт. Откройте «Список групп» для проверки.")
        await maybe_install_reply_nav_keyboard(event)
    finally:
        await client.disconnect()
        cursor.close()

