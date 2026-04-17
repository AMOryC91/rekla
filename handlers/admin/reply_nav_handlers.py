"""
Текстовые кнопки под полем ввода (reply keyboard): «Назад» и «Главное меню».
Не смешиваются с inline — остальные действия остаются в сообщении.
"""
from loguru import logger

from config import ADMIN_ID_LIST, New_Message, bot, callback_message
from utils.reply_nav import (
    RK_BACK,
    RK_HOME,
    clear_admin_wizards,
    nav_after_back,
    nav_reset_home,
)


@bot.on(
    New_Message(
        func=lambda e: e.sender_id in ADMIN_ID_LIST
        and (e.raw_text or "").strip() in (RK_BACK, RK_HOME)
    )
)
async def admin_reply_navigation(event: callback_message) -> None:
    txt = (event.raw_text or "").strip()
    try:
        if txt == RK_HOME:
            clear_admin_wizards(event.sender_id)
            nav_reset_home(event.sender_id)
            from handlers.admin.admin_handlers import deliver_admin_home

            await deliver_admin_home(event)
        else:
            token = nav_after_back(event.sender_id)
            await _dispatch_nav_token(event, token)
    except Exception as ex:
        logger.error("admin_reply_navigation: {}", ex)
        from handlers.admin.admin_handlers import deliver_admin_home

        await deliver_admin_home(event)
    try:
        event.stop_propagation()
    except Exception:
        pass


async def _dispatch_nav_token(event: callback_message, token: str) -> None:
    if token == "H":
        from handlers.admin.admin_handlers import deliver_admin_home

        await deliver_admin_home(event)
        return
    if token == "L":
        from handlers.account.account_management import send_my_accounts_ui

        await send_my_accounts_ui(event)
        return
    if token.startswith("A:"):
        uid = int(token.split(":", 1)[1])
        from handlers.account.account_management import send_account_card_ui

        await send_account_card_ui(event, uid)
        return
    if token.startswith("G:"):
        uid = int(token.split(":", 1)[1])
        from handlers.group.group_management import send_groups_list_ui

        await send_groups_list_ui(event, uid)
        return
    if token.startswith("I:"):
        parts = token.split(":", 2)
        uid = int(parts[1])
        gid = int(parts[2])
        from handlers.group.group_info_handlers import send_group_info_ui

        await send_group_info_ui(event, uid, gid)
        return
    from handlers.admin.admin_handlers import deliver_admin_home

    await deliver_admin_home(event)
