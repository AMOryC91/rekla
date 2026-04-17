"""
Reply-клавиатура под полем ввода: «Назад» и «Главное меню» (не inline).
Стек токенов в config.admin_nav_stack.
"""
from __future__ import annotations

from telethon import Button

from config import (
    ADMIN_ID_LIST,
    admin_nav_stack,
    admin_reply_kb_installed,
    add_group_to_account_state,
    broadcast_all_state,
    broadcast_all_state_account,
    broadcast_solo_state,
)

RK_BACK = "◀️ Назад"
RK_HOME = "🏠 Главное меню"


def reply_nav_buttons():
    return [[Button.text(RK_BACK), Button.text(RK_HOME)]]


def nav_reset_home(user_id: int) -> None:
    admin_nav_stack[user_id] = ["H"]


def nav_set_accounts_list(user_id: int) -> None:
    admin_nav_stack[user_id] = ["H", "L"]


def nav_append(user_id: int, token: str) -> None:
    st = admin_nav_stack.setdefault(user_id, ["H"])
    if st and st[-1] == token:
        return
    st.append(token)


def nav_after_back(user_id: int) -> str:
    """Удаляет текущий экран и возвращает токен экрана, куда вернулись."""
    st = admin_nav_stack.get(user_id, ["H"])
    if len(st) <= 1:
        return "H"
    st.pop()
    admin_nav_stack[user_id] = st
    return st[-1]


def clear_admin_wizards(user_id: int) -> None:
    broadcast_solo_state.pop(user_id, None)
    broadcast_all_state.pop(user_id, None)
    broadcast_all_state_account.pop(user_id, None)
    add_group_to_account_state.pop(user_id, None)


async def maybe_install_reply_nav_keyboard(event) -> None:
    """Один раз отправляет «невидимое» сообщение с reply-клавиатурой навигации."""
    uid = event.sender_id
    if uid not in ADMIN_ID_LIST or uid in admin_reply_kb_installed:
        return
    chat = await event.get_input_chat()
    await event.client.send_message(chat, "\u2060", buttons=reply_nav_buttons())
    admin_reply_kb_installed.add(uid)
