from loguru import logger

from telethon import Button
from config import callback_message, callback_query, ADMIN_ID_LIST, New_Message, Query, bot
from utils.access import is_allowed, subscription_text_html
from utils.reply_nav import maybe_install_reply_nav_keyboard, nav_reset_home


def admin_main_keyboard():
    """Главное меню администратора: без редко используемой «общей» привязки группы — группы добавляются из карточки аккаунта."""
    return [
        [
            Button.inline("➕ Добавить аккаунт", b"add_account"),
            Button.inline("👤 Мои аккаунты", b"my_accounts"),
        ],
        [Button.inline("📨 Рассылка по всем аккаунтам", b"broadcast_All_account")],
        [Button.inline("⏹ Остановить рассылку везде", b"Stop_Broadcast_All_account")],
        [Button.inline("🕗 История рассылки", b"show_history")],
    ]


ADMIN_HOME_TEXT = (
    "<b>Панель рассылки @postfeelings</b>\n\n"
    "Управление аккаунтами Telegram и автоматическими сообщениями в ваших группах и каналах.\n\n"
    "<b>Что здесь можно сделать</b>\n"
    "• <b>Добавить аккаунт</b> — привязать профиль для рассылки (удобнее всего вход по QR).\n"
    "• <b>Мои аккаунты</b> — список профилей, привязанные чаты, запуск и остановка рассылок.\n"
    "• <b>Рассылка по всем аккаунтам</b> — один сценарий сразу для каждого привязанного аккаунта.\n"
    "• <b>Остановить везде</b> — снять активные задачи по всем аккаунтам и группам.\n"
    "• <b>История</b> — последние отправленные тексты и чаты.\n\n"
    "<b>Подсказка:</b> чтобы добавить группу к конкретному аккаунту, откройте "
    "<b>«Мои аккаунты»</b> → выберите аккаунт → <b>«Добавить группу»</b> или "
    "<b>«Синхронизировать из Telegram»</b>."
)


async def deliver_admin_home(event) -> None:
    """Главная панель админа + reply-клавиатура «Назад» / «Главное меню» под полем ввода."""
    nav_reset_home(event.sender_id)
    await event.respond(ADMIN_HOME_TEXT, buttons=admin_main_keyboard())
    await maybe_install_reply_nav_keyboard(event)


@bot.on(Query(data=b"admin_open_menu"))
async def admin_open_menu(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Доступно только администраторам.", alert=True)
        return
    await event.answer()
    await deliver_admin_home(event)


@bot.on(New_Message(pattern="/start"))
async def start(event: callback_message) -> None:
    """
    Обрабатывает команду /start
    """
    logger.info(f"Нажата команда /start")
    if event.sender_id in ADMIN_ID_LIST:
        await deliver_admin_home(event)
    elif is_allowed(event.sender_id):
        await event.respond(
            "<b>Ваш кабинет</b>\n\n"
            "В разделе <b>«Мои аккаунты»</b> отображается только профиль, привязанный к вашему Telegram ID.",
            buttons=[
                [
                    Button.inline("👤 Мои аккаунты", b"my_accounts"),
                    Button.inline("➕ Добавить аккаунт", b"add_account"),
                ],
                [
                    Button.inline("💳 Купить подписку", b"sub_buy"),
                    Button.inline("✅ Я оплатил", b"sub_paid"),
                ],
            ],
        )
    else:
        await event.respond(
            subscription_text_html(),
            buttons=[
                [Button.inline("💳 Купить подписку", b"sub_buy")],
                [Button.inline("✅ Я оплатил", b"sub_paid")],
            ],
        )
