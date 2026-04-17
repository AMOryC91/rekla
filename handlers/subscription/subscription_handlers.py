from __future__ import annotations

from loguru import logger
from telethon import Button

from config import New_Message, Query, bot, callback_message, callback_query
from utils.access import (
    CRYPTO_CONTACT,
    SUBSCRIPTION_PRICE_RUB,
    TBANK_REQUISITES,
    subscription_text_html,
    create_payment_request,
    set_subscription_active,
)
from config import ADMIN_ID_LIST, conn


def _subscription_buttons():
    return [
        [Button.inline("💳 Купить подписку", b"sub_buy")],
        [Button.inline("✅ Я оплатил", b"sub_paid")],
    ]


@bot.on(Query(data=b"sub_buy"))
async def sub_buy(event: callback_query) -> None:
    me = await event.get_sender()
    uname = getattr(me, "username", None)
    who = f"@{uname}" if uname else f"ID <code>{event.sender_id}</code>"
    await event.respond(
        "<b>Оплата подписки</b>\n\n"
        f"1. Переведите <b>{SUBSCRIPTION_PRICE_RUB}₽</b> по реквизитам:\n"
        f"• <b>T-Банк:</b> <code>{TBANK_REQUISITES}</code>\n"
        f"• <b>Крипта / связь:</b> {CRYPTO_CONTACT}\n\n"
        "2. Напишите в Telegram <b>@postfeelings</b> в личку: "
        "<b>скрин чека</b> и ваш ник, как в боте — "
        f"{who}\n\n"
        "3. Или нажмите <b>«✅ Я оплатил»</b> ниже и пришлите чек/скрин <b>сюда боту</b> — заявка уйдёт админам.",
        buttons=_subscription_buttons(),
    )


# State: waiting for proof from this user
_waiting_proof: dict[int, bool] = {}


@bot.on(Query(data=b"sub_paid"))
async def sub_paid(event: callback_query) -> None:
    _waiting_proof[event.sender_id] = True
    me = await event.get_sender()
    uname = getattr(me, "username", None)
    who = f"@{uname}" if uname else f"ID <code>{event.sender_id}</code>"
    await event.respond(
        "<b>Чек оплаты</b>\n\n"
        "Пришлите <b>одним сообщением</b> скрин чека или tx-hash.\n"
        f"Укажите, что платили с аккаунта: {who}\n\n"
        "Заявка уйдёт админам; доступ откроют после проверки.\n"
        "Либо параллельно можете написать <b>@postfeelings</b> с чеком и этим ником.",
    )


@bot.on(New_Message(func=lambda e: _waiting_proof.pop(e.sender_id, None) is True))
async def sub_proof_message(event: callback_message) -> None:
    user_id = event.sender_id
    proof_text = event.raw_text or ""
    req_id = create_payment_request(user_id, proof_text[:2000] if proof_text else None)

    # forward to admins with approve/deny buttons
    buttons = [
        [
            Button.inline("✅ Одобрить", f"sub_approve_{user_id}_{req_id}".encode()),
            Button.inline("❌ Отклонить", f"sub_deny_{user_id}_{req_id}".encode()),
        ]
    ]

    try:
        for admin_id in ADMIN_ID_LIST:
            try:
                await bot.send_message(
                    admin_id,
                    (
                        "<b>Запрос подписки</b>\n\n"
                        f"User ID: <code>{user_id}</code>\n"
                        f"Request ID: <code>{req_id}</code>\n\n"
                        "<b>Сообщение пользователя:</b>\n"
                        f"<code>{(proof_text[:3500] if proof_text else '(без текста)')}</code>"
                    ),
                    buttons=buttons,
                )
                # If user attached media, forward it too
                if event.message.media:
                    await bot.forward_messages(admin_id, event.message)
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")

        await event.respond(
            "<b>Отправлено на проверку</b>\n\n"
            "Админ подтвердит оплату и доступ откроется автоматически.",
            buttons=_subscription_buttons(),
        )
    except Exception as e:
        logger.error(f"sub_proof_message error: {e}")
        await event.respond("⚠ Не удалось отправить админам. Попробуйте ещё раз позже.")


@bot.on(Query(data=lambda d: d.decode().startswith("sub_approve_")))
async def sub_approve(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.respond("⛔ Недостаточно прав.")
        return

    _, _, user_id_s, req_id_s = event.data.decode().split("_", 3)
    user_id = int(user_id_s)
    req_id = int(req_id_s)

    cur = conn.cursor()
    try:
        cur.execute("UPDATE payment_requests SET status='approved' WHERE id=? AND user_id=?", (req_id, user_id))
        conn.commit()
    finally:
        cur.close()

    set_subscription_active(user_id, True)
    await event.respond(f"✅ Подписка активирована для <code>{user_id}</code>.")
    try:
        await bot.send_message(
            user_id,
            "<b>Доступ открыт</b>\n\nПодписка активна. Нажмите /start.",
        )
    except Exception:
        pass


@bot.on(Query(data=lambda d: d.decode().startswith("sub_deny_")))
async def sub_deny(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.respond("⛔ Недостаточно прав.")
        return

    _, _, user_id_s, req_id_s = event.data.decode().split("_", 3)
    user_id = int(user_id_s)
    req_id = int(req_id_s)

    cur = conn.cursor()
    try:
        cur.execute("UPDATE payment_requests SET status='denied' WHERE id=? AND user_id=?", (req_id, user_id))
        conn.commit()
    finally:
        cur.close()

    await event.respond(f"❌ Отклонено для <code>{user_id}</code>.")
    try:
        await bot.send_message(
            user_id,
            "<b>Оплата не подтверждена</b>\n\n"
            "Проверьте реквизиты и пришлите чек/tx ещё раз (кнопка «✅ Я оплатил»).",
            buttons=_subscription_buttons(),
        )
    except Exception:
        pass

