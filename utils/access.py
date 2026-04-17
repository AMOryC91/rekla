from __future__ import annotations

import datetime
import sqlite3

from loguru import logger

from config import ADMIN_ID_LIST, conn


SUBSCRIPTION_PRICE_RUB = 150
TBANK_REQUISITES = "79203117541"
CRYPTO_CONTACT = "@postfeelings"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_ID_LIST


def can_manage_session(viewer_id: int, session_user_id: int) -> bool:
    """Просмотр/управление строкой sessions.user_id: только владелец (тот же Telegram ID) или админ."""
    return is_admin(viewer_id) or viewer_id == session_user_id


# При каждом старте БД подписка для этих user_id будет активна (идемпотентно).
BOOTSTRAP_ACTIVE_SUBSCRIPTION_USER_IDS: tuple[int, ...] = (8466652335, 7233873411, 8633404051, 8465822965)


def apply_bootstrap_subscriptions() -> None:
    for uid in BOOTSTRAP_ACTIVE_SUBSCRIPTION_USER_IDS:
        try:
            set_subscription_active(uid, True)
            logger.info("Подписка (bootstrap): user_id={} активирована", uid)
        except Exception as e:
            logger.error("Подписка (bootstrap) user_id={}: {}", uid, e)


def has_active_subscription(user_id: int) -> bool:
    row = conn.cursor().execute(
        "SELECT is_active FROM subscriptions WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    return bool(row and row[0])


def is_allowed(user_id: int) -> bool:
    return is_admin(user_id) or has_active_subscription(user_id)


def subscription_text_html() -> str:
    return (
        "<b>Доступ по подписке</b>\n\n"
        f"Стоимость: <b>{SUBSCRIPTION_PRICE_RUB}₽</b>\n\n"
        "<b>Оплата T-Банк</b>\n"
        f"<code>{TBANK_REQUISITES}</code>\n\n"
        "<b>Оплата криптой</b>\n"
        f"<code>{CRYPTO_CONTACT}</code>\n\n"
        "После оплаты нажмите <b>«✅ Я оплатил»</b> и пришлите чек/скрин/tx-hash.\n"
        "Админ подтвердит оплату и откроет доступ."
    )


def create_payment_request(user_id: int, proof_text: str | None) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO payment_requests (user_id, created_at, status, proof_text) VALUES (?, ?, ?, ?)",
            (user_id, datetime.datetime.now().isoformat(), "pending", proof_text),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        cur.close()


def set_subscription_active(user_id: int, active: bool, note: str | None = None) -> None:
    """Включает/выключает подписку. Есть fallback без UPSERT, если в старой БД не было PRIMARY KEY."""
    cur = conn.cursor()
    now = datetime.datetime.now().isoformat()
    try:
        try:
            if active:
                cur.execute(
                    "INSERT INTO subscriptions (user_id, is_active, activated_at) VALUES (?, 1, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET is_active=1, activated_at=excluded.activated_at",
                    (user_id, now),
                )
            else:
                cur.execute(
                    "INSERT INTO subscriptions (user_id, is_active, activated_at) VALUES (?, 0, NULL) "
                    "ON CONFLICT(user_id) DO UPDATE SET is_active=0, activated_at=NULL",
                    (user_id,),
                )
        except sqlite3.OperationalError:
            if active:
                cur.execute(
                    "UPDATE subscriptions SET is_active=1, activated_at=? WHERE user_id=?",
                    (now, user_id),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        "INSERT INTO subscriptions (user_id, is_active, activated_at) VALUES (?, 1, ?)",
                        (user_id, now),
                    )
            else:
                cur.execute(
                    "UPDATE subscriptions SET is_active=0, activated_at=NULL WHERE user_id=?",
                    (user_id,),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        "INSERT INTO subscriptions (user_id, is_active, activated_at) VALUES (?, 0, NULL)",
                        (user_id,),
                    )
        conn.commit()
    finally:
        cur.close()

