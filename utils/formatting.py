"""Мелкие хелперы для отображения данных пользователя в боте."""
from __future__ import annotations

import html
from typing import Optional


def format_telegram_phone(phone: Optional[str]) -> str:
    """
    Номер для показа в HTML: без лишнего «+» перед фразой «не указан».
    У Telegram поле phone часто пустое из‑за приватности.
    """
    if not phone:
        return "не указан (Telegram часто скрывает номер у ботов — это нормально)"
    p = str(phone).strip()
    if p.startswith("+"):
        return html.escape(p)
    digits = "".join(c for c in p if c.isdigit())
    if digits:
        return html.escape(f"+{digits}")
    return html.escape(p)


def user_display_name(first_name: Optional[str], username: Optional[str], user_id: int) -> str:
    parts = []
    if first_name and str(first_name).strip():
        parts.append(html.escape(str(first_name).strip()))
    if username and str(username).strip():
        u = str(username).strip().lstrip("@")
        parts.append(html.escape(f"@{u}"))
    if parts:
        return " · ".join(parts)
    return f"id <code>{user_id}</code>"

