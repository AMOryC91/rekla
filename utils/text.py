from __future__ import annotations

import json
import re
from typing import Any, List, Optional

from loguru import logger


_TAG_RE = re.compile(r"</?([a-zA-Z][a-zA-Z0-9-]*)(\s[^>]*)?>")

# Telegram HTML (клиентский режим): b, strong, i, em, u, ins, s, strike, del, code, pre, a, blockquote, tg-emoji, tg-spoiler
_ALLOWED = {
    "b",
    "strong",
    "i",
    "em",
    "u",
    "ins",
    "s",
    "strike",
    "del",
    "code",
    "pre",
    "a",
    "blockquote",
    "tg-emoji",
    "tg-spoiler",
}

_MD_BOLD_RE = re.compile(r"(?<!\*)\*\*(.+?)\*\*(?!\*)", flags=re.DOTALL)


def normalize_telegram_html(text: str) -> str:
    """
    Normalizes user-provided HTML-ish text for Telegram parse_mode='html'.
    - Converts <strong> to <b>
    - Converts <br> to newline
    - Strips unsupported tags (keeps their inner text)
    """
    if not text:
        return ""

    t = text.replace("\r\n", "\n")
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.IGNORECASE)
    t = re.sub(r"</?strong\s*>", lambda m: "<b>" if m.group(0)[1] != "/" else "</b>", t, flags=re.IGNORECASE)

    # Strip unsupported tags
    def _strip(m: re.Match) -> str:
        tag = (m.group(1) or "").lower()
        return m.group(0) if tag in _ALLOWED else ""

    t = _TAG_RE.sub(_strip, t)
    return t


def markdown_bold_to_html(text: str) -> str:
    """
    Minimal Markdown -> Telegram HTML conversion for bold.
    Supports **bold** (common Markdown). Intended as a fallback when we don't have entities.
    """
    if not text:
        return ""
    t = text.replace("\r\n", "\n")
    t = _MD_BOLD_RE.sub(r"<b>\1</b>", t)
    return t


def serialize_message_entities(entities) -> Optional[str]:
    """Сохраняет entities сообщения в JSON (для БД). None если список пуст."""
    if not entities:
        return None
    try:
        return json.dumps([e.to_dict() for e in entities], ensure_ascii=False)
    except Exception as ex:
        logger.warning("serialize_message_entities: {}", ex)
        return None


def deserialize_message_entities(data: Optional[str]) -> Optional[List[Any]]:
    """Восстанавливает список TypeMessageEntity из JSON."""
    if not data or not str(data).strip():
        return None
    try:
        from telethon.tl import types as typ

        raw = json.loads(data)
        out: List[Any] = []
        for item in raw:
            d = dict(item)
            name = d.pop("_", None)
            if not name:
                continue
            cls = getattr(typ, name, None)
            if cls is None:
                logger.warning("deserialize_message_entities: неизвестный тип {}", name)
                continue
            out.append(cls(**d))
        return out or None
    except Exception as ex:
        logger.warning("deserialize_message_entities: {}", ex)
        return None


def event_to_broadcast_payload(event) -> tuple[str, Optional[str]]:
    """
    Текст и форматирование как в Telegram: plain-текст message + entities (UTF-16 offsets как в API).
    Подходит для пересланных сообщений, подписи к фото и обычного текста.
    """
    msg = getattr(event, "message", None)
    if msg is None:
        return "", None
    text = msg.message or ""
    entities = getattr(msg, "entities", None)
    return text, serialize_message_entities(entities)


def event_text_to_telegram_html(event) -> str:
    """
    Extracts message text from a Telethon event preserving formatting.
    Priority:
      1) Unparse entities to HTML (Telegram-native formatting from UI)
      2) Normalize user-provided HTML-ish text
      3) Fallback: convert minimal Markdown bold (**x**) to HTML
    """
    msg = getattr(event, "message", None)
    text = (getattr(msg, "message", None) if msg is not None else None) or (
        getattr(event, "raw_text", None) or getattr(event, "text", None) or ""
    )
    text = text or ""

    entities = getattr(msg, "entities", None) if msg is not None else None
    if entities:
        try:
            # Telethon ships helpers to convert entities -> HTML text
            from telethon.extensions import html as _tl_html  # type: ignore

            return _tl_html.unparse(text, entities)
        except Exception:
            # Fall back below (we'll still log send failures elsewhere)
            pass

    # If user pasted HTML tags manually, keep only safe subset
    normalized = normalize_telegram_html(text)
    if normalized != text:
        return normalized

    # Last resort: support **bold**
    return normalize_telegram_html(markdown_bold_to_html(text))


def strip_html_tags(text: str) -> str:
    if not text:
        return ""
    return _TAG_RE.sub("", text)


def ellipsize(text: str, max_len: int = 160) -> str:
    t = (text or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max(0, max_len - 1)].rstrip() + "…"

