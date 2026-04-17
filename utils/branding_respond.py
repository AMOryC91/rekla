"""
Подмешивает брендовое изображение ко всем ответам бота через event.respond / event.reply.

Пропускает вложение, если:
  - передан no_brand=True;
  - уже указан file=...;
  - нет файла assets/branding.png;
  - текст подписи длиннее лимита Telegram для подписи к фото (чтобы не ломать отправку).

Внутренние вызовы bot.send_message(...) не затрагиваются.
"""
from __future__ import annotations

from pathlib import Path

BRAND_PATH = Path(__file__).resolve().parent.parent / "assets" / "branding.png"
# Telegram: подпись к медиа до 1024 символов; оставляем запас под сущности/HTML.
_CAPTION_SAFE = 1008

_orig_message_respond = None
_orig_message_reply = None
_orig_callback_respond = None


def _caption_text(args: tuple, kwargs: dict) -> str:
    if args and isinstance(args[0], str):
        return args[0]
    m = kwargs.get("message")
    if m is not None:
        return str(m)
    return ""


def _with_brand(args: tuple, kwargs: dict) -> tuple[tuple, dict]:
    kw = dict(kwargs)
    if kw.pop("no_brand", None):
        return args, kw
    if kw.get("file") is not None:
        return args, kw
    if not BRAND_PATH.is_file():
        return args, kw
    text = _caption_text(args, kw)
    if len(text) > _CAPTION_SAFE:
        return args, kw
    kw["file"] = str(BRAND_PATH)
    return args, kw


def install_branded_respond() -> None:
    """Патчит Telethon один раз при старте приложения."""
    global _orig_message_respond, _orig_message_reply, _orig_callback_respond

    import telethon.events.callbackquery as cbmod
    import telethon.tl.custom.message as mmod

    if _orig_message_respond is not None:
        return

    _orig_message_respond = mmod.Message.respond
    _orig_message_reply = mmod.Message.reply
    _orig_callback_respond = cbmod.CallbackQuery.Event.respond

    async def message_respond(self, *args, **kwargs):
        args, kwargs = _with_brand(args, kwargs)
        return await _orig_message_respond(self, *args, **kwargs)

    async def message_reply(self, *args, **kwargs):
        args, kwargs = _with_brand(args, kwargs)
        return await _orig_message_reply(self, *args, **kwargs)

    async def callback_respond(self, *args, **kwargs):
        args, kwargs = _with_brand(args, kwargs)
        return await _orig_callback_respond(self, *args, **kwargs)

    mmod.Message.respond = message_respond  # type: ignore[method-assign]
    mmod.Message.reply = message_reply  # type: ignore[method-assign]
    cbmod.CallbackQuery.Event.respond = callback_respond  # type: ignore[method-assign]
