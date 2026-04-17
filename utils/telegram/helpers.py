from loguru import logger
from typing import List, Union, Optional, Any

from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat, InputPeerChannel, InputPeerChat
from telethon import TelegramClient

from config import conn

from utils.text import normalize_telegram_html, deserialize_message_entities


def gid_key(value: int) -> int:
    """Возвращает abs(id).  Для супергрупп (-100...) и обычных чатов получается один и тот же «ключ»."""
    return abs(value)


def broadcast_status_emoji(user_id: int,
                           group_id: int) -> str:
    """Короткий понятный статус рассылки по группе (без двусмысленного «закончена или не начата»)."""
    gid_key_str = gid_key(group_id)
    if gid_key_str in get_active_broadcast_groups(user_id):
        return "🟢 Рассылка вкл."
    return "⚪ Рассылка выкл."


def get_active_broadcast_groups(user_id: int) -> List[int]:
    active = set()
    cursor = conn.cursor()
    cursor.execute("""SELECT group_id FROM broadcasts WHERE is_active = ? AND user_id = ?""", (True, user_id))
    broadcasts = cursor.fetchall()
    for job in broadcasts:
        active.add(job[0])
    cursor.close()
    return list(active)


def create_broadcast_data(user_id: int,
                      group_id: int,
                      text: str,
                      interval_minutes: int,
                      photo_url: str = None,
                      entities_json: Optional[str] = None,
                      fwd_bot_id: Optional[int] = None,
                      fwd_msg_ids_json: Optional[str] = None) -> None:
    """Создает или обновляет запись в таблице broadcasts."""
    cursor = conn.cursor()

    # Используем gid_key для правильной обработки ID группы
    group_id_key = gid_key(group_id)

    # Проверяем наличие записи
    cursor.execute("""SELECT * FROM broadcasts WHERE user_id = ? AND group_id = ?""", (user_id, group_id_key))
    if cursor.fetchone():
        # Обновляем существующую запись
        cursor.execute("""UPDATE broadcasts 
                          SET broadcast_text = ?, interval_minutes = ?, is_active = ?, photo_url = ?, broadcast_entities = ?,
                              broadcast_fwd_bot_id = ?, broadcast_fwd_msg_ids = ?
                          WHERE user_id = ? AND group_id = ?""",
                       (text, interval_minutes, True, photo_url, entities_json, fwd_bot_id, fwd_msg_ids_json,
                        user_id, group_id_key))
    else:
        # Создаем новую запись
        cursor.execute("""INSERT INTO broadcasts 
                          (user_id, group_id, broadcast_text, interval_minutes, is_active, photo_url, broadcast_entities,
                           broadcast_fwd_bot_id, broadcast_fwd_msg_ids) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                       (user_id, group_id_key, text, interval_minutes, True, photo_url, entities_json,
                        fwd_bot_id, fwd_msg_ids_json))

    conn.commit()
    cursor.close()


def event_message_has_image(event) -> bool:
    """Фото (сжатое) или изображение как документ — для одного шага «текст+картинка»."""
    if getattr(event, "photo", None):
        return True
    doc = getattr(event, "document", None)
    mime = getattr(doc, "mime_type", None) or ""
    return mime.startswith("image/")


async def deliver_broadcast(
    client: TelegramClient,
    entity: Any,
    text: str,
    photo_path: Optional[str] = None,
    entities_json: Optional[str] = None,
    fwd_bot_id: Optional[int] = None,
    fwd_msg_ids_json: Optional[str] = None,
):
    """
    Отправка копии сообщения в целевой чат (текст + entities и/или файл).
    Поля пересылки из БД игнорируются — раньше использовались для forward_messages.
    """
    _ = fwd_bot_id, fwd_msg_ids_json
    if photo_path:
        try:
            return await send_broadcast_formatted(client, entity, text, photo_path, entities_json)
        except Exception as photo_error:
            logger.error(f"Ошибка при отправке с фото: {photo_error}")
            return await send_broadcast_formatted(client, entity, text, None, entities_json)
    return await send_broadcast_formatted(client, entity, text, None, entities_json)


async def send_broadcast_formatted(
    client: TelegramClient,
    entity: Any,
    text: str,
    photo_path: Optional[str] = None,
    entities_json: Optional[str] = None,
):
    """
    Отправка текста/подписи с сохранением форматирования API (entities) или fallback HTML.
    """
    entities = deserialize_message_entities(entities_json)
    if photo_path:
        if entities:
            return await client.send_file(
                entity,
                photo_path,
                caption=text if text else None,
                formatting_entities=entities,
            )
        cap = normalize_telegram_html(text) if text else ""
        if cap:
            return await client.send_file(entity, photo_path, caption=cap, parse_mode="html")
        return await client.send_file(entity, photo_path)
    # Telegram не принимает полностью пустое тело без файла
    body = text or ""
    if entities:
        if not body.strip():
            body = "\u2060"
        return await client.send_message(entity, body, formatting_entities=entities)
    safe = normalize_telegram_html(body)
    if not (safe or "").strip():
        safe = "\u2060"
    return await client.send_message(entity, safe, parse_mode="html")


async def get_entity_by_id(client: TelegramClient, group_id: int) -> Optional[Union[Channel, Chat]]:
    """
    Пытается получить объект группы по ID, используя разные методы.
    
    Args:
        client: Экземпляр TelegramClient
        group_id: ID группы
    
    Returns:
        Объект Channel или Chat, если удалось получить, иначе None
    """
    try:
        # Пробуем получить как канал (большинство групп в Telegram - это каналы)
        try:
            entity = await client.get_entity(PeerChannel(group_id))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как PeerChannel: {e}")
            
        # Пробуем получить как обычный чат
        try:
            entity = await client.get_entity(PeerChat(group_id))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как PeerChat: {e}")
            
        # Пробуем через InputPeer
        try:
            entity = await client.get_entity(InputPeerChannel(group_id, 0))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как InputPeerChannel: {e}")
            
        try:
            entity = await client.get_entity(InputPeerChat(group_id))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как InputPeerChat: {e}")
            
        # Пробуем напрямую по ID
        try:
            entity = await client.get_entity(group_id)
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить напрямую по ID: {e}")
            
        logger.error(f"Не удалось получить entity для group_id={group_id}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении entity: {e}")
        return None
