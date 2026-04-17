import html
import os.path

from config import callback_query, Query, bot, conn
from utils.access import is_admin
from utils.reply_nav import maybe_install_reply_nav_keyboard


@bot.on(Query(data=lambda data: data.decode().startswith("show_history")))
async def show_history(event: callback_query) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Только для администратора.", alert=True)
        return
    cursor = conn.cursor()
    
    # Обновляем запрос, чтобы получить информацию о фото
    cursor.execute("""
            SELECT h.group_name, h.sent_at, h.message_text, b.photo_url
            FROM send_history h
            LEFT JOIN broadcasts b ON h.user_id = b.user_id AND h.group_id = b.group_id
            ORDER BY h.sent_at DESC
            LIMIT 10
        """)
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        await event.respond("<b>🕗 История</b>\n\nПока нет записей об отправках.")
        await maybe_install_reply_nav_keyboard(event)
        return

    messages = ["<b>🕗 Последние 10 отправок</b>\n\n"]
    current_msg_index = 0
    current_length = len(messages[0])
    max_length = 4000  
    
    num = 1
    for row in rows:
        group_name, sent_at, message_text, photo_url = row
        
        if message_text and len(message_text) > 100:
            message_text = message_text[:97] + "..."
            
        gname = html.escape(str(group_name or ""))
        sent_esc = html.escape(str(sent_at or ""))
        msg_esc = html.escape(str(message_text or ""))
        entry = (
            f"📌 <b>№{num}</b> · {gname}\n"
            f"🕓 <code>{sent_esc}</code>\n"
            f"💬 {msg_esc}"
        )
        
        # Добавляем информацию о фото, если оно есть
        if photo_url:
            # Получаем только имя файла из пути
            photo_name = html.escape(os.path.basename(photo_url) if photo_url else "неизвестно")
            entry += f"\n🖼 {photo_name}"
        
        entry += "\n\n"
        entry_length = len(entry)
        
        
        if current_length + entry_length > max_length:
            
            messages.append(entry)
            current_msg_index += 1
            current_length = entry_length
        else:
            
            messages[current_msg_index] += entry
            current_length += entry_length
        
        num += 1
    if len(messages) == 1:
        await event.respond(messages[0])
    else:
        for msg in messages[:-1]:
            await event.respond(msg)
        await event.respond(messages[-1])
    await maybe_install_reply_nav_keyboard(event)