from config import user_sessions_deleting, callback_query, callback_message, Query, New_Message, bot, conn, processed_callbacks


@bot.on(Query(data=b"delete_group"))
async def handle_delete_group(event: callback_query) -> None:
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{event.query.msg_id}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    user_sessions_deleting[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond(
        "<b>🗑 Удаление группы</b>\n\n"
        "Пришлите <b>@username</b> или <b>ID</b> группы.\n\n"
        "Пример: <code>@mygroup</code>\n"
        "Пример: <code>-1001234567890</code>"
    )


def is_awaiting_group_deletion(event):
    """Проверяет, ожидает ли пользователь ввода группы для удаления"""
    user_state = user_sessions_deleting.get(event.sender_id)
    return user_state and user_state.get("step") == "awaiting_group_username"


@bot.on(New_Message(func=is_awaiting_group_deletion))
async def handle_user_input(event: callback_message) -> None:
    group_input = event.text.strip()

    if group_input.startswith("@") or group_input.isdigit() or group_input.startswith("-"):
        cursor = conn.cursor()
        
        # Поиск по username или ID
        if group_input.startswith("@"):
            cursor.execute("SELECT * FROM groups WHERE group_username = ?", (group_input,))
        else:
            # Поиск по ID (может быть числовой или отрицательный)
            try:
                group_id = int(group_input)
                cursor.execute("SELECT * FROM groups WHERE group_id = ?", (group_id,))
            except ValueError:
                await event.respond("⚠ Введите корректный <b>@username</b> или <b>ID</b> группы.")
                return
        
        group = cursor.fetchone()

        if group:
            # Удаляем по тому же параметру, по которому искали
            if group_input.startswith("@"):
                cursor.execute("DELETE FROM groups WHERE group_username = ?", (group_input,))
                await event.respond(f"✅ Группа <code>{group_input}</code> удалена.")
            else:
                group_id = int(group_input)
                cursor.execute("DELETE FROM groups WHERE group_id = ?", (group_id,))
                await event.respond(f"✅ Группа с ID <code>{group_id}</code> удалена.")
            
            conn.commit()
        else:
            if group_input.startswith("@"):
                await event.respond(f"⚠ Группа <code>{group_input}</code> не найдена.")
            else:
                await event.respond(f"⚠ Группа с ID <code>{group_input}</code> не найдена.")

        user_sessions_deleting.pop(event.sender_id, None)
        cursor.close()
    else:
        await event.respond(
            "⚠ Введите корректный <b>@username</b> или <b>ID</b>.\n\n"
            "Пример: <code>@mygroup</code>\n"
            "Пример: <code>-1001234567890</code>"
        )
        return