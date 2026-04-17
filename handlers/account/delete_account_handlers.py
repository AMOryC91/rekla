from loguru import logger
from telethon import Button

from config import Query, bot, conn, callback_query
from utils.access import can_manage_session
from utils.reply_nav import maybe_install_reply_nav_keyboard


@bot.on(Query(data=lambda event: event.decode().startswith(f"delete_account_")))
async def handle_user_input(event: callback_query):
    user_id: int = int(event.data.decode().strip().split("_")[2])
    if not can_manage_session(event.sender_id, user_id):
        await event.answer("Нет доступа к этому аккаунту.", alert=True)
        return
    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()

    if user:
        cursor.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        cursor.execute("""DELETE FROM groups WHERE user_id = ?""", (user_id, ))
        conn.commit()
        logger.info(f"✅ Аккаунт  id={user_id} успешно удален.")
        await event.respond(
            f"✅ Аккаунт <code>{user_id}</code> удалён из базы вместе с привязанными группами.",
            buttons=[[Button.inline("👤 Мои аккаунты", b"my_accounts")]],
        )
        await maybe_install_reply_nav_keyboard(event)
    else:
        logger.warning(f"Аккаунт id={user} не найден")
        await event.respond("⚠ Этот аккаунт не найден в базе.")
        await maybe_install_reply_nav_keyboard(event)
    cursor.close()
