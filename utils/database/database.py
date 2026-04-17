from config import conn


def create_table() -> None:
    """
    Создает таблицы SQL если их нет
    :return:
        None
    """
    start_cursor = conn.cursor()
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS pre_groups (
            group_id INTEGER,
            group_username TEXT)""")

    # Ensure expected columns exist (older DBs may differ)
    try:
        cols = {r[1] for r in start_cursor.execute("PRAGMA table_info(pre_groups)").fetchall()}
        if "group_id" not in cols:
            start_cursor.execute("ALTER TABLE pre_groups ADD COLUMN group_id INTEGER")
        if "group_username" not in cols:
            start_cursor.execute("ALTER TABLE pre_groups ADD COLUMN group_username TEXT")
    except Exception:
        pass

    # Uniqueness for cataloged groups
    try:
        start_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pre_groups_username ON pre_groups(group_username)")
    except Exception:
        pass

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER,
            group_username TEXT,
            user_id INTEGER,
            group_title TEXT)""")

    # Migration: add group_title if missing
    try:
        cols = {r[1] for r in start_cursor.execute("PRAGMA table_info(groups)").fetchall()}
        if "group_title" not in cols:
            start_cursor.execute("ALTER TABLE groups ADD COLUMN group_title TEXT")
    except Exception:
        pass

    # Prevent duplicates per account
    try:
        start_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_user_groupid ON groups(user_id, group_id)")
    except Exception:
        pass
    try:
        start_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_user_username ON groups(user_id, group_username)")
    except Exception:
        pass

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            session_string TEXT,
            first_name TEXT,
            username TEXT,
            phone TEXT,
            updated_at TEXT)""")

    # Migration: add cached session metadata columns
    try:
        cols = {r[1] for r in start_cursor.execute("PRAGMA table_info(sessions)").fetchall()}
        if "first_name" not in cols:
            start_cursor.execute("ALTER TABLE sessions ADD COLUMN first_name TEXT")
        if "username" not in cols:
            start_cursor.execute("ALTER TABLE sessions ADD COLUMN username TEXT")
        if "phone" not in cols:
            start_cursor.execute("ALTER TABLE sessions ADD COLUMN phone TEXT")
        if "updated_at" not in cols:
            start_cursor.execute("ALTER TABLE sessions ADD COLUMN updated_at TEXT")
    except Exception:
        pass

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS broadcasts ( 
            user_id INTEGER, 
            group_id INTEGER, 
            session_string TEXT, 
            broadcast_text TEXT, 
            interval_minutes INTEGER,
            is_active BOOLEAN,
            error_reason TEXT,
            photo_url TEXT)""")
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            group_id INTEGER,
            group_name TEXT,
            sent_at TEXT,
            message_text TEXT);""")

    # Subscriptions / payments
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY,
            is_active BOOLEAN NOT NULL DEFAULT 0,
            activated_at TEXT
        );""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS payment_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            proof_text TEXT,
            admin_note TEXT
        );""")

    # Миграция: добавляем столбец error_reason, если он не существует
    try:
        start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN error_reason TEXT")
        conn.commit()
    except:
        pass

    # Миграция: добавляем столбец photo_url, если он не существует
    try:
        start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN photo_url TEXT")
        conn.commit()
    except:
        pass

    # JSON массив MessageEntity (Telethon to_dict) — сохраняет жирный/курсив/премиум-эмодзи и т.д.
    try:
        cols = {r[1] for r in start_cursor.execute("PRAGMA table_info(broadcasts)").fetchall()}
        if "broadcast_entities" not in cols:
            start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN broadcast_entities TEXT")
        if "broadcast_fwd_bot_id" not in cols:
            start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN broadcast_fwd_bot_id INTEGER")
        if "broadcast_fwd_msg_ids" not in cols:
            start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN broadcast_fwd_msg_ids TEXT")
        conn.commit()
    except Exception:
        pass

    conn.commit()
    start_cursor.close()

    try:
        from utils.access import apply_bootstrap_subscriptions

        apply_bootstrap_subscriptions()
    except Exception:
        pass


def delete_table() -> None:
    """
    После остановки бота меняет статус активных рассылок на неактивный,
    сохраняя при этом тексты и интервалы рассылок
    :return:
        None
    """
    end_cursor = conn.cursor()
    # Вместо удаления всех записей, просто меняем статус на неактивный
    end_cursor.execute("""UPDATE broadcasts SET is_active = ? WHERE is_active = ?""", (False, True))
    conn.commit()
    end_cursor.close()
