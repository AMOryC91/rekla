import asyncio
import sqlite3
import sys
import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.errors import PasswordHashInvalidError
from telethon.sessions import StringSession
from telethon.tl.functions.auth import ResendCodeRequest

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import API_ID, API_HASH
from utils.database.database import create_table


def _sent_code_info(sent: object) -> str:
    sent_type = getattr(sent, "type", None)
    sent_type_name = type(sent_type).__name__ if sent_type is not None else "Unknown"
    timeout = getattr(sent, "timeout", None)
    next_type = getattr(sent, "next_type", None)
    next_type_name = type(next_type).__name__ if next_type is not None else None
    parts = [f"type={sent_type_name}"]
    if timeout is not None:
        parts.append(f"timeout={timeout}s")
    if next_type_name:
        parts.append(f"next_type={next_type_name}")
    return ", ".join(parts)


async def main() -> None:
    """
    Adds a Telegram account without sending login codes to the bot chat.
    Run locally in a terminal:

      python tools/add_account_cli.py
    """
    create_table()

    phone = input("Phone in international format (e.g. +447...): ").strip()
    if not (phone.startswith("+") and phone[1:].isdigit()):
        raise SystemExit("Invalid phone format.")

    client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        use_ipv6=False,
        connection_retries=5,
        retry_delay=2,
        timeout=15,
    )

    try:
        await client.connect()
    except Exception as e:
        msg = str(e) or e.__class__.__name__
        raise SystemExit(
            "Failed to connect to Telegram servers.\n"
            f"Reason: {msg}\n\n"
            "Most common cause: Telegram is blocked on this network.\n"
            "Fix options:\n"
            "- Use VPN / allow Telegram in firewall/router.\n"
        ) from e
    try:
        sent = await client.send_code_request(phone)
        print(f"Code requested ({_sent_code_info(sent)}).")
        print(
            "Where to look:\n"
            "- In Telegram app: open the service chat named 'Telegram' (may be archived).\n"
            "- Or SMS, depending on your account/region.\n"
        )

        while True:
            code = input("Enter the code (or type 'resend'): ").strip()
            if code.lower() == "resend":
                try:
                    sent = await client(ResendCodeRequest(phone_number=phone, phone_code_hash=sent.phone_code_hash))
                    print(f"Code resent ({_sent_code_info(sent)}). Enter the newest code.")
                    continue
                except Exception as e:
                    print(f"Resend failed: {e}")
                    continue
            if not code.isdigit():
                print("Code must be digits. Try again.")
                continue
            break

        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
        except SessionPasswordNeededError:
            while True:
                pwd = input("2FA password required. Enter password: ")
                try:
                    await client.sign_in(password=pwd)
                    break
                except PasswordHashInvalidError:
                    print("Invalid 2FA password. Try again.")

        me = await client.get_me()
        session_string = client.session.save()

        conn = sqlite3.connect(str(PROJECT_ROOT / "sessions.db"), timeout=30.0)
        try:
            cur = conn.cursor()
            exists = cur.execute("SELECT 1 FROM sessions WHERE user_id = ?", (me.id,)).fetchone()
            if exists:
                print("Account already exists in DB.")
            else:
                cur.execute(
                    "INSERT INTO sessions (user_id, session_string, first_name, username, phone, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        me.id,
                        session_string,
                        getattr(me, "first_name", None),
                        getattr(me, "username", None),
                        getattr(me, "phone", None),
                        datetime.datetime.now().isoformat(),
                    ),
                )
                conn.commit()
                print(f"OK. Added account {me.id} (@{getattr(me, 'username', None)})")
        finally:
            conn.close()
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
