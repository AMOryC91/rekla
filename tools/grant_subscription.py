"""
Разово выдать активную подписку в sessions.db (без перезапуска бота можно,
если бот не держит эксклюзивной блокировки — лучше остановить бота на секунду).

  python tools/grant_subscription.py 8466652335
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Корень проекта в sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.access import set_subscription_active  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Выдать подписку по Telegram user_id")
    p.add_argument("user_id", type=int, nargs="?", default=8466652335, help="Telegram user id")
    args = p.parse_args()
    set_subscription_active(args.user_id, True)
    print(f"OK: подписка активирована для user_id={args.user_id}")


if __name__ == "__main__":
    main()
