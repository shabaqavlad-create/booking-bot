# config.py # конфиг
import os
import logging
from datetime import timedelta, time, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv

load_dotenv()

# ----- Базовые настройки -----
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан. Добавь его в .env")

ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан. Добавь его в .env")

# ----- Часовой пояс -----
try:
    TZ = ZoneInfo("Asia/Yekaterinburg")
except ZoneInfoNotFoundError:
    try:
        import tzdata  # noqa: F401
        TZ = ZoneInfo("Asia/Yekaterinburg")
    except Exception:
        logging.warning("tzdata не найден, использую фиксированный UTC+5 без переходов.")
        TZ = timezone(timedelta(hours=5))

# ----- Часы работы -----
OPEN_H, OPEN_M = 13, 0
CLOSE_H, CLOSE_M = 23, 0
OPEN_T = time(OPEN_H, OPEN_M, tzinfo=TZ)
CLOSE_T = time(CLOSE_H, CLOSE_M, tzinfo=TZ)

# ----- Бизнес-константы -----
MAX_SIMS = 4
HOLD_MINUTES = 30

PRICES = {30: 390, 60: 690, 90: 990, 120: 1290}
MAX_ACTIVE_BOOKINGS_PER_USER = 6

SAFETY_GAP = timedelta(minutes=5)
REMIND_BEFORE = timedelta(hours=2)
AUTOCONFIRM_BEFORE = timedelta(minutes=45)
