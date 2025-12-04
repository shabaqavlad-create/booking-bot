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

MANAGERS = [int(x) for x in os.getenv("MANAGERS", "").split(",") if x.strip().isdigit()]

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
CLOSE_H, CLOSE_M = 23, 15
OPEN_T = time(OPEN_H, OPEN_M, tzinfo=TZ)
CLOSE_T = time(CLOSE_H, CLOSE_M, tzinfo=TZ)

# ----- Бизнес-константы -----
MAX_SIMS = 4
HOLD_MINUTES = 30

PRICES = {30: 390, 60: 690, 90: 990, 120: 1290}
MAX_ACTIVE_BOOKINGS_PER_USER = 6

SAFETY_GAP = timedelta(minutes=1)
REMIND_BEFORE = timedelta(hours=2)
AUTOCONFIRM_BEFORE = timedelta(minutes=45)

# --- Address & map (Yandex) ---
ADDRESS_FULL = "Екатеринбург, ул. Академика Парина, 35"
ADDRESS_AREA = "Академический"
ADDRESS_MAP_URL = "https://yandex.ru/maps/?text=%D0%95%D0%BA%D0%B0%D1%82%D0%B5%D1%80%D0%B8%D0%BD%D0%B1%D1%83%D1%80%D0%B3%2C%20%D1%83%D0%BB.%20%D0%90%D0%BA%D0%B0%D0%B4%D0%B5%D0%BC%D0%B8%D0%BA%D0%B0%20%D0%9F%D0%B0%D1%80%D0%B8%D0%BD%D0%B0%2C%2035"

# Краткая памятка "Как добраться"
HOWTO_TEXT = (
    "🚶 Как добраться:\n"
    f"• Мы находимся в районе {ADDRESS_AREA}, {ADDRESS_FULL}.\n"
    "• Вход со стороны улицы.\n"
    "• Парковка вдоль улицы, свободная.\n"
    "• Если что — звоните: +7 953 046-36-54\n"
)

# какие статусы считаем "занимающими симы"
ACTIVE_STATUSES = ("pending", "confirmed", "block")