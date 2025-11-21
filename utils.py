#utils.py            форматирование, телефоны и т.п.
import re
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

# Если хочешь — перенеси TZ сюда, но можно оставить в config
from config import TZ, PRICES

RU_MONTHS = [
    "",
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

def localize(dt: datetime) -> datetime:
    return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt.astimezone(TZ)

def human(dt: datetime) -> str:
    return localize(dt).strftime("%d.%m %H:%M")

def today_local() -> date:
    return datetime.now(TZ).date()

def sims_word(n: int) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19:
        return "симов"
    if n1 == 1:
        return "сим"
    if 2 <= n1 <= 4:
        return "сима"
    return "симов"

def human_status(status: str) -> str:
    mapping = {
        "pending": "⏳ Ожидает подтверждения",
        "confirmed": "✅ Подтверждено",
        "done": "🏁 Завершено",
        "no_show": "🚫 Не пришёл",
        "cancelled": "❌ Отменено",
        "block": "🔧 Техперерыв",
    }
    return mapping.get(status, status)

def within_booking_window(d: date, days_ahead: int = 30) -> bool:
    return today_local() <= d <= (today_local() + timedelta(days=days_ahead))

# ------------------ Контакты ------------------

PHONE_RE = re.compile(r"[\d\+\(\)\-\s]{6,}")

def normalize_phone(p: str) -> str:
    p = p.strip()
    digits = "".join(ch for ch in p if ch.isdigit())

    if len(digits) < 10:
        return ""

    # 9XXXXXXXXX → Россия
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits

    # 8XXXXXXXXXX → Россия
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    return "+" + digits

def looks_like_contact(raw: str) -> bool:
    return bool(PHONE_RE.search(raw.strip()))

def split_contact(raw: str) -> tuple[str, str]:
    raw = raw.strip()

    if "," in raw:
        name, phone = raw.split(",", 1)
    else:
        name, phone = raw, ""

    return name.strip(), normalize_phone(phone)

def price_for(duration: int, sims: int) -> int:
    return PRICES[duration] * sims

def _ensure_tz(dt: datetime) -> datetime:
    return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt.astimezone(TZ)

start = _ensure_tz(start)
end   = _ensure_tz(end)