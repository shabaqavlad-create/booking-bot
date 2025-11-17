import os
import asyncio
import calendar
import signal
import re
import contextlib
from typing import Optional
from datetime import datetime, timedelta, time, timezone, date
import logging

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


import csv
import tempfile

from aiohttp import ClientTimeout
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import FSInputFile
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeChat,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)
from sqlalchemy import (
    BigInteger,
    Integer,
    String,
    DateTime,
    select,
    func,
    text,
    Index,
    CheckConstraint,
    Boolean,  # ← добавили
)


# ====================== CONFIG ======================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан. Добавь его в .env")

ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x}

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан. Добавь его в .env")

# Екатеринбург (UTC+5)
# заменяет TZ
try:
    TZ = ZoneInfo("Asia/Yekaterinburg")
except ZoneInfoNotFoundError:
    try:
        import tzdata  # noqa: F401
        TZ = ZoneInfo("Asia/Yekaterinburg")
    except Exception:
        logging.warning("tzdata не найден, использую фиксированный UTC+5 без переходов.")
        TZ = timezone(timedelta(hours=5))

OPEN_H, OPEN_M = 13, 0
CLOSE_H, CLOSE_M = 23, 0
OPEN_T = time(OPEN_H, OPEN_M, tzinfo=TZ)
CLOSE_T = time(CLOSE_H, CLOSE_M, tzinfo=TZ)

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

MAX_SIMS = 4
HOLD_MINUTES = 30
PRICES = {30: 390, 60: 690, 90: 990, 120: 1290}
MAX_ACTIVE_BOOKINGS_PER_USER = 6  # лимит активных броней

SAFETY_GAP = timedelta(minutes=5)

REMIND_BEFORE = timedelta(hours=2)
AUTOCONFIRM_BEFORE = timedelta(minutes=45)
# user_id -> booking_id, который мы ждём контакт
PENDING_CONTACTS: dict[int, int] = {}
# ================== DATABASE MODELS =================
class Base(DeclarativeBase):
    pass


class Waitlist(Base):
    __tablename__ = "waitlist"
    __table_args__ = (
        Index("ix_waitlist_start_end", "start_at", "end_at"),
        Index("ix_waitlist_active", "active"),
        Index("ix_waitlist_by_time_active", "active", "start_at", "end_at"),
        Index(
            "ux_waitlist_unique_active",
            "user_id", "start_at", "end_at", "duration", "sims_needed",
            unique=True,
            postgresql_where=text("active = true"),
        ),
        CheckConstraint(f"sims_needed >= 1 AND sims_needed <= {MAX_SIMS}", name="ck_waitlist_sims_range"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration: Mapped[int] = mapped_column(Integer, nullable=False)
    sims_needed: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
class Booking(Base):
    __tablename__ = "bookings"
    __table_args__ = (
        Index("ix_bookings_start_end", "start_at", "end_at"),
        Index("ix_bookings_user_active", "user_id", "status", "end_at"),
        Index("ix_bookings_status_start", "status", "start_at"),
        Index("ix_bookings_status_end", "status", "end_at"),
        Index("ix_bookings_status_time", "status", "start_at", "end_at"),
        Index("ix_bookings_user_active_future", "user_id", "status", "end_at"),  # ← новое
        CheckConstraint("sims >= 1", name="ck_sims_ge_1"),
        CheckConstraint("duration IN (30,60,90,120)", name="ck_duration_allowed"),
        CheckConstraint("end_at > start_at", name="ck_end_gt_start"),
        CheckConstraint("price >= 0", name="ck_price_ge_0"),
        CheckConstraint("status IN ('pending','confirmed','cancelled','done','no_show','block')", name="ck_status_enum"),
)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)  # Telegram user id

    # новое 👇
    client_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    client_phone: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sims: Mapped[int] = mapped_column(Integer, nullable=False)
    duration: Mapped[int] = mapped_column(Integer, nullable=False)  # minutes
    price: Mapped[int] = mapped_column(Integer, nullable=False)  # rubles
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)   # pending/confirmed/cancelled
    created_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True),
    server_default=func.now()
)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

# ================ ENGINE & SESSION ==================
engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ====================== BOT CORE ====================
SESSION_TIMEOUT = 120  # сек, важно чтобы было число
session = AiohttpSession(timeout=SESSION_TIMEOUT)
bot = Bot(BOT_TOKEN, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ====================== FSM =========================
# Состояние, когда ждём контакты после выбора слота
class BookingContactForm(StatesGroup):
    waiting_contact = State()

class UpdateContactForm(StatesGroup):
    waiting_new_contact = State()


# Глобальный список фоновых задач, чтобы startup/shutdown могли им управлять
BG_TASKS: list[asyncio.Task] = []
# ----------------- UTILITIES ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("botsim")
logging.getLogger("aiogram").setLevel(logging.DEBUG)


async def setup_commands():
    # команды для обычных пользователей
    user_cmds = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="my", description="Мои активные заявки"),
        BotCommand(command="edit", description="Изменить время заявки"),
        BotCommand(command="cancel", description="Отменить заявку"),
        BotCommand(command="help", description="Помощь"),
    ]

    # дефолтные команды для всех
    await bot.set_my_commands(commands=user_cmds)

    # команды для админов (добавим /day)
    admin_cmds = user_cmds + [
        BotCommand(command="day", description="Расписание по дням"),
    ]

    for admin_id in ADMINS:
        try:
            await bot.set_my_commands(
                commands=admin_cmds,
                scope=BotCommandScopeChat(chat_id=admin_id),
            )
        except Exception:
            # если бот ещё не писал админу
            pass

async def safe_edit_text(msg, *args, **kwargs):
    try:
        return await msg.edit_text(*args, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return None
        raise

async def safe_edit_reply_markup(msg, *args, **kwargs):
    try:
        return await msg.edit_reply_markup(*args, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return None
        raise

def short_booking_line(b: Booking) -> str:
    return (
        f"#{b.id} "
        f"{human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{b.sims} {sims_word(b.sims)} / {b.duration}мин / {b.price}₽ / {human_status(b.status)} | "
        f"{(b.client_name or '-')} {(b.client_phone or '-')}"
    )


PHONE_RE = re.compile(r"[\d\+\(\)\-\s]{6,}")


def normalize_phone(p: str) -> str:
    """Нормализует номер телефона в формат +7XXXXXXXXXX."""
    p = p.strip()
    digits = "".join(ch for ch in p if ch.isdigit())
    if len(digits) < 10:
        return ""
    # 10-значный без кода страны, начинающийся с 9 -> Россия
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    # 11-значный, начинается с 8 -> Россия
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    # Всегда с +
    return "+" + digits

def looks_like_contact(raw: str) -> bool:
    raw = raw.strip()
    return bool(PHONE_RE.search(raw))

def split_contact(raw: str) -> tuple[str, str]:
    raw = raw.strip()
    if "," in raw:
        name_part, phone_part = raw.split(",", 1)
    else:
        name_part, phone_part = raw, ""
    return name_part.strip(), normalize_phone(phone_part)

def build_day_timetable(bookings: list[Booking], target_date: date) -> str:
    """
    Расписание на день (шаг 30 мин) с пометками статуса:
    ⏳ — pending, ✅ — confirmed. Показываем занятость и кто занимает.
    """
    day_start = datetime.combine(target_date, OPEN_T)
    day_end = datetime.combine(target_date, CLOSE_T)

    slot_len = timedelta(minutes=30)
    status_icon = {
        "pending": "⏳",
        "confirmed": "✅",
        "cancelled": "❌",
        "block": "🔧",
    }

    # Опционально: сортируем брони по началу, чтобы отображение было стабильным
    bookings_sorted = sorted(bookings, key=lambda b: (b.start_at, b.id))

    lines: list[str] = []
    cur = day_start
    while cur < day_end:
        cur_end = cur + slot_len

        # Брони, пересекающие слот
        overlapping: list[Booking] = []
        for b in bookings_sorted:
            # было: if b.status in ("pending", "confirmed"):
            if b.status in ("pending", "confirmed", "block"):
                if b.start_at < cur_end and b.end_at > cur:
                    overlapping.append(b)

        # Суммарная занятость в симах
        total_sims_busy = sum(b.sims for b in overlapping)
        if total_sims_busy > MAX_SIMS:
            total_sims_busy = MAX_SIMS  # на всякий случай

        # Кого показать в строке слота
        if overlapping:
            who_parts = []
            for b in overlapping:
                nm = b.client_name or "?"
                icon = status_icon.get(b.status, "")
                who_parts.append(f"#{b.id} {nm}({b.sims},{icon})")
            who_str = ", ".join(who_parts)
        else:
            who_str = "—"

        load_note = "FULL" if total_sims_busy >= MAX_SIMS else f"{total_sims_busy}/{MAX_SIMS}"

        lines.append(
            f"{cur.astimezone(TZ).strftime('%H:%M')}–{cur_end.astimezone(TZ).strftime('%H:%M')}  "
            f"занято {load_note}  {who_str}"
        )

        cur = cur_end

    header = (
        f"Расписание по 30 минут ({target_date.strftime('%d.%m.%Y')}):\n"
        f"Легенда статуса: ⏳ — ожидает подтверждения, ✅ — подтверждено, 🔧 — техперерыв"
    )
    return header + "\n" + "\n".join(lines)

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
        "block": "🔧 Техперерыв",  # ← добавить
    }
    return mapping.get(status, status)

def today_local() -> date:
    return datetime.now(TZ).date()

def within_booking_window(d: date, days_ahead: int = 30) -> bool:
    return today_local() <= d <= (today_local() + timedelta(days=days_ahead))

RU_MONTHS = [
    "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

def build_month_kb(year: int, month: int, duration: int):
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdayscalendar(year, month)

    rows = [[InlineKeyboardButton(text=f"{RU_MONTHS[month]} {year}", callback_data="noop")]]

    rows.append([InlineKeyboardButton(text=t, callback_data="noop") for t in ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]])

    for w in weeks:
        row = []
        for day in w:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="noop"))
                continue
            d = date(year, month, day)
            if within_booking_window(d):
                row.append(
                    InlineKeyboardButton(
                        text=str(day),
                        callback_data=f"book:date:{d.isoformat()}:{duration}"
                    )
                )
            else:
                row.append(InlineKeyboardButton(text="·", callback_data="noop"))
        rows.append(row)

    cur_first = date(year, month, 1)
    prev_month = (cur_first - timedelta(days=1)).replace(day=1)
    next_month = (cur_first + timedelta(days=32)).replace(day=1)

    nav = []
    if prev_month >= today_local().replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"cal:page:{prev_month.year}-{prev_month.month}:{duration}"
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    nav.append(InlineKeyboardButton(text="Закрыть", callback_data=f"book:dur:{duration}"))

    last_allowed = today_local() + timedelta(days=30)
    if next_month <= last_allowed.replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"cal:page:{next_month.year}-{next_month.month}:{duration}"
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_month_kb_edit(year: int, month: int, bid: int, duration: int, sims: int):
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdayscalendar(year, month)

    rows = [[InlineKeyboardButton(text=f"{RU_MONTHS[month]} {year}", callback_data="noop")]]

    rows.append([InlineKeyboardButton(text=t, callback_data="noop") for t in ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]])

    for w in weeks:
        row = []
        for day in w:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="noop"))
                continue
            d = date(year, month, day)
            if within_booking_window(d):
                row.append(
                    InlineKeyboardButton(
                        text=str(day),
                        callback_data=f"edit:date:{bid}:{d.isoformat()}:{duration}:{sims}"
                    )
                )
            else:
                row.append(InlineKeyboardButton(text="·", callback_data="noop"))
        rows.append(row)

    cur_first = date(year, month, 1)
    prev_month = (cur_first - timedelta(days=1)).replace(day=1)
    next_month = (cur_first + timedelta(days=32)).replace(day=1)

    nav = []
    if prev_month >= today_local().replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"editcal:page:{bid}:{prev_month.year}-{prev_month.month}:{duration}:{sims}"
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    nav.append(
        InlineKeyboardButton(
            text="Закрыть",
            callback_data="noop"
        )
    )

    last_allowed = today_local() + timedelta(days=30)
    if next_month <= last_allowed.replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"editcal:page:{bid}:{next_month.year}-{next_month.month}:{duration}:{sims}"
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def localize(dt: datetime) -> datetime:
    return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt.astimezone(TZ)

def gen_slots(day_dt: datetime, step_min=30):
    base = localize(day_dt).date()
    start_dt = datetime.combine(base, OPEN_T)
    end_dt   = datetime.combine(base, CLOSE_T)
    cur = start_dt
    step = timedelta(minutes=step_min)
    slots = []
    while cur + step <= end_dt:
        slots.append(cur)
        cur += step
    return slots

def human(dt: datetime) -> str:
    return localize(dt).strftime("%d.%m %H:%M")

def price_for(duration: int, sims: int) -> int:
    return PRICES[duration] * sims

async def ensure_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def free_sims_for_interval(start_at: datetime, end_at: datetime, exclude_id: Optional[int] = None) -> int:
    start_at, end_at = localize(start_at), localize(end_at)
    async with SessionLocal() as s:
        # зачистка просроченных pending заявок
        await s.execute(
    text("""UPDATE bookings
            SET status='cancelled'
            WHERE status='pending'
              AND expires_at IS NOT NULL
              AND expires_at < :now"""),
    {"now": datetime.now(TZ)}
)
        await s.commit()

        q = select(func.coalesce(func.sum(Booking.sims), 0)).where(
    Booking.status.in_(("pending", "confirmed", "block")),
    Booking.start_at < end_at,
    Booking.end_at > start_at
)
        if exclude_id is not None:
            q = q.where(Booking.id != exclude_id)

        total_taken = (await s.execute(q)).scalar_one()
        free = MAX_SIMS - int(total_taken)
        return max(0, free)

def confirm_user_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить в календарь (.ics)",
                    callback_data=f"ics:send:{bid}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="📄 Мои заявки",
                    callback_data="my:list"
                )
            ],
        ]
    )

# --------------- KEYBOARDS & MENUS ------------------
def main_menu_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Забронировать", callback_data="book:start")],
            [InlineKeyboardButton(text="📄 Мои заявки", callback_data="my:list")],
            [
                InlineKeyboardButton(text="💳 Тарифы", callback_data="tariffs"),
                InlineKeyboardButton(text="🕒 Часы работы", callback_data="hours")
            ],
            [InlineKeyboardButton(text="📍 Адрес", callback_data="address")],  # 👈 добавили
            [InlineKeyboardButton(text="📚 Помощь", callback_data="help:open")],
            [InlineKeyboardButton(text="📞 Связаться", callback_data="contact")]
        ]
    )


# ===================== HANDLERS =====================

@dp.message(Command("ics"))
async def ics_cmd(m: Message):
    parts = m.text.split()
    if len(parts)!=2 or not parts[1].isdigit():
        await m.answer("Использование: /ics ID"); return
    bid = int(parts[1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != m.from_user.id:
            await m.answer("Заявка не найдена."); return
    if b.status not in ("confirmed", "done"):
        await m.answer("ICS доступен после подтверждения."); return

    import uuid, tempfile
    uid = uuid.uuid4().hex
    ics = (
        "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//simclub//ru//\nBEGIN:VEVENT\n"
        f"UID:{uid}\nDTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTSTART:{b.start_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTEND:{b.end_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}\n"
        "SUMMARY:Симрейсинг\nLOCATION:Екатеринбург, Академический\n"
        f"DESCRIPTION:{b.sims} сим(ов), {b.duration} мин\nEND:VEVENT\nEND:VCALENDAR\n"
    )

    path = None
    try:
        fd, path = tempfile.mkstemp(prefix=f"booking_{bid}_", suffix=".ics")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(ics)
        await m.answer_document(FSInputFile(path), caption=f"Календарь для брони #{bid}")
    finally:
        if path and os.path.exists(path):
            os.remove(path)

@dp.callback_query(F.data.startswith("contact:ask:"))
async def contact_ask_cb(c: CallbackQuery, state: FSMContext):
    bid = int(c.data.split(":")[-1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != c.from_user.id:
            await c.answer("Заявка не найдена", show_alert=True); return
    await state.update_data(bid=bid)
    await state.set_state(UpdateContactForm.waiting_new_contact)
    await c.message.answer("Пришли новые данные: Имя, телефон\nНапример: Игорь, +7 999 123-45-67")
    await c.answer()

@dp.callback_query(F.data.startswith("cancel:ask:"))
async def cancel_ask_cb(c: CallbackQuery):
    bid = int(c.data.split(":")[-1])
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Да, отменить", callback_data=f"cancel:do:{bid}"),
        InlineKeyboardButton(text="Нет", callback_data="back_home"),
    ]])
    await c.message.answer(f"Точно отменить заявку #{bid}?", reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("cancel:do:"))
async def cancel_do_cb(c: CallbackQuery):
    bid = int(c.data.split(":")[-1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != c.from_user.id:
            await c.answer("Заявка не найдена", show_alert=True); return
        if datetime.now(TZ) >= b.start_at.astimezone(TZ):
            await c.answer("Нельзя отменить — время уже наступило.", show_alert=True); return
        if b.status == "cancelled":
            await c.answer("Уже отменена"); return
        b.status = "cancelled"; b.expires_at = None
        await s.commit()
    await c.message.answer(f"❌ Заявка #{bid} отменена.")
    await c.answer()

@dp.callback_query(F.data == "help:open")
async def help_open_cb(c: CallbackQuery):
    text = (
        "🧭 <b>Помощь</b>\n\n"
        "• 📅 Бронирование: выберите длительность и время, оставьте контакт — админ подтвердит.\n"
        "• 📄 Мои заявки: смотрите статус, меняйте время (если ещё pending) или отменяйте.\n"
        "• 🔔 Уведомить: бот сообщит, когда освободится нужный слот.\n"
        "• 💳 Тарифы: цена за 1 симулятор.\n"
        "• 🕒 Работаем: 13:00–23:00 (Екатеринбург, UTC+5).\n\n"
        "Команды:\n"
        "/start /book /my /edit /cancel /contact /promo\n"
    )
    await safe_edit_text(c.message, text, reply_markup=main_menu_kb())
    await c.answer()

@dp.message(Command("block"))
async def block_cmd(m: Message):
    if m.from_user.id not in ADMINS:
        await m.answer("Команда доступна только администратору.")
        return

    # /block YYYY-MM-DD HH:MM DURATION SIMS [NOTE...]
    parts = m.text.strip().split(maxsplit=5)
    if len(parts) < 5:
        await m.answer("Использование:\n/block YYYY-MM-DD HH:MM DURATION SIMS [КОММЕНТАРИЙ]")
        return

    _, d_str, t_str, dur_str, sims_str, *note_rest = parts
    try:
        duration = int(dur_str)
        sims = int(sims_str)
        if duration not in PRICES or not (1 <= sims <= MAX_SIMS):
            raise ValueError
        start_local = datetime.strptime(d_str + " " + t_str, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        end_local = start_local + timedelta(minutes=duration)
    except Exception:
        await m.answer("Неверные параметры.")
        return

    note = note_rest[0] if note_rest else ""
    # проверим пересечения по мощностям
    if await free_sims_for_interval(start_local, end_local) < sims:
        await m.answer("Недостаточно свободных симов для техперерыва в это окно.")
        return

    async with SessionLocal() as s:
        b = Booking(
            user_id=0,
            client_name=f"Техперерыв {note}".strip(),
            client_phone=None,
            start_at=start_local,
            end_at=end_local,
            sims=sims,
            duration=duration,
            price=0,
            status="block",
            expires_at=None,
        )
        s.add(b)
        await s.commit()
        await s.refresh(b)

    await m.answer(f"🔧 Добавлен техперерыв #{b.id}: {human(start_local)}–{end_local.astimezone(TZ).strftime('%H:%M')} | {sims} {sims_word(sims)}")

@dp.message(Command("unblock"))
async def unblock_cmd(m: Message):
    if m.from_user.id not in ADMINS:
        await m.answer("Команда доступна только администратору.")
        return
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /unblock ID")
        return
    bid = int(parts[1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.status != "block":
            await m.answer("Техперерыв не найден.")
            return
        await s.delete(b)
        await s.commit()
    await m.answer(f"✅ Техперерыв #{bid} удалён.")

@dp.message(Command("wait"))
async def wait_cmd(m: Message):
    """
    /wait YYYY-MM-DD HH:MM DURATION SIMS
    пример: /wait 2025-11-02 18:00 60 2
    """
    parts = m.text.strip().split()
    if len(parts) != 5:
        await m.answer("Использование:\n/wait YYYY-MM-DD HH:MM DURATION SIMS\nНапр.: /wait 2025-11-02 18:00 60 2")
        return

    _, d_str, t_str, dur_str, sims_str = parts
    try:
        duration = int(dur_str)
        sims_needed = int(sims_str)
        if duration not in PRICES or not (1 <= sims_needed <= MAX_SIMS):
            raise ValueError
        start_local = datetime.strptime(d_str + " " + t_str, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    except Exception:
        await m.answer("Не получилось разобрать параметры. Проверь формат и допустимые значения.")
        return

    # проверим в рабочие часы и в окно бронирования
    close_dt = datetime.combine(start_local.date(), CLOSE_T)
    if start_local < datetime.now(TZ):
        await m.answer("Нельзя подписаться на прошлое время 🙂")
        return
    if start_local.time() < OPEN_T or (start_local + timedelta(minutes=duration)) > (close_dt - SAFETY_GAP):
        await m.answer("Время вне рабочих часов или слишком близко к закрытию.")
        return

    end_local = start_local + timedelta(minutes=duration)

    async with SessionLocal() as s:
        w = Waitlist(
            user_id=m.from_user.id,
            start_at=start_local,
            end_at=end_local,
            duration=duration,
            sims_needed=sims_needed,
            active=True,
        )
        s.add(w)
        await s.commit()
        await s.refresh(w)

    await m.answer(
        f"🔔 Подписка оформлена #{w.id}\n"
        f"{human(start_local)}–{end_local.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims_needed} {sims_word(sims_needed)} | {duration} мин\n"
        f"Сообщу, если окно освободится 👌"
    )


@dp.message(Command("unwait"))
async def unwait_cmd(m: Message):
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /unwait ID (например /unwait 12)")
        return

    wid = int(parts[1])
    async with SessionLocal() as s:
        w = await s.get(Waitlist, wid)
        if not w or w.user_id != m.from_user.id:
            await m.answer("Подписка не найдена.")
            return
        if w.active is False:
            await m.answer("Эта подписка уже отключена.")
            return
        w.active = False
        await s.commit()

    await m.answer(f"❎ Подписка #{wid} отключена.")

@dp.message(CommandStart())
async def start_cmd(m: Message):
    await m.answer(
        "🏁 Привет! Это бот симрейсинг-клуба (4 симулятора).\n"
        "Бронируй слот — администратор подтвердит заявку.\n",
        reply_markup=main_menu_kb()
    )

@dp.callback_query(F.data == "hours")
async def hours_cb(c: CallbackQuery):
    await safe_edit_text(
        c.message,
        "🕒 Часы работы: ежедневно <b>13:00–23:00</b> (Екатеринбург, UTC+5).",
        reply_markup=main_menu_kb()
    )
    await c.answer()

@dp.callback_query(F.data == "tariffs")
async def tariffs_cb(c: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{d} мин — {PRICES[d]} ₽/сим", callback_data=f"tariffs:dur:{d}")]
            for d in (30, 60, 90, 120)
        ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home")]]
    )
    await safe_edit_text(c.message, "💳 Выбери длительность, посчитаю итог:", reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("tariffs:dur:"))
async def tariffs_pick_qty(c: CallbackQuery):
    duration = int(c.data.split(":")[-1])
    rows = [[InlineKeyboardButton(
        text=f"{n} — {price_for(duration, n)} ₽ итого",
        callback_data=f"tariffs:qty:{duration}:{n}"
    )] for n in range(1, MAX_SIMS+1)]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="tariffs")])
    await safe_edit_text(
        c.message,
        f"Длительность: {duration} мин\nЦена за 1 сим: {PRICES[duration]} ₽\nВыбери количество:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

@dp.callback_query(F.data.startswith("tariffs:qty:"))
async def tariffs_show_total(c: CallbackQuery):
    _, _, duration, sims = c.data.split(":")
    duration, sims = int(duration), int(sims)
    total = price_for(duration, sims)
    await safe_edit_text(
        c.message,
        (f"🧮 Итого: <b>{total} ₽</b>\n\n"
         f"• Длительность: {duration} мин\n"
         f"• Симуляторов: {sims} {sims_word(sims)}\n"
         f"• Тариф: {PRICES[duration]} ₽/сим\n\n"
         f"Можно перейти к брони: /book"),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад: количество", callback_data=f"tariffs:dur:{duration}")],
                [InlineKeyboardButton(text="🏁 Перейти к бронированию", callback_data=f"book:dur:{duration}")]
            ]
        )
    )
    await c.answer()

@dp.callback_query(F.data == "contact")
async def contact_cb(c: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗺 Открыть карту", url=ADDRESS_MAP_URL)],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_home")]
        ]
    )
    await safe_edit_text(
        c.message,
        "📞 Связаться с администратором:\n"
        "• Телефон: +7 953 046-36-54\n"
        "• Telegram: @shaba_V\n\n"
        f"📍 Адрес: {ADDRESS_FULL} ({ADDRESS_AREA})",
        reply_markup=kb
    )
    await c.answer()

@dp.callback_query(F.data == "address")
async def address_cb(c: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗺 Открыть карту", url=ADDRESS_MAP_URL)],
            [InlineKeyboardButton(text="🧭 Как добраться", callback_data="howto")],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_home")]
        ]
    )
    await safe_edit_text(
        c.message,
        f"📍 {ADDRESS_FULL}\nРайон: {ADDRESS_AREA}\n\n"
        "Нажми «Открыть карту», чтобы построить маршрут в Яндекс.Картах.",
        reply_markup=kb
    )
    await c.answer()

@dp.callback_query(F.data == "howto")
async def howto_cb(c: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к адресу", callback_data="address")]]
    )
    await safe_edit_text(
        c.message,
        HOWTO_TEXT,
        reply_markup=kb
    )
    await c.answer()

# -------- Booking flow --------
@dp.callback_query(F.data == "book:start")
async def book_start(c: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{d} мин ({PRICES[d]} ₽/сим)",
                    callback_data=f"book:dur:{d}"
                )
            ] for d in (60, 90, 120, 30)
        ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home")]]
    )
    await safe_edit_text(c.message, "Выбери длительность:", reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data == "back_home")
async def back_home(c: CallbackQuery):
    await safe_edit_text(c.message, "Главное меню:", reply_markup=main_menu_kb())
    await c.answer()

@dp.callback_query(F.data.startswith("book:dur:"))
async def book_pick_day(c: CallbackQuery):
    duration = int(c.data.split(":")[-1])

    if duration not in PRICES:
        await c.answer("Неверная длительность", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Сегодня", callback_data=f"book:day:0:{duration}")],
            [InlineKeyboardButton(text="Завтра", callback_data=f"book:day:1:{duration}")],
            [InlineKeyboardButton(text="Послезавтра", callback_data=f"book:day:2:{duration}")],
            [InlineKeyboardButton(text="📅 Другая дата", callback_data=f"cal:open:{duration}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="book:start")]
        ]
    )
    await safe_edit_text(
        c.message,
        f"Длительность — <b>{duration} мин</b>\nВыбери день:",
        reply_markup=kb
    )
    await c.answer()

@dp.callback_query(F.data.startswith("cal:open:"))
async def cal_open(c: CallbackQuery):
    duration = int(c.data.split(":")[-1])
    if duration not in PRICES:
        await c.answer("Неверная длительность", show_alert=True)
        return

    d = datetime.now(TZ).date()
    kb = build_month_kb(d.year, d.month, duration)
    await safe_edit_text(
        c.message,
        f"Выбери дату (до 30 дней вперёд). Длительность: {duration} мин",
        reply_markup=kb
    )
    await c.answer()

@dp.callback_query(F.data.startswith("cal:page:"))
async def cal_page(c: CallbackQuery):
    _, _, ym, duration = c.data.split(":")
    y, m = map(int, ym.split("-"))
    duration = int(duration)

    if duration not in PRICES:
        await c.answer("Неверная длительность", show_alert=True)
        return

    kb = build_month_kb(y, m, duration)
    await safe_edit_reply_markup(c.message, reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("book:date:"))
async def book_date_pick(c: CallbackQuery):
    _, _, iso, duration = c.data.split(":")
    duration = int(duration)
    if duration not in PRICES:
        await c.answer("Неверная длительность", show_alert=True)
        return

    y, m, d = map(int, iso.split("-"))
    picked_date = date(y, m, d)

    base = datetime.combine(picked_date, time(0,0,tzinfo=TZ))

    slots = gen_slots(base)
    now = datetime.now(TZ)
    close_dt = datetime.combine(base.date(), CLOSE_T)
    today = today_local()

    slots = [
        s for s in slots
        if (base.date() != today or s > now + timedelta(minutes=10))
        and (s + timedelta(minutes=duration) <= (close_dt - SAFETY_GAP))
    ]

    rows = []
    for s in slots:
        end = s + timedelta(minutes=duration)
        free = await free_sims_for_interval(s, end)
        label = f"{s.strftime('%H:%M')} ({free} {sims_word(free)})"
        if free > 0:
            rows.append([InlineKeyboardButton(
                text=label,
                callback_data=f"book:time:{int(s.timestamp())}:{duration}:X"
            )])
        else:
            rows.append([
    InlineKeyboardButton(text=label, callback_data="noop"),
    InlineKeyboardButton(
        text="🔔 Уведомить",
        callback_data=f"wait:ask:{int(s.timestamp())}:{duration}"
    )
])

    if not rows:
        rows.append([InlineKeyboardButton(text="Нет доступных слотов", callback_data="noop")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cal:open:{duration}")])

    await safe_edit_text(
        c.message,
        f"Выбери время на <b>{base.strftime('%d.%m')}</b> (длительность {duration} мин):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

@dp.callback_query(F.data.startswith("book:day:"))
async def book_pick_time(c: CallbackQuery):
    _, _, day_offset, duration = c.data.split(":")
    day_offset, duration = int(day_offset), int(duration)

    if duration not in PRICES or day_offset not in (0, 1, 2):
        await c.answer("Некорректные параметры", show_alert=True)
        return

    base = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=day_offset)

    slots = gen_slots(base)
    now_local = datetime.now(TZ)
    close_dt = datetime.combine(base.date(), CLOSE_T)

    slots = [
        s for s in slots
        if (day_offset != 0 or s > now_local + timedelta(minutes=10))
        and (s + timedelta(minutes=duration) <= (close_dt - SAFETY_GAP))
    ]

    rows = []
    for s in slots:
        end = s + timedelta(minutes=duration)
        free = await free_sims_for_interval(s, end)
        label = f"{s.strftime('%H:%M')} ({free} {sims_word(free)})"
        if free > 0:
            rows.append([InlineKeyboardButton(
                text=label,
                callback_data=f"book:time:{int(s.timestamp())}:{duration}:{day_offset}"
            )])
        else:
            # добавили вторую кнопку «Уведомить»
            rows.append([
                InlineKeyboardButton(text=label, callback_data="noop"),
                InlineKeyboardButton(
                    text="🔔 Уведомить",
                    callback_data=f"wait:ask:{int(s.timestamp())}:{duration}"
                ),
            ])

    if not rows:
        rows.append([InlineKeyboardButton(text="Нет доступных слотов", callback_data="noop")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"book:dur:{duration}")])

    await safe_edit_text(
        c.message,
        f"Выбери время на <b>{base.strftime('%d.%m')}</b> (длительность {duration} мин):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

@dp.callback_query(F.data.startswith("wait:ask:"))
async def wait_ui_ask_sims(c: CallbackQuery):
    # wait:ask:{ts}:{duration}
    _, _, ts, duration = c.data.split(":")
    ts_i = int(ts)
    duration_i = int(duration)
    rows = [[InlineKeyboardButton(text=str(n), callback_data=f"wait:set:{ts}:{duration}:{n}")]
            for n in range(1, MAX_SIMS + 1)]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"book:date:{datetime.fromtimestamp(ts_i, tz=TZ).date().isoformat()}:{duration_i}")])
    await safe_edit_text(
        c.message,
        "Сколько симов нужно для уведомления?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

@dp.callback_query(F.data.startswith("wait:set:"))
async def wait_ui_set(c: CallbackQuery):
    # wait:set:{ts}:{duration}:{sims}
    _, _, ts, duration, sims = c.data.split(":")
    start_local = datetime.fromtimestamp(int(ts), tz=TZ)
    duration_i = int(duration)
    sims_i = int(sims)
    end_local = start_local + timedelta(minutes=duration_i)

    # быстрая валидация рабочих часов
    close_dt = datetime.combine(start_local.date(), CLOSE_T)
    if start_local < datetime.now(TZ) or (start_local + timedelta(minutes=duration_i)) > (close_dt - SAFETY_GAP):
        await c.answer("Время вне рабочих часов/слишком поздно.", show_alert=True)
        return

    async with SessionLocal() as s:
        w = Waitlist(
            user_id=c.from_user.id,
            start_at=start_local,
            end_at=end_local,
            duration=duration_i,
            sims_needed=sims_i,
            active=True,
        )
        s.add(w)
        await s.commit()
        await s.refresh(w)

    await safe_edit_text(
        c.message,
        (f"🔔 Подписка оформлена #{w.id}\n"
         f"{human(start_local)}–{end_local.astimezone(TZ).strftime('%H:%M')} | "
         f"{sims_i} {sims_word(sims_i)} | {duration_i} мин\n"
         "Сообщу, если окно освободится 👌")
    )
    await c.answer("Готово!")

@dp.callback_query(F.data.startswith("book:time:"))
async def book_pick_sims(c: CallbackQuery):
    _, _, ts, duration, day_marker = c.data.split(":")

    try:
        duration = int(duration)
        _ = int(ts)
    except ValueError:
        await c.answer("Некорректные параметры", show_alert=True)
        return

    if duration not in PRICES:
        await c.answer("Неверная длительность", show_alert=True)
        return

    start = datetime.fromtimestamp(int(ts), tz=TZ)
    end = start + timedelta(minutes=duration)

    free = await free_sims_for_interval(start, end)
    if free <= 0:
        await c.answer("Нет свободных симов на это время", show_alert=True)
        return

    rows = [[
    InlineKeyboardButton(
        text=f"{n} — {price_for(duration, n)} ₽ итого",
        callback_data=f"book:qty:{ts}:{duration}:{n}:{day_marker}"
    )
] for n in range(1, min(MAX_SIMS, free) + 1)]
    if day_marker == "X":
        back_cb = f"book:date:{start.date().isoformat()}:{duration}"
    else:
        back_cb = f"book:day:{int(day_marker)}:{duration}"

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)])

    await safe_edit_text(
        c.message,
        f"Свободно симов: <b>{free}</b>\nСколько забронировать?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

# ---------- ВАЖНО: теперь мы не создаём бронь сразу! ----------
# Мы сохраняем выбор юзера во FSM и спрашиваем контакт.

@dp.callback_query(F.data.startswith("book:qty:"))
async def book_qty_confirm_ask_contact(c: CallbackQuery, state: FSMContext):
    _, _, ts, duration, sims, _day_marker = c.data.split(":")

    try:
        duration = int(duration)
        sims = int(sims)
        start_ts = int(ts)
    except ValueError:
        await c.answer("Некорректные параметры", show_alert=True)
        return

    if duration not in PRICES or not (1 <= sims <= MAX_SIMS):
        await c.answer("Неверные параметры", show_alert=True)
        return

    start = datetime.fromtimestamp(start_ts, tz=TZ)
    end = start + timedelta(minutes=duration)

    # лимит активных заявок на юзера
    async with SessionLocal() as s:
        active_cnt_q = (
            select(func.count())
            .select_from(Booking)
            .where(
                Booking.user_id == c.from_user.id,
                Booking.status.in_(("pending", "confirmed")),
                Booking.end_at > datetime.now(TZ),
            )
        )
        active_cnt = (await s.execute(active_cnt_q)).scalar_one()

    if active_cnt >= MAX_ACTIVE_BOOKINGS_PER_USER:
        await c.answer(
            f"У тебя уже {active_cnt} активных броней. Лимит {MAX_ACTIVE_BOOKINGS_PER_USER}.",
            show_alert=True
        )
        return

    # повторная проверка свободных симов
    if await free_sims_for_interval(start, end) < sims:
        await c.answer("Упс, слот только что заняли. Выбери другое время.", show_alert=True)
        return

    base_price = price_for(duration, sims)
    final_price, _promo_code = apply_promo(base_price, c.from_user.id)  # ← распаковка
    price = final_price

    # Кладём это всё во временное состояние пользователя:
    await state.update_data(
        start_ts=start_ts,
        duration=duration,
        sims=sims,
        end_ts=int(end.timestamp()),
        price=price,
    )

    # Спрашиваем контакты
    await state.set_state(BookingContactForm.waiting_contact)
    await safe_edit_text(
        c.message,
        (
            "Последний шаг 👇\n"
            "Напиши как с тобой связаться, пожалуйста.\n"
            "Формат: Имя, телефон\n\n"
            "Например:\n"
            "Игорь, +7 999 123-45-67"
        )
    )
    await c.answer("Жду контакт 👌")

@dp.message(UpdateContactForm.waiting_new_contact)
async def update_contact_finish(m: Message, state: FSMContext):
    client_name, client_phone = split_contact(m.text)

    data = await state.get_data()
    bid = data["bid"]

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b or b.user_id != m.from_user.id:
            await m.answer("Что-то пошло не так, заявка больше недоступна.")
            await state.clear()
            return

        # тут больше НЕ проверяем b.status == "pending"
        b.client_name = client_name
        b.client_phone = client_phone
        await s.commit()
        await s.refresh(b)

        start_at = b.start_at
        end_at = b.end_at
        sims = b.sims
        dur = b.duration
        price = b.price

    await m.answer(
        "Контакт обновлён ✅\n\n"
        f"Заявка #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Теперь указано:\n"
        f"{client_name}, {client_phone}\n\n"
        "Администратор получил новые данные 👌"
    )

    admin_text = (
        f"✏️ Обновлён контакт в заявке #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Новый контакт: {client_name}, {client_phone}"
    )
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, admin_text)
        except Exception:
            pass

    await state.clear()

REF_DISCOUNT_PERCENT = 5
REF_TOTAL_LIMIT = 200   # общий лимит по каждому реф-коду
REF_PER_USER_LIMIT = 1  # один раз для каждого «друга»
REF_PREFIX = "REF"

@dp.message(Command("ref"))
async def ref_cmd(m: Message):
    owner_id = m.from_user.id
    code = f"{REF_PREFIX}{owner_id}"
    # создаём (или обновляем) правило на лету
    PROMO_RULES[code] = {
        "kind": "percent",
        "value": REF_DISCOUNT_PERCENT,
        "until": date(2099, 1, 1),
        "one_time": False,                 # для друзей многоразово (но см. per_user_limit)
        "per_user_limit": REF_PER_USER_LIMIT,
        "total_limit": REF_TOTAL_LIMIT,
        "min_total": 0,
        "owner_id": owner_id,              # владелец не может применить сам
    }
    await m.answer(
        "Твой реферальный код:\n"
        f"<code>{code}</code>\n\n"
        f"Даст другу {REF_DISCOUNT_PERCENT}% скидки.\n"
        f"Каждый новый пользователь может применить 1 раз.\n"
        f"Ты — не можешь использовать свой код."
    )

# ---------- Пользователь прислал контакт (имя + телефон) ----------
@dp.message(BookingContactForm.waiting_contact)
async def book_finalize(m: Message, state: FSMContext):
    client_name, client_phone = split_contact(m.text)

    data = await state.get_data()
    start_ts = data["start_ts"]
    end_ts = data["end_ts"]
    duration = data["duration"]
    sims = data["sims"]
    price = data["price"]

    start = datetime.fromtimestamp(start_ts, tz=TZ)
    end = datetime.fromtimestamp(end_ts, tz=TZ)
    
    # финальная проверка слота на всякий случай
    if await free_sims_for_interval(start, end) < sims:
        await m.answer("😔 Пока ты писал контакт, слот заняли. Попробуй снова /start")
        await state.clear()
        return

    async with SessionLocal() as s:
        b = Booking(
            user_id=m.from_user.id,
            client_name=client_name,
            client_phone=client_phone,
            start_at=start,
            end_at=end,
            sims=sims,
            duration=duration,
            price=price,
            status="pending",
            expires_at=datetime.now(TZ) + timedelta(minutes=HOLD_MINUTES),
        )
        s.add(b)
        await s.commit()
        await s.refresh(b)
    booking_id = b.id
    expires_local = b.expires_at.astimezone(TZ)

    # учёт промокода (если был в pending)
    applied = PROMOS_PENDING.pop(m.from_user.id, None)
    promo_note = ""
    if applied:
        code = applied["code"]
        rule = applied["rule"]
        _promo_mark_used(code, m.from_user.id, rule)
        promo_note = f" (с промокодом {code})"

    # Сообщаем юзеру
    await m.answer(
        f"📝 Заявка #{booking_id} отправлена администратору.\n\n"
        f"Дата: <b>{human(start)}–{end.strftime('%H:%M')}</b>\n"
        f"Симуляторов: <b>{sims} {sims_word(sims)}</b>\n"
        f"Длительность: <b>{duration} мин</b>\n"
        f"Сумма: <b>{price} ₽</b>{promo_note}\n"
        f"Контакт: <b>{client_name}</b>, {client_phone}\n\n"
        f"Статус: <b>ожидает подтверждения</b> (до {expires_local.strftime('%H:%M')})."
    )

    # уведомление админам
    kb = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(
                text="✅ Подтвердить",
                callback_data=f"admin:approve:{booking_id}"
            ),
            InlineKeyboardButton(
                text="❌ Отклонить",
                callback_data=f"admin:reject:{booking_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                text="📞 Контакт",
                callback_data=f"admin:contact:{booking_id}"
            ),
            InlineKeyboardButton(
                text="✏ Запросить контакт",
                callback_data=f"admin:askcontact:{booking_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                text="🚫 Не пришёл",
                callback_data=f"admin:noshow:{booking_id}"
            ),
            InlineKeyboardButton(
                text="🏁 Пришёл",
                callback_data=f"admin:done:{booking_id}"
            ),
        ],
    ]
)
    uname = m.from_user.username or m.from_user.full_name
    txt = (
        f"🆕 Заявка #{booking_id} от @{uname}\n"
        f"{human(start)}–{end.strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {duration} мин | {price} ₽{promo_note}\n"
        f"Имя: {client_name}\n"
        f"Тел: {client_phone}"
    )
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, txt, reply_markup=kb)
        except Exception:
            pass

    # чистим состояние
    await state.clear()
    await m.answer(
    "Готово 🙌 Заявка отправлена админу. "
    "Если нужно посмотреть статус — команда /my.\n"
    "Вернуться в меню — /start"
)

# -------- Edit booking (время) --------
@dp.callback_query(F.data.startswith("editcal:open:"))
async def edit_cal_open(c: CallbackQuery):
    # editcal:open:{bid}:{duration}:{sims}
    _, _, bid_str, duration_str, sims_str = c.data.split(":")
    bid = int(bid_str)
    duration = int(duration_str)
    sims = int(sims_str)

    if duration not in PRICES or not (1 <= sims <= MAX_SIMS):
        await c.answer("Некорректные параметры", show_alert=True)
        return

    d = datetime.now(TZ).date()
    kb = build_month_kb_edit(d.year, d.month, bid, duration, sims)

    await safe_edit_text(
        c.message,
        f"Выбери новую дату (до 30 дней вперёд).\n"
        f"Заявка #{bid}, {sims} {sims_word(sims)}, {duration} мин:",
        reply_markup=kb
    )
    await c.answer()

@dp.message(Command("csv"))
async def csv_cmd(m: Message):
    if m.from_user.id not in ADMINS:
        await m.answer("Команда доступна только администратору.")
        return

    parts = m.text.strip().split()
    if len(parts) != 2:
        await m.answer("Использование: /csv YYYY-MM или /csv YYYY-MM-DD")
        return

    arg = parts[1]
    try:
        if len(arg) == 7:  # YYYY-MM
            year, month = map(int, arg.split("-"))
            start = datetime(year, month, 1, tzinfo=TZ)
            end = (start + timedelta(days=32)).replace(day=1)
            title = f"{year:04d}-{month:02d}"
        else:  # YYYY-MM-DD
            d = date.fromisoformat(arg)
            start = datetime.combine(d, time(0, 0, tzinfo=TZ))
            end = datetime.combine(d, time(23, 59, 59, tzinfo=TZ))
            title = d.isoformat()
    except Exception:
        await m.answer("Неверный формат. Используй YYYY-MM или YYYY-MM-DD.")
        return

    async with SessionLocal() as s:
        q = (select(Booking)
             .where(Booking.start_at >= start, Booking.start_at <= end)
             .order_by(Booking.start_at))
        rows = (await s.execute(q)).scalars().all()

    if not rows:
        await m.answer("Нет данных за указанный период.")
        return

    # формируем CSV
    path = None
    try:
        fd, path = tempfile.mkstemp(prefix=f"bookings_{title}_", suffix=".csv")
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["id","user_id","start_at","end_at","sims","duration","price","status","client_name","client_phone","created_at"])
            for b in rows:
                writer.writerow([
                    b.id, b.user_id,
                    b.start_at.astimezone(TZ).isoformat(),
                    b.end_at.astimezone(TZ).isoformat(),
                    b.sims, b.duration, b.price, b.status,
                    (b.client_name or ""), (b.client_phone or ""),
                    (b.created_at.astimezone(TZ).isoformat() if b.created_at else "")
                ])
        await m.answer_document(FSInputFile(path), caption=f"Выгрузка {title}")
    finally:
        if path and os.path.exists(path):
            os.remove(path)

@dp.message(Command("report"))
async def report_cmd(m: Message):
    # доступ только админам
    if m.from_user.id not in ADMINS:
        await m.answer("Команда доступна только администратору.")
        return

    parts = m.text.split()
    if len(parts) == 1:
        # если дату не передали, берём сегодня по локальному TZ
        target_date = datetime.now(TZ).date()
    else:
        try:
            target_date = date.fromisoformat(parts[1])
        except Exception:
            await m.answer("Использование: /report YYYY-MM-DD (или без даты — за сегодня)")
            return

    # границы дня локальные
    day_start = datetime.combine(target_date, time(0, 0, tzinfo=TZ))
    day_end   = datetime.combine(target_date, time(23, 59, 59, tzinfo=TZ))

    async with SessionLocal() as s:
        q = (
            select(Booking)
            .where(
                Booking.start_at >= day_start,
                Booking.start_at <= day_end,
            )
            .order_by(Booking.start_at)
        )
        bookings = (await s.execute(q)).scalars().all()

    if not bookings:
        await m.answer(
            f"📊 Отчёт за {target_date.strftime('%d.%m.%Y')}\n"
            f"Брони не найдены."
        )
        return

    now_local = datetime.now(TZ)

    # группируем
    done_list = []
    cancelled_list = []
    noshow_list = []
    pending_list = []
    confirmed_future_list = []

    revenue_sum = 0

    for b in bookings:
        st = b.status

        if st == "done":
            done_list.append(b)
            revenue_sum += b.price

        elif st == "no_show":
            noshow_list.append(b)

        elif st == "cancelled":
            cancelled_list.append(b)

        elif st == "pending":
            pending_list.append(b)

        elif st == "confirmed":
            # смотрим — прошло или ещё впереди
            if b.end_at < now_local:
                # подтверждён был, время прошло,
                # но админ ещё не отметил ни done ни no_show
                # это СЫРЫЕ кандидаты на no_show
                noshow_list.append(b)
            else:
                confirmed_future_list.append(b)

    # строим текстовый отчёт

    # 1. хедер и метрики
    head_lines = [
        f"📊 Отчёт за {target_date.strftime('%d.%m.%Y')}",
        "",
        f"🏁 Пришли (done): {len(done_list)} шт.",
        f"💰 Выручка (по done): {revenue_sum} ₽",
        "",
        f"🚫 Не пришли / кандидаты: {len(noshow_list)}",
        f"❌ Отменены заранее (cancelled): {len(cancelled_list)}",
        f"⏳ Висело в ожидании подтверждения (pending): {len(pending_list)}",
        f"📌 Подтверждено и ещё впереди (confirmed, будущее): {len(confirmed_future_list)}",
        "",
        "Детали ниже 👇",
        "",
    ]

    # вспомогательная функция для форматирования строки брони
    def fmt_booking(b: Booking) -> str:
        return (
            f"#{b.id} {human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')} | "
            f"{b.sims} {sims_word(b.sims)} | {b.duration} мин | {b.price} ₽ | "
            f"{(b.client_name or '—')}, {(b.client_phone or '—')}"
        )

    # 2. блоки по категориям

    block_lines = []

    if done_list:
        block_lines.append("🏁 Завершили (done):")
        for b in done_list:
            block_lines.append("• " + fmt_booking(b))
        block_lines.append("")

    if noshow_list:
        block_lines.append("🚫 Не пришли (no_show) И/ИЛИ кандидаты (было confirmed, но время прошло):")
        for b in noshow_list:
            block_lines.append("• " + fmt_booking(b))
        block_lines.append("")

    if cancelled_list:
        block_lines.append("❌ Отменено (cancelled):")
        for b in cancelled_list:
            block_lines.append("• " + fmt_booking(b))
        block_lines.append("")

    if pending_list:
        block_lines.append("⏳ Висело в ожидании (pending):")
        for b in pending_list:
            block_lines.append("• " + fmt_booking(b))
        block_lines.append("")

    if confirmed_future_list:
        block_lines.append("📌 Подтверждено и ещё впереди/в процессе (confirmed, будущее относительно сейчас):")
        for b in confirmed_future_list:
            block_lines.append("• " + fmt_booking(b))
        block_lines.append("")

    text_report = "\n".join(head_lines + block_lines)

    # Telegram может ругаться на слишком длинные сообщения >4к символов,
    # но наш отчёт в обычный день туда влезет. Если прямо будет адово много,
    # можно потом нарезать. Пока отправляем одним куском.
    await m.answer(text_report)


@dp.callback_query(F.data.startswith("editcal:page:"))
async def edit_cal_page(c: CallbackQuery):
    # editcal:page:{bid}:{YYYY-MM}:{duration}:{sims}
    _, _, bid_str, ym, duration_str, sims_str = c.data.split(":")
    bid = int(bid_str)
    y, m = map(int, ym.split("-"))
    duration = int(duration_str)
    sims = int(sims_str)

    if duration not in PRICES or not (1 <= sims <= MAX_SIMS):
        await c.answer("Некорректные параметры", show_alert=True)
        return

    kb = build_month_kb_edit(y, m, bid, duration, sims)
    await safe_edit_reply_markup(c.message, reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("edit:day:"))
async def edit_pick_time_from_relative(c: CallbackQuery):
    # edit:day:{bid}:{day_offset}:{duration}:{sims}
    _, _, bid_str, day_offset_str, duration_str, sims_str = c.data.split(":")
    bid = int(bid_str)
    day_offset = int(day_offset_str)
    duration = int(duration_str)
    sims = int(sims_str)

    if duration not in PRICES or not (1 <= sims <= MAX_SIMS):
        await c.answer("Некорректные параметры", show_alert=True)
        return
    if day_offset not in (0, 1, 2):
        await c.answer("Слишком далеко", show_alert=True)
        return

    base = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=day_offset)

    await _edit_show_times(c, bid, base.date(), duration, sims)

@dp.callback_query(F.data.startswith("edit:date:"))
async def edit_pick_time_from_calendar(c: CallbackQuery):
    # edit:date:{bid}:{YYYY-MM-DD}:{duration}:{sims}
    _, _, bid_str, iso, duration_str, sims_str = c.data.split(":")
    bid = int(bid_str)
    duration = int(duration_str)
    sims = int(sims_str)

    if duration not in PRICES or not (1 <= sims <= MAX_SIMS):
        await c.answer("Некорректные параметры", show_alert=True)
        return

    y, m, d = map(int, iso.split("-"))
    picked_date = date(y, m, d)

    await _edit_show_times(c, bid, picked_date, duration, sims)

async def waitlist_worker():
    while True:
        try:
            now_local = datetime.now(TZ)
            async with SessionLocal() as s:
                q = (
                    select(Waitlist)
                    .where(Waitlist.active == True, Waitlist.start_at > now_local)
                )
                items = (await s.execute(q)).scalars().all()

            for w in items:
                free = await free_sims_for_interval(w.start_at, w.end_at)
                if free >= w.sims_needed:
                    # Условие выполнено — уведомляем и деактивируем подписку
                    try:
                        kb = InlineKeyboardMarkup(
                            inline_keyboard=[[
                                InlineKeyboardButton(
                                    text="📅 Забронировать",
                                    callback_data=f"book:time:{int(w.start_at.timestamp())}:{w.duration}:X"
                                )
                            ]]
                        )
                        await bot.send_message(
                            w.user_id,
                            (
                                "✅ Появилось окно!\n"
                                f"{human(w.start_at)}–{w.end_at.astimezone(TZ).strftime('%H:%M')} | "
                                f"{w.sims_needed} {sims_word(w.sims_needed)} | {w.duration} мин\n"
                                "Жми, чтобы забронировать:"
                            ),
                            reply_markup=kb
                        )
                    except Exception:
                        pass

                    async with SessionLocal() as s:
                        w_db = await s.get(Waitlist, w.id)
                        if w_db:
                            w_db.active = 0
                            await s.commit()
        except Exception:
            # не валим воркер из-за единичной ошибки
            pass

        await asyncio.sleep(60)

async def _edit_show_times(c: CallbackQuery, bid: int, target_date: date, duration: int, sims: int):
    base_dt = datetime.combine(target_date, time(0,0,tzinfo=TZ))

    slots = gen_slots(base_dt)
    now_local = datetime.now(TZ)
    close_dt = datetime.combine(target_date, CLOSE_T)

    slots = [
        s for s in slots
        if (target_date != today_local() or s > now_local + timedelta(minutes=10))
        and (s + timedelta(minutes=duration) <= (close_dt - SAFETY_GAP)
)
    ]

    rows = []
    for s in slots:
        end = s + timedelta(minutes=duration)
        free = await free_sims_for_interval(s, end)
        label = f"{s.strftime('%H:%M')} ({free} {sims_word(free)})"
        if free >= sims:
            rows.append([
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"edit:time:{bid}:{int(s.timestamp())}:{duration}:{sims}"
                )
            ])
        else:
            rows.append([InlineKeyboardButton(text=label, callback_data="noop")])

    if not rows:
        rows.append([InlineKeyboardButton(text="Нет доступных слотов", callback_data="noop")])

    await safe_edit_text(
        c.message,
        f"Выбери новое время на <b>{target_date.strftime('%d.%m')}</b>\n"
        f"Длительность: {duration} мин | Станций: {sims}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

@dp.callback_query(F.data.startswith("edit:time:"))
async def edit_apply(c: CallbackQuery):
    # edit:time:{bid}:{ts_start}:{duration}:{sims}
    _, _, bid_str, ts_str, duration_str, sims_str = c.data.split(":")
    bid = int(bid_str)
    start_ts = int(ts_str)
    duration = int(duration_str)
    sims = int(sims_str)

    start = datetime.fromtimestamp(start_ts, tz=TZ)
    end = start + timedelta(minutes=duration)

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b:
            await c.answer("Заявка не найдена", show_alert=True)
            return

        if b.user_id != c.from_user.id:
            await c.answer("Это не твоя заявка", show_alert=True)
            return

        if b.status != "pending":
            await c.answer("Заявку уже нельзя изменить", show_alert=True)
            return

        # защита: нельзя редачить если время уже наступает
        if datetime.now(TZ) >= b.start_at.astimezone(TZ):
            await c.answer("Уже поздно менять эту бронь", show_alert=True)
            return

        free = await free_sims_for_interval(start, end, exclude_id=b.id)
        if free < sims:
            await c.answer("Это время только что заняли 😢 попробуй другое.", show_alert=True)
            return

        b.start_at = start
        b.end_at = end
        b.expires_at = datetime.now(TZ) + timedelta(minutes=HOLD_MINUTES)
        await s.commit()
        await s.refresh(b)

        b_status = b.status
        b_price = b.price
        client_name = b.client_name or "-"
        client_phone = b.client_phone or "-"

    await safe_edit_text(
        c.message,
        (
            f"✅ Заявка #{bid} обновлена.\n\n"
            f"Новый слот: <b>{human(start)}–{end.strftime('%H:%M')}</b>\n"
            f"{sims} {sims_word(sims)} | {duration} мин\n"
            f"Имя: {client_name}\n"
            f"Тел: {client_phone}\n"
            f"Статус: {b_status}\n"
            f"Ожидает подтверждения администратора."
        )
    )

    kb_admin = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить",
                    callback_data=f"admin:approve:{bid}"
                ),
                InlineKeyboardButton(
                    text="❌ Отклонить",
                    callback_data=f"admin:reject:{bid}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📞 Контакт",
                    callback_data=f"admin:contact:{bid}"
                ),
                InlineKeyboardButton(
                    text="✏ Запросить контакт",
                    callback_data=f"admin:askcontact:{bid}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Не пришёл",
                    callback_data=f"admin:noshow:{bid}"
                ),
                InlineKeyboardButton(
                    text="🏁 Пришёл",
                    callback_data=f"admin:done:{bid}"
                ),
            ],
        ]
    )

    uname = c.from_user.username or c.from_user.full_name
    txt_admin = (
        f"♻️ Обновлена заявка #{bid} от @{uname}\n"
        f"{human(start)}–{end.strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {duration} мин | {b_price} ₽\n"
        f"Имя: {client_name}\n"
        f"Тел: {client_phone}\n"
        f"Статус: {b_status}"
    )

    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, txt_admin, reply_markup=kb_admin)
        except Exception:
            pass

    await c.answer("Время обновлено")

# -------- Admin buttons --------
def is_admin(uid: int) -> bool:
    return uid in ADMINS

async def get_booking(session: AsyncSession, bid: int) -> Optional[Booking]:
    return await session.get(Booking, bid)

@dp.callback_query(F.data.startswith("admin:approve:"))
async def admin_approve(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True); return
    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        async with s.begin():
            b = (await s.execute(
                select(Booking).where(Booking.id == bid).with_for_update()
            )).scalar_one_or_none()
            if not b:
                await c.answer("Бронь не найдена", show_alert=True); return

            now = datetime.now(TZ)
            expired = (b.expires_at and b.expires_at < now)

            if expired:
                b.status = "cancelled"

            elif b.status == "pending":
                # Лочим пересекающиеся, считаем занятость
                await s.execute(
                    text("""SELECT id FROM bookings
                            WHERE status IN ('pending','confirmed','block')
                              AND start_at < :end AND end_at > :start
                            FOR UPDATE"""),
                    {"start": b.start_at, "end": b.end_at}
                )
                taken = (await s.execute(
                    select(func.coalesce(func.sum(Booking.sims), 0)).where(
                        Booking.status.in_(("pending","confirmed","block")),
                        Booking.start_at < b.end_at,
                        Booking.end_at > b.start_at,
                        Booking.id != b.id
                    )
                )).scalar_one()
                free = MAX_SIMS - int(taken)

                if free >= b.sims:
                    b.status = "confirmed"
                    b.expires_at = None
                else:
                    b.status = "cancelled"

            else:
                # Уже не pending — оставляем как есть (idempotency)
                pass

        # читаем поля ПОСЛЕ транзакции
        status = b.status
        user_id = b.user_id
        start_at, end_at = b.start_at, b.end_at
        sims, dur, price = b.sims, b.duration, b.price
        client_name = b.client_name or "-"
        client_phone = b.client_phone or "-"

    # Ответы и тексты
    if status == "confirmed":
        await safe_edit_text(c.message, f"✅ Подтверждена заявка #{bid}")
        await safe_edit_reply_markup(c.message, reply_markup=None)
        try:
            await bot.send_message(
                user_id,
                (
                    f"✅ Ваша бронь #{bid} подтверждена!\n"
                    f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
                    f"{sims} {sims_word(sims)} | {dur} мин\n"
                    f"Оплата на месте: <b>{price} ₽</b>\n"
                    f"Контакт у нас есть: {client_name}, {client_phone}\n"
                    f"📍 Адрес: {ADDRESS_FULL} ({ADDRESS_AREA})"
                ),
                reply_markup=confirm_user_kb(bid)   # ← вот это
            )
        except Exception:
            pass

    elif status == "pending":
        # Сюда мы уже не попадём, но оставим на будущее
        await safe_edit_text(c.message, f"⏳ Заявка #{bid} всё ещё в ожидании")
    elif status == "cancelled":
        await safe_edit_text(c.message, f"❌ Не удалось подтвердить заявку #{bid} (слот недоступен/просрочена)")
        try:
            await bot.send_message(
                user_id,
                (f"⚠️ Бронь #{bid} не удалось подтвердить — окно занято или заявка просрочена.\n"
                 f"Попробуйте выбрать другое время: /start")
            )
        except Exception:
            pass
    else:
        # Уже была confirmed/cancelled/done/no_show/block — ничего не меняли
        await safe_edit_text(c.message, f"ℹ️ Заявка #{bid} уже в статусе: {human_status(status)}")
        await safe_edit_reply_markup(c.message, reply_markup=None)
    await c.answer()

@dp.callback_query(F.data.startswith("admin:contact:"))
async def admin_contact_info(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b:
            await c.answer("Заявка не найдена", show_alert=True)
            return

    client_name = b.client_name or "—"
    client_phone = b.client_phone or "—"

    await c.answer()  # чтобы убрать "loading..." в интерфейсе
    await bot.send_message(
        c.from_user.id,
        (
            f"📞 Контакт по заявке #{bid}:\n"
            f"Имя: {client_name}\n"
            f"Телефон: {client_phone}"
        )
    )

@dp.callback_query(F.data.startswith("admin:askcontact:"))
async def admin_ask_contact(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b:
            await c.answer("Заявка не найдена", show_alert=True)
            return

        user_id = b.user_id
        start_local = human(b.start_at)
        sims_txt = f"{b.sims} {sims_word(b.sims)}"
        dur_txt = f"{b.duration} мин"

    # <--- вот тут запоминаем
    PENDING_CONTACTS[user_id] = bid

    try:
        await bot.send_message(
            user_id,
            (
                f"👋 По вашей брони #{bid} ({start_local}, {sims_txt}, {dur_txt}) "
                f"нужно уточнить контакт.\n\n"
                f"Отправьте одним сообщением:\n"
                f"Имя, телефон\n"
                f"Например:\n"
                f"Влад, +7 953 254-xx-xx\n\n"
                f"ИЛИ отправьте так:\n"
                f"/contact {bid} Влад, +7 953 254-xx-xx"
            )
        )
        await c.answer("Запрос отправлен клиенту ✅")
    except Exception:
        await c.answer("Не удалось написать клиенту 😕", show_alert=True)

@dp.callback_query(F.data.startswith("admin:done:"))
async def admin_mark_done(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b:
            await c.answer("Заявка не найдена", show_alert=True)
            return

        # логика безопасности:
        # отмечать done можно только если бронь уже была подтверждена
        # и время уже закончилось или прямо сейчас идёт
        now_local = datetime.now(TZ)
        if b.status not in ("confirmed", "done"):
            await c.answer("Эта заявка не была подтверждена, странно закрывать её как 'пришёл'", show_alert=True)
            return

        if now_local < b.start_at.astimezone(TZ) - timedelta(minutes=10):
            await c.answer("Слишком рано отмечать визит как завершённый 🙃", show_alert=True)
            return

        # фиксируем финальный статус
        b.status = "done"
        b.expires_at = None
        await s.commit()

        # пишем клиенту (если хотим — можно не писать, но это приятно)
        try:
            await bot.send_message(
                b.user_id,
                (
                    f"🏁 Ваша бронь #{bid} отмечена как завершённая.\n"
                    f"Спасибо, что были у нас 🙌"
                )
            )
        except Exception:
            pass

        await c.answer("Пометил как пришёл ✅", show_alert=False)

    # и обновим текст под админским сообщением (где были кнопки)
    await safe_edit_text(c.message, f"🏁 Заявка #{bid}: отмечено как пришёл (done)")

@dp.callback_query(F.data.startswith("admin:noshow:"))
async def admin_mark_noshow(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b:
            await c.answer("Заявка не найдена", show_alert=True)
            return

        # логику делаем аккуратно:
        # мы считаем no_show валидным только для заявок, которые были подтверждены (confirmed),
        # их время уже закончилось, и они ещё не помечены ни как done, ни как no_show
        now_local = datetime.now(TZ)
        if b.status not in ("confirmed", "no_show"):
            await c.answer("Можно отметить 'не пришёл' только для подтверждённых заявок.", show_alert=True)
            return

        if now_local < b.end_at.astimezone(TZ):
            await c.answer("Слот ещё не закончился, рано ставить 'не пришёл'.", show_alert=True)
            return

        b.status = "no_show"
        b.expires_at = None
        await s.commit()

        # клиенту в лоб не пишем «вы не пришли», это токсично :)
        # просто молча фиксируем

        await c.answer("Пометил как не пришёл 🚫", show_alert=False)

    await safe_edit_text(c.message, f"🚫 Заявка #{bid}: отмечено как не пришёл (no_show)")

@dp.callback_query(F.data.startswith("admin:reject:"))
async def admin_reject(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        b = await get_booking(s, bid)
        if not b:
            await c.answer("Бронь не найдена", show_alert=True)
            return

        b.status = "cancelled"
        await s.commit()

        user_id = b.user_id

    await safe_edit_text(c.message, f"❌ Отклонена заявка #{bid}")
    try:
        await bot.send_message(
            user_id,
            f"❌ Ваша заявка #{bid} отклонена. Свяжитесь с администратором."
        )
    except Exception:
        pass

    await c.answer()

@dp.callback_query(F.data == "noop")
async def noop_cb(c: CallbackQuery):
    await c.answer("Недоступно. Выберите другое время/дату")

# -------- User shortcuts --------
@dp.callback_query(F.data == "my:list")
async def my_list_cb(c: CallbackQuery):
    now_local = datetime.now(TZ)

    async with SessionLocal() as s:
        q = (
            select(Booking)
            .where(
                Booking.user_id == c.from_user.id,
                Booking.status.in_(("pending", "confirmed")),
                Booking.end_at > now_local,
            )
            .order_by(Booking.start_at)
        )
        rows = (await s.execute(q)).scalars().all()

    if not rows:
        await c.message.answer("У вас нет активных заявок.")
        await c.answer()
        return

    lines = []
    for b in rows:
        lines.append(
            f"#{b.id} — {human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')}\n"
            f"{b.sims} {sims_word(b.sims)} | {b.duration} мин | {b.price} ₽\n"
            f"Статус: {human_status(b.status)}\n"
            f"Контакт: {(b.client_name or '—')}, {(b.client_phone or '—')}\n"
            f"/edit {b.id} изменить время • /cancel {b.id} отменить\n"
            f"/contact {b.id} Имя, Телефон — обновить контакт\n"
        )

    await c.message.answer("Ваши активные заявки:\n\n" + "\n".join(lines))
    await c.answer()

@dp.message(Command("my"))
async def my_cmd(m: Message):
    now_local = datetime.now(TZ)

    async with SessionLocal() as s:
        q = (
            select(Booking)
            .where(
                Booking.user_id == m.from_user.id,
                Booking.status.in_(("pending", "confirmed")),
                Booking.end_at > now_local,
            )
            .order_by(Booking.start_at)
        )
        rows = (await s.execute(q)).scalars().all()

    if not rows:
        await m.answer("У вас нет активных заявок.")
        return

    lines = []
    for b in rows:
        lines.append(
            f"#{b.id} — {human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')}\n"
            f"{b.sims} {sims_word(b.sims)} | {b.duration} мин | {b.price} ₽\n"
            f"Статус: {human_status(b.status)}\n"
            f"Контакт: {(b.client_name or '—')}, {(b.client_phone or '—')}\n"
            f"/edit {b.id} изменить время • /cancel {b.id} отменить\n"
            f"/contact {b.id} Имя, Телефон — обновить контакт\n"
        )

    await m.answer("Ваши активные заявки:\n\n" + "\n".join(lines))

@dp.message(Command("edit"))
async def edit_cmd(m: Message):
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /edit ID (например /edit 123)")
        return

    bid = int(parts[1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b or b.user_id != m.from_user.id:
            await m.answer("Заявка не найдена.")
            return

        if b.status != "pending":
            await m.answer("Эту заявку уже нельзя изменить (она не в ожидании).")
            return

        if datetime.now(TZ) >= b.start_at.astimezone(TZ):
            await m.answer("Эту заявку уже нельзя изменить, время скоро начинается или уже началось.")
            return

        duration = b.duration
        sims = b.sims

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Сегодня", callback_data=f"edit:day:{bid}:0:{duration}:{sims}")],
                [InlineKeyboardButton(text="Завтра", callback_data=f"edit:day:{bid}:1:{duration}:{sims}")],
                [InlineKeyboardButton(text="Послезавтра", callback_data=f"edit:day:{bid}:2:{duration}:{sims}")],
                [InlineKeyboardButton(text="📅 Другая дата", callback_data=f"editcal:open:{bid}:{duration}:{sims}")],
            ]
        )

        msg_text = (
            f"Редактируем заявку #{bid}.\n"
            f"Текущая бронь: {human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')} "
            f"| {b.sims} {sims_word(b.sims)} | {b.duration} мин.\n"
            f"Имя: {b.client_name or '-'}\n"
            f"Тел: {b.client_phone or '-'}\n\n"
            "Выбери новый день:"
        )

    await m.answer(msg_text, reply_markup=kb)

@dp.message(Command("book"))
async def book_cmd(m: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{d} мин ({PRICES[d]} ₽/сим)", callback_data=f"book:dur:{d}")]
            for d in (60, 90, 120, 30)
        ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home")]]
    )
    await m.answer("Выбери длительность:", reply_markup=kb)

@dp.message(Command("no_show"))
async def no_show_cmd(m: Message):
    if m.from_user.id not in ADMINS:
        await m.answer("Команда доступна только администратору.")
        return
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /no_show ID")
        return
    bid = int(parts[1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b:
            await m.answer("Заявка не найдена.")
            return
        now_local = datetime.now(TZ)
        if b.status != "confirmed" or now_local < b.end_at.astimezone(TZ):
            await m.answer("Отметить 'не пришёл' можно только для завершившейся подтверждённой заявки.")
            return
        b.status = "no_show"
        b.expires_at = None
        await s.commit()

    await m.answer(f"🚫 Заявка #{bid}: отмечено как не пришёл.")

@dp.message(Command("cancel"))
async def cancel_cmd(m: Message):
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /cancel ID_ЗАЯВКИ (например /cancel 123)")
        return

    bid = int(parts[1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != m.from_user.id:
            await m.answer("Заявка не найдена.")
            return

        if datetime.now(TZ) >= b.start_at.astimezone(TZ):
            await m.answer("Нельзя отменить — время уже наступило.")
            return

        if b.status == "cancelled":
            await m.answer(f"Заявка #{bid} уже отменена.")
            return

        b.status = "cancelled"
        b.expires_at = None
        await s.commit()

    await m.answer(f"❌ Заявка #{bid} отменена.")

@dp.message(Command("help"))
async def help_cmd(m: Message):
    text = (
        "🧭 <b>Доступные команды</b>\n\n"
        "👤 <b>Пользователи:</b>\n"
        "• /start — главное меню\n"
        "• /book — начать бронирование\n"
        "• /my — показать ваши активные заявки\n"
        "  или воспользуйтесь кнопкой «📄 Мои заявки» в меню\n"
        "• /contact — обновить контактные данные (Имя, Телефон)\n"
        "• /help — помощь по командам\n\n"
        "👮 <b>Администраторы:</b>\n"
        "• /day [YYYY-MM-DD] — расписание на день\n"
        "• /report [YYYY-MM-DD] — отчёт по дню\n"
        "• /csv YYYY-MM|YYYY-MM-DD — выгрузка CSV\n"
        "• /block — создать техперерыв\n"
        "• /unblock ID — удалить техперерыв\n"
        "• /no_show ID — отметить неявку клиента\n"
        "• /promo — управление промокодами\n\n"
        "💡 <b>Подсказки:</b>\n"
        "• Нажмите «🔔 Уведомить», если нужное время занято — бот пришлёт сообщение, когда появится окно.\n"
        "• Используйте «📄 Мои заявки» в меню, чтобы быстро просмотреть или отменить бронь.\n"
        "• Бот автоматически подтвердит или завершит брони по времени.\n"
    )
    await m.answer(text, parse_mode="HTML")

# -------- Reminder worker --------
async def complete_worker():
    """
    Переводит прошедшие подтверждённые брони в статус done.
    Логика:
    - статус == confirmed
    - end_at < сейчас
    -> статус = done
    """
    while True:
        try:
            now_local = datetime.now(TZ)

            async with SessionLocal() as s:
                # найдём все просроченные подтверждённые брони
                q = (
                    select(Booking)
                    .where(
                        Booking.status == "confirmed",
                        Booking.end_at < now_local,
                    )
                )
                finished = (await s.execute(q)).scalars().all()

                if finished:
                    for b in finished:
                        b.status = "done"
                        b.expires_at = None  # на всякий случай
                    await s.commit()
        except Exception:
            # не падаем из-за случайной ошибки
            pass

        await asyncio.sleep(60)

async def reminder_worker():
    while True:
        try:
            now_local = datetime.now(TZ)

            remind_from = now_local + REMIND_BEFORE
            remind_to = now_local + REMIND_BEFORE + timedelta(minutes=1)

            async with SessionLocal() as s:
                q = (
                    select(Booking)
                    .where(
                        Booking.status == "confirmed",
                        Booking.start_at >= remind_from,
                        Booking.start_at < remind_to,
                    )
                )
                rows = (await s.execute(q)).scalars().all()

            for b in rows:
                try:
                    await bot.send_message(
                        b.user_id,
                        f"⏰ Напоминание!\n"
                        f"Ваша бронь #{b.id} в {human(b.start_at)} "
                        f"({b.sims} {sims_word(b.sims)}, {b.duration} мин). Ждём вас!"
                    )
                except Exception:
                    pass

        except Exception:
            pass

        await asyncio.sleep(60)

async def autoconfirm_worker():
    while True:
        now_local = datetime.now(TZ)
        soon_to = now_local + AUTOCONFIRM_BEFORE

        async with SessionLocal() as s:
            q = (
                select(Booking)
                .where(
                    Booking.status == "pending",
                    Booking.start_at > now_local,
                    Booking.start_at <= soon_to,
                )
            )
            pendings = (await s.execute(q)).scalars().all()

        for b in pendings:
            async with SessionLocal() as s:
                b = await s.get(Booking, b.id)
                if not b:
                    continue

                if b.status != "pending":
                    continue

                if b.expires_at and b.expires_at < datetime.now(TZ):
                    continue

                free = await free_sims_for_interval(b.start_at, b.end_at, exclude_id=b.id)
                if free < b.sims:
                    continue

                b.status = "confirmed"
                b.expires_at = None
                await s.commit()
                await s.refresh(b)

                b_user_id = b.user_id
                b_id = b.id
                b_start = b.start_at
                b_end = b.end_at
                b_sims = b.sims
                b_dur = b.duration
                b_price = b.price
                b_name = b.client_name or "-"
                b_phone = b.client_phone or "-"

            # 👇 Клавиатура для пользователя при автоподтверждении
            kb_user = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="📅 В календарь (.ics)",
                            callback_data=f"ics:send:{b_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="📄 Мои заявки",
                            callback_data="my:list"
                        )
                    ]
                ]
            )

            try:
                await bot.send_message(
                    b_user_id,
                    (
                        f"✅ Ваша бронь #{b_id} подтверждена автоматически!\n"
                        f"{human(b_start)}–{b_end.astimezone(TZ).strftime('%H:%M')} | "
                        f"{b_sims} {sims_word(b_sims)} | {b_dur} мин\n"
                        f"Оплата на месте: <b>{b_price} ₽</b>\n"
                        f"Контакт у нас есть: {b_name}, {b_phone}\n\n"
                        f"📍 Адрес: {ADDRESS_FULL} ({ADDRESS_AREA})\n"
                        f"Ждём вас 👌"
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="🗺 Открыть карту", url=ADDRESS_MAP_URL)]]
    ),
)
            except Exception:
                pass
            

            note_for_admins = (
                f"🤖 Автоподтверждение заявки #{b_id}\n"
                f"{human(b_start)}–{b_end.astimezone(TZ).strftime('%H:%M')} | "
                f"{b_sims} {sims_word(b_sims)} | {b_dur} мин | {b_price} ₽\n"
                f"Имя: {b_name}\n"
                f"Тел: {b_phone}"
            )
            for admin_id in ADMINS:
                try:
                    await bot.send_message(admin_id, note_for_admins)
                except Exception:
                    pass

        await asyncio.sleep(60)

@dp.message(Command("contact"))
async def contact_cmd(m: Message, state: FSMContext):
    # варианты:
    # 1) /contact 123 Антон, +7 ...
    # 2) /contact 123   (тогда запускаем FSM "пришли Имя, телефон")

    parts = m.text.split(maxsplit=2)

    if len(parts) < 2 or not parts[1].isdigit():
        await m.answer(
            "Использование:\n"
            "/contact ID Имя, Телефон\n"
            "или просто /contact ID и я сам спрошу дальше.\n\n"
            "Пример:\n"
            "/contact 123 Антон, +7 912 000-00-00"
        )
        return

    bid = int(parts[1])

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != m.from_user.id:
            await m.answer("Заявка не найдена.")
            return

    # Если он прислал сразу имя+тел — обрабатываем мгновенно
    if len(parts) == 3:
        client_name, client_phone = split_contact(parts[2])

        async with SessionLocal() as s:
            b = await s.get(Booking, bid)
            if not b or b.user_id != m.from_user.id:
                await m.answer("Заявка не найдена.")
                return

            # контакт можно менять даже после подтверждения
            b.client_name = client_name
            b.client_phone = client_phone
            await s.commit()
            await s.refresh(b)

            start_local = human(b.start_at)
            end_local = b.end_at.astimezone(TZ).strftime("%H:%M")
            sims = b.sims
            dur = b.duration

        await m.answer(
            f"Контакт по заявке #{bid} обновлён ✅\n"
            f"{client_name}, {client_phone}\n"
            f"{start_local}–{end_local} | {sims} {sims_word(sims)} | {dur} мин"
        )

        # уведомим админов
        note = (
            f"✏ Обновлён контакт по заявке #{bid}\n"
            f"{start_local}–{end_local} | {sims} {sims_word(sims)} | {dur} мин\n"
            f"Имя: {client_name}\n"
            f"Тел: {client_phone}"
        )
        for admin_id in ADMINS:
            try:
                await bot.send_message(admin_id, note)
            except Exception:
                pass

        return

    # иначе (он не прислал контакт сейчас) -> запускаем FSM второй стадией
    await state.update_data(bid=bid)
    await state.set_state(UpdateContactForm.waiting_new_contact)

    await m.answer(
        f"Ок, заявка #{bid}.\n"
        "Пришли новые данные в формате:\n"
        "Имя, телефон\n\n"
        "Например:\n"
        "Игорь, +7 999 123-45-67"
    )

@dp.callback_query(F.data.startswith("dayfree:"))
async def day_free_slots(c: CallbackQuery):
    # dayfree:YYYY-MM-DD:need_sims
    _, iso_date, need_sims_str = c.data.split(":")
    need_sims = int(need_sims_str)

    y, m, d = map(int, iso_date.split("-"))
    target = date(y, m, d)

    # границы дня
    close_dt = datetime.combine(target, CLOSE_T)
    safe_close = close_dt - SAFETY_GAP

    # для каждого duration собираем окна
    report_lines = [f"🔍 Доступные окна {target.strftime('%d.%m.%Y')} для {need_sims} {sims_word(need_sims)}"]

    for dur in (30, 60, 90, 120):
        win = timedelta(minutes=dur)
        t = datetime.combine(target, OPEN_T)

        slots_ok = []
        while t + win <= safe_close:
            # сколько реально свободно в этом интервале
            free = await free_sims_for_interval(t, t + win)
            if free >= need_sims:
                slots_ok.append(f"{t.strftime('%H:%M')} ({free} свободно)")
            t += timedelta(minutes=30)

        if slots_ok:
            report_lines.append(f"\n⏱ {dur} мин:\n" + ", ".join(slots_ok))
        else:
            report_lines.append(f"\n⏱ {dur} мин:\nнет слотов")

    await c.message.answer("\n".join(report_lines))
    await c.answer()

@dp.callback_query(F.data.startswith("ics:send:"))
async def ics_send_cb(c: CallbackQuery):
    bid = int(c.data.split(":")[-1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != c.from_user.id:
            await c.answer("Заявка не найдена", show_alert=True); return
        if b.status not in ("confirmed", "done"):
            await c.answer("ICS доступен после подтверждения.", show_alert=True); return
    await _send_ics(bot, c.from_user.id, b)
    await c.answer("Файл календаря отправлен ✅")

# -------- Operator day view --------
@dp.message(Command("day"))
async def day_cmd(m: Message):
    if m.from_user.id not in ADMINS:
        await m.answer("Команда доступна только администратору.")
        return

    parts = m.text.split()
    if len(parts) == 1:
        target = datetime.now(TZ).date()
    else:
        try:
            target = date.fromisoformat(parts[1])
        except Exception:
            await m.answer("Использование: /day YYYY-MM-DD (или без даты — за сегодня)")
            return

    day_start = datetime.combine(target, time(0,0,tzinfo=TZ))
    day_end   = datetime.combine(target, time(23,59,59,tzinfo=TZ))

    async with SessionLocal() as s:
        # подчистим протухшие pending
        await s.execute(
    text("""UPDATE bookings
            SET status='cancelled'
            WHERE status='pending'
              AND expires_at IS NOT NULL
              AND expires_at < :now"""),
    {"now": datetime.now(TZ)}
)
        await s.commit()

        q = (
            select(Booking)
            .where(Booking.start_at >= day_start, Booking.start_at <= day_end)
            .order_by(Booking.start_at)
        )
        rows = (await s.execute(q)).scalars().all()

    # компактный список броней
    if rows:
        booked_lines = "\n".join(short_booking_line(b) for b in rows)
    else:
        booked_lines = "Брони отсутствуют."

    # кнопки выбора "ищем свободно для N симов"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔍 1 сим", callback_data=f"dayfree:{target.isoformat()}:1"),
                InlineKeyboardButton(text="🔍 2 сима", callback_data=f"dayfree:{target.isoformat()}:2"),
            ],
            [
                InlineKeyboardButton(text="🔍 3 сима", callback_data=f"dayfree:{target.isoformat()}:3"),
                InlineKeyboardButton(text="🔍 4 сима", callback_data=f"dayfree:{target.isoformat()}:4"),
            ],
        ]
    )

    # отправляем первое сообщение — общее инфо и кнопки
    await m.answer(
        f"📅 {target.strftime('%d.%m.%Y')} (13:00–23:00)\n"
        f"Всего станций: {MAX_SIMS}\n\n"
        f"Брони:\n{booked_lines}\n\n"
        f"⤵️ Показать свободные окна для сколько станций?",
        reply_markup=kb
    )

    # строим и отправляем расписание дня по 30 минут
    timetable_text = build_day_timetable(rows, target)
    # делаем второе сообщение без клавиатуры
    await m.answer(timetable_text)

# ===== PROMOCODES (in-memory) =====
# Примеры с лимитами и минималкой
PROMO_RULES = {
    # одноразовый -10% для каждого пользователя, общий лимит 500 применений
    "WELCOME10": {
        "kind": "percent", "value": 10,
        "until": date(2099, 1, 1),
        "one_time": True,
        "per_user_limit": 1,   # раз на пользователя
        "total_limit": 500,    # общий лимит
        "min_total": 0,        # минимальная сумма заказа
    },
    # фиксированная скидка 100 ₽, минимум чек 600 ₽
    "FIX100": {
        "kind": "fixed", "value": 100,
        "until": date(2099, 1, 1),
        "one_time": True,
        "per_user_limit": 3,
        "total_limit": 1000,
        "min_total": 600,
    },
}

# user_id -> {"code": str, "rule": dict}
PROMOS_PENDING: dict[int, dict] = {}

# учёт применений
PROMO_USAGE_TOTAL: dict[str, int] = {}                # code -> total uses
PROMO_USAGE_PER_USER: dict[str, dict[int, int]] = {}  # code -> {user_id: n}

def _promo_can_use(code: str, rule: dict, user_id: int, base_price: int) -> tuple[bool, str | None]:
    # срок действия
    if rule.get("until") and today_local() > rule["until"]:
        return False, "Срок действия промокода истёк."
    # перс-реферальный запрет для владельца (если такой ключ появится)
    owner_id = rule.get("owner_id")
    if owner_id is not None and owner_id == user_id:
        return False, "Нельзя использовать свой реферальный код."
    # минимальная сумма
    if base_price < int(rule.get("min_total", 0)):
        return False, f"Минимальная сумма для этого промокода: {rule['min_total']} ₽."
    # общий лимит
    total_used = PROMO_USAGE_TOTAL.get(code, 0)
    total_limit = rule.get("total_limit")
    if total_limit is not None and total_used >= total_limit:
        return False, "Лимит промокода исчерпан."
    # лимит на пользователя
    per_user_limit = int(rule.get("per_user_limit", 0)) or None
    if per_user_limit:
        used_by_user = PROMO_USAGE_PER_USER.get(code, {}).get(user_id, 0)
        if used_by_user >= per_user_limit:
            return False, "Лимит использования на пользователя исчерпан."
    return True, None

def apply_promo(base_price: int, user_id: int) -> tuple[int, Optional[str]]:
    promo = PROMOS_PENDING.get(user_id)
    if not promo:
        return base_price, None

    code = promo["code"]
    rule = promo["rule"]
    ok, reason = _promo_can_use(code, rule, user_id, base_price)
    if not ok:
        # промо не подходит — убираем, но цену не трогаем
        PROMOS_PENDING.pop(user_id, None)
        return base_price, None

    if rule["kind"] == "percent":
        new_price = int(round(base_price * (100 - int(rule["value"])) / 100))
    elif rule["kind"] == "fixed":
        new_price = max(0, base_price - int(rule["value"]))
    else:
        new_price = base_price

    return new_price, code

def _promo_mark_used(code: str, user_id: int, rule: dict):
    # учёт статистики
    PROMO_USAGE_TOTAL[code] = PROMO_USAGE_TOTAL.get(code, 0) + 1
    per_user = PROMO_USAGE_PER_USER.setdefault(code, {})
    per_user[user_id] = per_user.get(user_id, 0) + 1

    # одноразовый флаг — удаляем pending
    if rule.get("one_time"):
        PROMOS_PENDING.pop(user_id, None)

# === ICS helpers ===
def _ics_text_for_booking(b: Booking) -> str:
    import uuid
    uid = uuid.uuid4().hex
    return (
        "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//simclub//ru//\nBEGIN:VEVENT\n"
        f"UID:{uid}\nDTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTSTART:{b.start_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTEND:{b.end_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}\n"
        f"SUMMARY:Симрейсинг — {b.sims} {sims_word(b.sims)}\n"
        f"LOCATION:{ADDRESS_FULL}\n"
        f"DESCRIPTION:{b.sims} {sims_word(b.sims)}, {b.duration} мин\nEND:VEVENT\nEND:VCALENDAR\n"
    )

async def _send_ics(bot: Bot, chat_id: int, b: Booking):
    import tempfile, os
    ics = _ics_text_for_booking(b)
    fd, path = tempfile.mkstemp(prefix=f"booking_{b.id}_", suffix=".ics")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(ics)
        await bot.send_document(chat_id, FSInputFile(path), caption=f"Календарь для брони #{b.id}")
    finally:
        if os.path.exists(path):
            os.remove(path)

@dp.message(Command("promo"))
async def promo_cmd(m: Message):
    parts = m.text.strip().split(maxsplit=1)
    if len(parts) != 2:
        await m.answer("Использование: /promo КОД\nНапример: /promo WELCOME10")
        return

    code = parts[1].strip().upper()
    rule = PROMO_RULES.get(code)
    if not rule:
        await m.answer("Промокод не найден 😕")
        return

    # проверим базово без суммы (минималка проверится при цене брони),
    # но если у кода есть owner_id = сам пользователь — откажем сразу
    if rule.get("owner_id") == m.from_user.id:
        await m.answer("Нельзя использовать свой реферальный код.")
        return

    PROMOS_PENDING[m.from_user.id] = {"code": code, "rule": rule}
    kind = "скидка %" if rule["kind"] == "percent" else "скидка ₽"
    lim_user = rule.get("per_user_limit")
    lim_total = rule.get("total_limit")
    min_total = rule.get("min_total", 0)
    lines = [f"Ок! Применю промокод <b>{code}</b> ({kind}: {rule['value']}) к следующей брони."]
    if min_total:
        lines.append(f"Минимальный чек: {min_total} ₽.")
    if lim_user:
        lines.append(f"Лимит на пользователя: {lim_user}.")
    if lim_total:
        used = PROMO_USAGE_TOTAL.get(code, 0)
        lines.append(f"Осталось по коду: {max(lim_total - used, 0)} применений.")
    await m.answer("\n".join(lines))

@dp.startup()
async def on_startup(bot: Bot):
    # (опц.) сброс вебхука
    try:
        info = await bot.get_webhook_info()
        if info.url:
            print(f"Webhook was set to: {info.url} — removing...")
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print(f"delete_webhook failed: {e}")

    # команды и таблицы
    await ensure_tables()
    await setup_commands()

    # фоновые воркеры — тут, а не в main()
    BG_TASKS[:] = [
        asyncio.create_task(reminder_worker(), name="reminder_worker"),
        asyncio.create_task(autoconfirm_worker(), name="autoconfirm_worker"),
        asyncio.create_task(complete_worker(), name="complete_worker"),
        asyncio.create_task(waitlist_worker(), name="waitlist_worker"),
        asyncio.create_task(cleanup_pending_worker(), name="cleanup_pending_worker"),
    ]


@dp.shutdown()
async def on_shutdown(bot: Bot):
    # аккуратно гасим все фоновые таски
    for t in BG_TASKS:
        t.cancel()
    for t in BG_TASKS:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    # aiogram сам закроет bot.session внутри shutdown

@dp.message()
async def catch_free_contact(m: Message):
    if m.from_user.id not in PENDING_CONTACTS:
        return

    if not looks_like_contact(m.text):
        # не похоже на контакт → не считаем это ответом на запрос
        return

    bid = PENDING_CONTACTS.pop(m.from_user.id)

    client_name, client_phone = split_contact(m.text)

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        # защита от дурака: заявка пропала / не его
        if not b or b.user_id != m.from_user.id:
            await m.answer("Не получилось обновить контакт по заявке. Если что, можно написать администратору напрямую 🙌")
            return

        # пишем новые данные
        b.client_name = client_name
        b.client_phone = client_phone
        await s.commit()
        await s.refresh(b)

        start_at = b.start_at
        end_at = b.end_at
        sims = b.sims
        dur = b.duration
        price = b.price

    # ответ клиенту
    await m.answer(
        "Контакт обновлён ✅\n\n"
        f"Заявка #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Теперь указано:\n"
        f"{client_name}, {client_phone}\n\n"
        "Спасибо! Администратор получил новые данные 👌"
    )

    # пуш админам
    admin_text = (
        f"✏️ Обновлён контакт в заявке #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Новый контакт: {client_name}, {client_phone}"
    )

    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, admin_text)
        except Exception:
            pass

    # дублируем логику update_contact_finish: парсим текст, пишем в БД,
    # отвечаем юзеру, шлём админам.

async def cleanup_pending_worker():
    while True:
        try:
            async with SessionLocal() as s:
                await s.execute(
    text("""UPDATE bookings
            SET status='cancelled'
            WHERE status='pending'
              AND expires_at IS NOT NULL
              AND expires_at < :now"""),
    {"now": datetime.now(TZ)}
)
                await s.commit()
        except Exception:
            pass
        await asyncio.sleep(60)

# ====================== RUN =========================

async def main():
    print("Bot started ✅")

    # Проверка токена
    try:
        me = await bot.get_me()
        print(f"Authorized as @{me.username} id={me.id}")
    except Exception as e:
        print(f"BOT_TOKEN problem? get_me failed: {e}")
        return

    # Сброс webhook перед polling
    try:
        info = await bot.get_webhook_info()
        if info.url:
            print(f"Webhook was set to: {info.url} — removing...")
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print(f"delete_webhook failed: {e}")

    # Просто ждём polling; startup/shutdown сами поднимут/погасят BG_TASKS
    await dp.start_polling(bot, polling_timeout=60)

if __name__ == "__main__":
    asyncio.run(main())