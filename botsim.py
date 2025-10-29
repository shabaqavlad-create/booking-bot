import os, asyncio, calendar

from typing import Optional
from datetime import datetime, timedelta, time, timezone, date

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
TZ = timezone(timedelta(hours=5))
OPEN_H, OPEN_M = 13, 0
CLOSE_H, CLOSE_M = 23, 0
OPEN_T = time(OPEN_H, OPEN_M, tzinfo=TZ)
CLOSE_T = time(CLOSE_H, CLOSE_M, tzinfo=TZ)

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

class Booking(Base):
    __tablename__ = "bookings"
    __table_args__ = (
        Index("ix_bookings_start_end", "start_at", "end_at"),
        Index("ix_bookings_status", "status"),
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
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

# ================ ENGINE & SESSION ==================
engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ====================== BOT CORE ====================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ====================== FSM =========================
# Состояние, когда ждём контакты после выбора слота
class BookingContactForm(StatesGroup):
    waiting_contact = State()

class UpdateContactForm(StatesGroup):
    waiting_new_contact = State()

# ----------------- UTILITIES ------------------------
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
        f"{b.sims}с / {b.duration}мин / {b.price}₽ / {human_status(b.status)} | "
        f"{(b.client_name or '-')} {(b.client_phone or '-')}"
    )

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
            if b.status in ("pending", "confirmed"):
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
                # пример: "#123 Влад(2,⏳)" или "#125 Антон(1,✅)"
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
        f"Легенда статуса: ⏳ — ожидает подтверждения, ✅ — подтверждено"
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

def looks_like_contact(raw: str) -> bool:
    raw = raw.strip()
    # есть хотя бы одна цифра? если нет — скорее это не контакт
    return any(ch.isdigit() for ch in raw)

def split_contact(raw: str) -> tuple[str, str]:
    raw = raw.strip()
    if "," in raw:
        name_part, phone_part = raw.split(",", 1)
        return name_part.strip(), phone_part.strip()
    else:
        return raw, ""

async def ensure_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def free_sims_for_interval(start_at: datetime, end_at: datetime, exclude_id: Optional[int] = None) -> int:
    start_at, end_at = localize(start_at), localize(end_at)
    async with SessionLocal() as s:
        # зачистка просроченных pending заявок
        await s.execute(
            text(
                "UPDATE bookings "
                "SET status='cancelled' "
                "WHERE status='pending' "
                "AND expires_at IS NOT NULL "
                "AND expires_at < NOW()"
            )
        )
        await s.commit()

        q = select(func.coalesce(func.sum(Booking.sims), 0)).where(
            Booking.status.in_(("pending", "confirmed")),
            Booking.start_at < end_at,
            Booking.end_at > start_at
        )
        if exclude_id is not None:
            q = q.where(Booking.id != exclude_id)

        total_taken = (await s.execute(q)).scalar_one()
        free = MAX_SIMS - int(total_taken)
        return max(0, free)

# --------------- KEYBOARDS & MENUS ------------------
def main_menu_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Забронировать", callback_data="book:start")],
            [
                InlineKeyboardButton(text="💳 Тарифы", callback_data="tariffs"),
                InlineKeyboardButton(text="🕒 Часы работы", callback_data="hours")
            ],
            [InlineKeyboardButton(text="📞 Связаться", callback_data="contact")]
        ]
    )

# ===================== HANDLERS =====================
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
    t = "\n".join([f"{d} мин — <b>{p} ₽</b>" for d, p in PRICES.items()])
    await safe_edit_text(
        c.message,
        f"💳 Тарифы за 1 сим:\n{t}",
        reply_markup=main_menu_kb()
    )
    await c.answer()

@dp.callback_query(F.data == "contact")
async def contact_cb(c: CallbackQuery):
    await safe_edit_text(
        c.message,
        "📞 Связаться с администратором:\n"
        "• Телефон: +7 953 046-36-54\n"
        "• Telegram: @shaba_V\n"
        "Адрес: Екатеринбург, Район Академический",
        reply_markup=main_menu_kb()
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
            rows.append([InlineKeyboardButton(text=label, callback_data="noop")])

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
        and (s + timedelta(minutes=duration) <= (close_dt - SAFETY_GAP)
)
    ]

    rows = []
    for s in slots:
        end = s + timedelta(minutes=duration)
        free = await free_sims_for_interval(s, end)
        label = f"{s.strftime('%H:%M')} ({free} {sims_word(free)})"
        if free > 0:
            rows.append([
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"book:time:{int(s.timestamp())}:{duration}:{day_offset}"
                )
            ])
        else:
            rows.append([InlineKeyboardButton(text=label, callback_data="noop")])

    if not rows:
        rows.append([InlineKeyboardButton(text="Нет доступных слотов", callback_data="noop")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"book:dur:{duration}")])

    await safe_edit_text(
        c.message,
        f"Выбери время на <b>{base.strftime('%d.%m')}</b> (длительность {duration} мин):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await c.answer()

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
            text=str(n),
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

    price = price_for(duration, sims)

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

    # Сообщаем юзеру
    await m.answer(
        f"📝 Заявка #{booking_id} отправлена администратору.\n\n"
        f"Дата: <b>{human(start)}–{end.strftime('%H:%M')}</b>\n"
        f"Симуляторов: <b>{sims} {sims_word(sims)}</b>\n"
        f"Длительность: <b>{duration} мин</b>\n"
        f"Сумма: <b>{price} ₽</b>\n"
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
        f"{sims} {sims_word(sims)} | {duration} мин | {price} ₽\n"
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
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        b = await get_booking(s, bid)
        if not b:
            await c.answer("Бронь не найдена", show_alert=True)
            return

        if b.status != "pending" or (b.expires_at and b.expires_at < datetime.now(timezone.utc)):
            b.status = "cancelled"
            await s.commit()
            await c.answer("Заявка уже недействительна", show_alert=True)
            return

        free = await free_sims_for_interval(b.start_at, b.end_at, exclude_id=b.id)
        if free < b.sims:
            b.status = "cancelled"
            await s.commit()
            await c.answer("Слот занят, заявка отменена", show_alert=True)
            return

        b.status = "confirmed"
        b.expires_at = None
        await s.commit()

        user_id = b.user_id
        start_at = b.start_at
        end_at = b.end_at
        sims = b.sims
        dur = b.duration
        price = b.price
        client_name = b.client_name or "-"
        client_phone = b.client_phone or "-"

    await safe_edit_text(c.message, f"✅ Подтверждена заявка #{bid}")
    try:
        await bot.send_message(
            user_id,
            (
                f"✅ Ваша бронь #{bid} подтверждена!\n"
                f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
                f"{sims} {sims_word(sims)} | {dur} мин\n"
                f"Оплата на месте: <b>{price} ₽</b>\n"
                f"Контакт у нас есть: {client_name}, {client_phone}"
            )
        )
    except Exception:
        pass

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
    await m.answer(
        "Команды:\n"
        "• /start — меню\n"
        "• /my — мои активные заявки\n"
        "• /edit ID — изменить время своей заявки (если ещё не подтверждена)\n"
        "• /cancel ID — отменить свою заявку до начала\n"
        "• /contact ID — обновить имя и телефон по заявке\n"
        "• /day [YYYY-MM-DD] — сводка на день (админы)\n\n"
        "Пример: /edit 123\n"
        "Пример: /contact 123\n"
    )

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
                        f"Ваша бронь #{b.id} сегодня в {human(b.start_at)} "
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

                if b.expires_at and b.expires_at < datetime.now(timezone.utc):
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

            try:
                await bot.send_message(
                    b_user_id,
                    (
                        f"✅ Ваша бронь #{b_id} подтверждена автоматически!\n"
                        f"{human(b_start)}–{b_end.astimezone(TZ).strftime('%H:%M')} | "
                        f"{b_sims} {sims_word(b_sims)} | {b_dur} мин\n"
                        f"Оплата на месте: <b>{b_price} ₽</b>\n"
                        f"Контакт у нас есть: {b_name}, {b_phone}\n\n"
                        f"Ждём вас 👌"
                    )
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
    day_start = datetime.combine(target, time(0,0,tzinfo=TZ))
    close_dt = datetime.combine(target, CLOSE_T)

    # для каждого duration собираем окна
    report_lines = [f"🔍 Доступные окна {target.strftime('%d.%m.%Y')} для {need_sims} {sims_word(need_sims)}"]

    for dur in (60, 90, 120):
        win = timedelta(minutes=dur)
        t = datetime.combine(target, OPEN_T)

        slots_ok = []
        while t + win <= close_dt:
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
            text(
                "UPDATE bookings SET status='cancelled' "
                "WHERE status='pending' AND expires_at IS NOT NULL AND expires_at < NOW()"
            )
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

# ====================== RUN =========================
async def main():
    await ensure_tables()
    print("Bot started ✅")

    await setup_commands()

    reminder_task = asyncio.create_task(reminder_worker())
    autoconfirm_task = asyncio.create_task(autoconfirm_worker())
    complete_task = asyncio.create_task(complete_worker())  # 👈 новый воркер

    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        pass
    finally:
        reminder_task.cancel()
        autoconfirm_task.cancel()
        complete_task.cancel()  # 👈 отменяем тоже
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
