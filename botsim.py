 # точка входа + хендлеры
import os
import asyncio
import contextlib
from typing import Optional
from datetime import datetime, timedelta, time, timezone, date
import logging

import csv
import tempfile

from aiogram.client.session.aiohttp import AiohttpSession
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
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile,

)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text

from db import SessionLocal, Booking, Waitlist, ensure_tables, Client

from config import (
    BOT_TOKEN,
    ADMINS,
    MANAGERS,
    TZ,
    OPEN_T,
    CLOSE_T,
    MAX_SIMS,
    HOLD_MINUTES,
    PRICES,
    MAX_ACTIVE_BOOKINGS_PER_USER,
    SAFETY_GAP,
    REMIND_BEFORE,
    AUTOCONFIRM_BEFORE,
    ADDRESS_FULL, ADDRESS_AREA, ADDRESS_MAP_URL, HOWTO_TEXT,
    ACTIVE_STATUSES
)

from booking_service import free_sims_for_interval, create_pending_booking, cleanup_expired_pending

from promo_service import PROMO_RULES

from utils import (
    human,
    today_local,
    localize,
    human_status,
    sims_word,
    normalize_phone,
    looks_like_contact,
    split_contact,
    price_for
)

from keyboards import (
    main_menu_kb,
    build_month_kb,
    build_month_kb_edit,
    build_admin_booking_kb,
    build_tariffs_kb,
    build_tariffs_qty_kb,
)

from services.bonus_runtime import BONUS_RATE, BONUS_MAX_SHARE, upsert_client_stats
from services.promo_runtime import PROMOS_PENDING, PROMO_USAGE_TOTAL, PROMO_USAGE_PER_USER, apply_promo, _promo_mark_used
from services.ics_service import send_ics
from client_service import get_client_balance, get_client_by_tg, ensure_client
from commands_service import refresh_user_commands


STAFF_IDS = list(ADMINS.union(set(MANAGERS)))
# user_id -> booking_id, который мы ждём контакт
PENDING_CONTACTS: dict[int, int] = {}

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

class PromoForm(StatesGroup):
    waiting_code = State()


# Глобальный список фоновых задач, чтобы startup/shutdown могли им управлять
BG_TASKS: list[asyncio.Task] = []
# ----------------- UTILITIES ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("botsim")
logging.getLogger("aiogram").setLevel(logging.INFO)


async def setup_commands():
    """
    Глобальная настройка команд при старте бота.
    """
    # Базовые команды для обычных пользователей
    base_user_cmds: list[BotCommand] = [
        BotCommand(command="start",   description="Главное меню"),
        BotCommand(command="my",      description="Мои активные заявки"),
        BotCommand(command="map",     description="Как нас найти"),
        BotCommand(command="support", description="Связаться"),
        BotCommand(command="help",    description="Помощь"),
    ]

    # По умолчанию — эти команды для всех
    await bot.set_my_commands(commands=base_user_cmds)

    # Отдельный набор команд для менеджеров
    manager_cmds: list[BotCommand] = [
        BotCommand(command="day",  description="Расписание по дням"),
        BotCommand(command="help", description="Подсказка по кнопкам"),
    ]
    for manager_id in MANAGERS:
        try:
            await bot.set_my_commands(
                commands=manager_cmds,
                scope=BotCommandScopeChat(chat_id=manager_id),
            )
        except Exception:
            # если боту ещё не писали или нет прав — просто пропускаем
            pass

    # Расширенный набор для админов (и юзер, и служебные)
    admin_cmds = base_user_cmds + [
        BotCommand(command="day", description="Расписание по дням"),
        BotCommand(command="csv", description="Экспорт отчёта CSV"),  # если есть такая команда
    ]
    for admin_id in ADMINS:
        try:
            await bot.set_my_commands(
                commands=admin_cmds,
                scope=BotCommandScopeChat(chat_id=admin_id),
            )
        except Exception:
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
            if b.status in ACTIVE_STATUSES:
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

def contact_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Отправить мой телефон", request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

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

def build_admin_booking_kb_confirmed(bid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура для админа ПОСЛЕ подтверждения заявки:
    оставляем только кнопки 'Пришёл' / 'Не пришёл',
    чтобы не было соблазна ещё раз подтверждать/отклонять.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚫 Не пришёл",
                    callback_data=f"admin:noshow:{bid}",
                ),
                InlineKeyboardButton(
                    text="🏁 Пришёл",
                    callback_data=f"admin:done:{bid}",
                ),
            ],
        ]
    )

# ===================== HANDLERS =====================

@dp.message(Command("support"))
async def support_cmd(m: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗺 Открыть карту", url=ADDRESS_MAP_URL)],
        ]
    )
    await m.answer(
        "📞 Связаться с администратором:\n"
        "• Телефон: +7 953 046-36-54\n"
        "• Telegram: @shaba_V\n\n"
        f"📍 Адрес: {ADDRESS_FULL} ({ADDRESS_AREA})",
        reply_markup=kb
    )

@dp.message(Command("map"))
async def map_cmd(m: Message):
    await m.answer(
        f"📍 Мы находимся: {ADDRESS_FULL} ({ADDRESS_AREA})\n\n"
        "Открыть карту: " + ADDRESS_MAP_URL
    )


@dp.message(Command("ics"))
async def ics_cmd(m: Message):
    parts = m.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /ics ID")
        return

    bid = int(parts[1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != m.from_user.id:
            await m.answer("Заявка не найдена.")
            return

    if b.status not in ("confirmed", "done"):
        await m.answer("ICS доступен после подтверждения.")
        return

    await send_ics(bot, m.from_user.id, b)
    await m.answer("Файл календаря отправлен ✅")

@dp.callback_query(F.data.startswith("contact:ask:"))
async def contact_ask_cb(c: CallbackQuery, state: FSMContext):
    bid = int(c.data.split(":")[-1])
    async with SessionLocal() as s:
        b = await s.get(Booking, bid)
        if not b or b.user_id != c.from_user.id:
            await c.answer("Заявка не найдена", show_alert=True); return

    await state.update_data(bid=bid)
    await state.set_state(UpdateContactForm.waiting_new_contact)
    await c.message.answer(
        "Пришли новые данные: Имя, телефон\nНапример: Игорь, +7 999 123-45-67\n\n"
        "Или нажми кнопку ниже, чтобы отправить номер из Telegram 👇",
        reply_markup=contact_request_kb(),
    )
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

        b.status = "cancelled"
        b.expires_at = None

        # забираем данные до коммита
        start_at = b.start_at
        end_at = b.end_at
        sims = b.sims
        dur = b.duration
        price = b.price

        await s.commit()

    await c.message.answer(f"❌ Заявка #{bid} отменена.")
    await c.answer()

    # уведомление админам
    uname = c.from_user.username or c.from_user.full_name
    text = (
        f"❌ Пользователь @{uname} отменил заявку #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Освободилось: {sims} {sims_word(sims)}"
    )
    for staff_id in STAFF_IDS:
        try:
            await bot.send_message(staff_id, text)
        except Exception:
            pass

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

    # обновим персональное меню команд для этого пользователя
    await refresh_user_commands(bot, m.from_user.id)

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
    await safe_edit_text(
        c.message,
        "💳 Выбери длительность, посчитаю итог:",
        reply_markup=build_tariffs_kb()
    )
    await c.answer()

@dp.callback_query(F.data.startswith("tariffs:dur:"))
async def tariffs_pick_qty(c: CallbackQuery):
    duration = int(c.data.split(":")[-1])
    await safe_edit_text(
        c.message,
        f"Длительность: {duration} мин\nЦена за 1 сим: {PRICES[duration]} ₽\nВыбери количество:",
        reply_markup=build_tariffs_qty_kb(duration)
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

    # считаем цену и промо
    base_price = price_for(duration, sims)
    final_price, _promo_code = apply_promo(base_price, c.from_user.id)
    price_after_promo = final_price

    # смотрим бонусы
    bonus_balance = 0
    max_bonus_use = 0
    if price_after_promo > 0:
        async with SessionLocal() as s:
            bonus_balance = await get_client_balance(s, c.from_user.id)
        if bonus_balance > 0:
            # максимум 50% от суммы
            max_bonus_use = min(bonus_balance, price_after_promo // 2)

    # сохраняем базу в FSM
    await state.update_data(
        start_ts=start_ts,
        duration=duration,
        sims=sims,
        end_ts=int(end.timestamp()),
        base_price=base_price,
        price_after_promo=price_after_promo,
        bonus_max=max_bonus_use,
        bonus_used=0,   # пока не выбрали
    )

    # если бонусов использовать нечего — сразу просим контакт (старое поведение)
    if max_bonus_use <= 0:
        await state.update_data(
            price_after_promo=price_after_promo,
            bonus_planned=0,
        )
        await state.set_state(BookingContactForm.waiting_contact)

        # Обновим старое сообщение (уберём кнопки бронирования)
        await safe_edit_text(
            c.message,
            "Последний шаг 👇\nСейчас попрошу контакт 🙂"
        )

        # А тут уже просим контакт + даём кнопку
        await c.message.answer(
            (
                "Напиши, как с тобой связаться.\n"
                "Формат: Имя, телефон\n\n"
                "Например:\n"
                "Игорь, +7 999 123-45-67\n\n"
                "Или нажми кнопку ниже, чтобы отправить телефон из Telegram 👇"
            ),
            reply_markup=contact_request_kb(),
        )

        await c.answer("Жду контакт 👌")
        return

    # иначе — предлагаем потратить бонусы
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"Использовать {max_bonus_use} ₽ → к оплате {price_after_promo - max_bonus_use} ₽",
                    callback_data=f"bonus:use:{max_bonus_use}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Не использовать, копить дальше",
                    callback_data="bonus:skip"
                )
            ],
        ]
    )

    await safe_edit_text(
        c.message,
        (
            f"🧮 Итого без бонусов: <b>{price_after_promo} ₽</b>\n\n"
            f"🎁 На балансе: <b>{bonus_balance} ₽</b>\n"
            f"Можно использовать сейчас до <b>{max_bonus_use} ₽</b> "
            f"(не больше 50% стоимости).\n\n"
            f"Как поступим?"
        ),
        reply_markup=kb
    )
    await c.answer()

@dp.callback_query(F.data == "bonus:open")
async def bonus_open_cb(c: CallbackQuery):
    async with SessionLocal() as s:
        res = await s.execute(
            select(Client)
            .where(Client.tg_user_id == c.from_user.id)
            .order_by(Client.id.desc())
        )
        client = res.scalars().first()

    if client and client.bonus_balance > 0:
        text = (
            f"🎁 На твоём бонусном счёте сейчас <b>{client.bonus_balance} ₽</b>.\n"
            "Ими можно оплатить до <b>50%</b> стоимости следующего визита."
        )
    else:
        text = (
            "🎁 У тебя пока нет бонусов.\n"
            "После каждого посещения копится <b>5%</b> от суммы визита — "
            "эти бонусы можно будет потратить на до <b>50%</b> следующей игры."
        )

    await c.message.answer(text, parse_mode="HTML")
    await c.answer()

@dp.callback_query(F.data.startswith("bonus:use:"))
async def bonus_use_cb(c: CallbackQuery, state: FSMContext):
    """
    Пользователь выбрал вариант "использовать N бонусов".
    Здесь мы ТОЛЬКО запоминаем желаемую сумму списания и показываем
    пользователю предварительную цену. Реальное списание будет в book_finalize.
    """
    _, _, amount_str = c.data.split(":")
    try:
        amount = int(amount_str)
    except ValueError:
        await c.answer("Некорректная сумма бонусов", show_alert=True)
        return

    data = await state.get_data()
    price_after_promo = data["price_after_promo"]
    bonus_max = data.get("bonus_max", 0)

    # Перестраховка по всем фронтам: не больше max, не больше цены
    bonus_used = min(amount, bonus_max, price_after_promo)
    final_price_preview = price_after_promo - bonus_used

    # Запоминаем, сколько клиент ХОЧЕТ списать
    await state.update_data(
        bonus_planned=bonus_used,
    )

    # Переводим FSM в ожидание контакта
    await state.set_state(BookingContactForm.waiting_contact)

    # Обновляем сообщение с выбором бонусов
    await safe_edit_text(
        c.message,
        (
            f"Ок! Списали бы бонусами <b>{bonus_used} ₽</b>.\n"
            f"К оплате на месте останется <b>{final_price_preview} ₽</b>.\n"
            "Теперь нужен контакт 🙂"
        ),
        reply_markup=None,
    )

    # Просим контакт отдельным сообщением
    await c.message.answer(
        (
            "Напиши, как с тобой связаться:\n"
            "Имя, телефон\n\n"
            "Например:\n"
            "Игорь, +7 999 123-45-67\n\n"
            "Или нажми кнопку ниже, чтобы отправить номер из Telegram 👇"
        ),
        reply_markup=contact_request_kb(),
    )

    await c.answer("Бонусы учтены 👌")


@dp.callback_query(F.data == "bonus:skip")
async def bonus_skip_cb(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    price_after_promo = data["price_after_promo"]

    await state.update_data(
        price_after_promo=price_after_promo,
        bonus_planned=0,
    )

    await state.set_state(BookingContactForm.waiting_contact)

    await safe_edit_text(
        c.message,
        "Ок, бонусы оставляем копиться 🎁\nТеперь нужен контакт 🙂",
        reply_markup=None,
    )

    await c.message.answer(
        (
            "Последний шаг 👇\n"
            "Напиши, как с тобой связаться.\n"
            "Формат: Имя, телефон\n\n"
            "Например:\n"
            "Игорь, +7 999 123-45-67\n\n"
            "Или просто нажми кнопку ниже, чтобы отправить номер 👇"
        ),
        reply_markup=contact_request_kb(),
    )

    await c.answer("Бонусы не трогаем 👍")   

@dp.message(UpdateContactForm.waiting_new_contact)
async def update_contact_finish(m: Message, state: FSMContext):
    # 1) Если пользователь нажал "Поделиться контактом"
    if m.contact:
        client_name = m.contact.first_name or ""
        if m.contact.last_name:
            client_name += f" {m.contact.last_name}"
        client_name = client_name.strip()
        client_phone = m.contact.phone_number

    else:
        # 2) Если контакт приходит текстом — проверяем, что текст есть и похож на контакт
        if not m.text:
            await m.answer(
                "Не понял контакт 🤔\n"
                "Пришли, пожалуйста, в формате:\n"
                "Имя, телефон\n\n"
                "Например:\n"
                "Игорь, +7 999 123-45-67\n\n"
                "Или нажми кнопку ниже, чтобы отправить номер из Telegram 👇",
                reply_markup=contact_request_kb(),
            )
            return

        if not looks_like_contact(m.text):
            await m.answer(
                "Похоже, это не контакт 🤔\n"
                "Пришли данные в формате: Имя, телефон\n"
                "Например: Игорь, +7 999 123-45-67\n\n"
                "Или просто нажми кнопку «📱 Отправить мой телефон» 👇",
                reply_markup=contact_request_kb(),
            )
            return

        client_name, client_phone = split_contact(m.text)

    # дальше всё как было
    data = await state.get_data()
    bid = data["bid"]

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b or b.user_id != m.from_user.id:
            await m.answer("Что-то пошло не так, заявка больше недоступна.")
            await state.clear()
            return

        # контакт можно менять в любом статусе
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
        "Администратор получил новые данные 👌",
        reply_markup=ReplyKeyboardRemove()
    )

    admin_text = (
        f"✏️ Обновлён контакт в заявке #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Новый контакт: {client_name}, {client_phone}"
    )
    for staff_id in STAFF_IDS:
        try:
            await bot.send_message(staff_id, text)
        except Exception:
            pass

    await state.clear()

    #  константы

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
        f"Твой реферальный код:\n"
        f"<code>{code}</code>\n\n"
        f"Даст другу {REF_DISCOUNT_PERCENT}% скидки.\n"
        f"Каждый новый пользователь может применить 1 раз.\n"
        f"Ты — не можешь использовать свой код.",
        reply_markup=ReplyKeyboardRemove()
    )

async def apply_bonus_for_booking(session: AsyncSession, booking: Booking):
    """
    Начисляет бонусы за бронь, если:
    - статус == 'done'
    - бонусы ещё не были начислены (booking.bonus_applied == False)

    Ничего не коммитит, это делает вызывающий код.
    """
    if booking.status != "done":
        return

    if getattr(booking, "bonus_applied", False):
        # уже начисляли — выходим
        return

    client_name = booking.client_name
    client_phone = booking.client_phone
    tg_user_id = booking.user_id
    amount = booking.price

    if amount <= 0:
        booking.bonus_applied = True
        return

    client, earned = await upsert_client_stats(
        session,
        tg_user_id=tg_user_id,
        name=client_name,
        phone=client_phone,
        add_spent=amount,
    )

    booking.bonus_applied = True

    # 👇 ДОБАВЬ ЭТО:
    # после начисления бонусов обновим меню для пользователя
    try:
        await refresh_user_commands(bot, tg_user_id)
    except Exception:
        pass

    return client, earned

# ---------- Пользователь прислал контакт (имя + телефон) ----------
@dp.message(BookingContactForm.waiting_contact)
async def book_finalize(m: Message, state: FSMContext):
    # Если пришёл Telegram-контакт
    if m.contact:
        client_name = m.contact.first_name
        if m.contact.last_name:
            client_name += f" {m.contact.last_name}"
        client_phone = m.contact.phone_number
    else:
        client_name, client_phone = split_contact(m.text)

    data = await state.get_data()
    start_ts = data["start_ts"]
    end_ts = data["end_ts"]
    duration = data["duration"]
    sims = data["sims"]

    # цена после промокода, но ДО бонусов
    price_after_promo = data["price_after_promo"]
    bonus_planned = int(data.get("bonus_planned", 0))

    start = datetime.fromtimestamp(start_ts, tz=TZ)
    end = datetime.fromtimestamp(end_ts, tz=TZ)

    # финальная проверка слота на всякий случай
    if await free_sims_for_interval(start, end) < sims:
        await m.answer("😔 Пока ты писал контакт, слот заняли. Попробуй снова /start")
        await state.clear()
        return

    # считаем финальную цену и реально списываем бонусы
    final_price = price_after_promo
    bonus_used_real = 0

    async with SessionLocal() as s:
        # найдём/создадим клиента
        client = await ensure_client(s, m.from_user.id, client_name, client_phone)

        if bonus_planned > 0 and price_after_promo > 0:
            # перестраховка: баланс, план и 50% от суммы
            can_use = min(
                bonus_planned,
                client.bonus_balance,
                int(price_after_promo * BONUS_MAX_SHARE),
            )
            if can_use > 0:
                client.bonus_balance -= can_use
                bonus_used_real = can_use
                final_price = price_after_promo - can_use

        await s.commit()

    # создаём бронирование через сервисный слой
    b = await create_pending_booking(
        user_id=m.from_user.id,
        client_name=client_name,
        client_phone=client_phone,
        start=start,
        end=end,
        sims=sims,
        duration=duration,
        price=final_price,
    )
    booking_id = b.id
    expires_local = b.expires_at.astimezone(TZ)

    bonus_line = (
        "\n\n🎁 У нас работает бонусная программа: "
        "после фактического посещения копится 5% от суммы, "
        "которыми можно оплатить до 50% следующего визита."
    )

    # промокод
    applied = PROMOS_PENDING.pop(m.from_user.id, None)
    promo_note = ""
    if applied:
        code = applied["code"]
        rule = applied["rule"]
        _promo_mark_used(code, m.from_user.id, rule)
        promo_note = f" (со скидкой по коду {code})"

    # текст про списанные бонусы для пользователя
    bonus_note = ""
    if bonus_used_real > 0:
        bonus_note = f"\nСписано бонусами: <b>{bonus_used_real} ₽</b>."

    # Сообщаем юзеру
    await m.answer(
        f"📝 Заявка #{booking_id} отправлена администратору.\n\n"
        f"Дата: <b>{human(start)}–{end.strftime('%H:%M')}</b>\n"
        f"Симуляторов: <b>{sims} {sims_word(sims)}</b>\n"
        f"Длительность: <b>{duration} мин</b>\n"
        f"Сумма: <b>{final_price} ₽</b>{promo_note}{bonus_note}\n"
        f"Контакт: <b>{client_name}</b>, {client_phone}\n\n"
        f"Статус: <b>ожидает подтверждения</b> (до {expires_local.strftime('%H:%M')})."
        f"{bonus_line}",
        reply_markup=ReplyKeyboardRemove()
    )

    # уведомление админам
    kb = build_admin_booking_kb(booking_id)
    uname = m.from_user.username or m.from_user.full_name
    admin_bonus_note = f" (−{bonus_used_real} ₽ бонусами)" if bonus_used_real > 0 else ""

    txt = (
        f"🆕 Заявка #{booking_id} от @{uname}\n"
        f"{human(start)}–{end.strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {duration} мин | {final_price} ₽{promo_note}{admin_bonus_note}\n"
        f"Имя: {client_name}\n"
        f"Тел: {client_phone}"
    )

    for staff_id in STAFF_IDS:
        try:
            await bot.send_message(staff_id, text)
        except Exception:
            pass

    # чистим состояние
    await state.clear()
    await m.answer(
        "Готово 🙌 Заявка отправлена админу. "
        "Если нужно посмотреть статус — команда /my.\n"
        "Вернуться в меню — /start"
    )

@dp.message(Command("bonus"))
async def bonus_cmd(m: Message):
    async with SessionLocal() as s:
        result = await s.execute(
            select(Client)
            .where(Client.tg_user_id == m.from_user.id)
            .order_by(Client.id.desc())
        )
        client = result.scalars().first()

    if not client or client.bonus_balance <= 0:
        await m.answer(
            "🎁 У тебя пока нет бонусов.\n\n"
            "За каждую игру после посещения копится <b>5%</b> от суммы визита, "
            "которыми можно оплатить до <b>50%</b> следующего."
        )
        return

    await m.answer(
        f"🎁 Твой бонусный баланс: <b>{client.bonus_balance} ₽</b>\n\n"
        "Ими можно оплатить до <b>50%</b> стоимости следующего визита.\n"
        "При новом бронировании я предложу списать часть бонусов перед подтверждением 😉"
    )

    await refresh_user_commands(bot, m.from_user.id)

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
    bonus_sum = sum(
        int(b.price * BONUS_RATE)
        for b in bookings
        if b.status == "done" and getattr(b, "bonus_applied", False)
    )

    # 1. хедер и метрики
    head_lines = [
        f"📊 Отчёт за {target_date.strftime('%d.%m.%Y')}",
        "",
        f"🏁 Пришли (done): {len(done_list)} шт.",
        f"💰 Выручка (по done): {revenue_sum} ₽",
        f"🎁 Начислено бонусов за день: {bonus_sum} ₽",
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
                    .where(Waitlist.active.is_(True), Waitlist.start_at > now_local)
                )
                items = (await s.execute(q)).scalars().all()

            if items:
                logger.debug("waitlist_worker: активных подписок %d", len(items))

            for w in items:
                free = await free_sims_for_interval(w.start_at, w.end_at)
                if free >= w.sims_needed:
                    logger.info(
                        "waitlist_worker: сработала подписка #%d для user_id=%d (нужно %d, свободно %d)",
                        w.id, w.user_id, w.sims_needed, free
                    )
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
                    except Exception as e:
                        logger.exception("waitlist_worker: не удалось отправить уведомление user_id=%d: %s", w.user_id, e)

                    async with SessionLocal() as s:
                        w_db = await s.get(Waitlist, w.id)
                        if w_db:
                            w_db.active = False
                            await s.commit()
        except Exception as e:
            logger.exception("waitlist_worker: ошибка в цикле: %s", e)

        await asyncio.sleep(60)

async def _edit_show_times(c: CallbackQuery, bid: int, target_date: date, duration: int, sims: int):
    base_dt = datetime.combine(target_date, time(0,0,tzinfo=TZ))

    slots = gen_slots(base_dt)
    now_local = datetime.now(TZ)
    close_dt = datetime.combine(target_date, CLOSE_T)

    slots = [
        s for s in slots
        if (target_date != today_local() or s > now_local + timedelta(minutes=10))
        and (s + timedelta(minutes=duration) <= (close_dt - SAFETY_GAP))
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

# -------- Admin buttons --------
def is_admin(user_id: int) -> bool:
    return user_id in ADMINS

def is_manager(user_id: int) -> bool:
    return user_id in MANAGERS

def is_staff(user_id: int) -> bool:
    """Админ или менеджер (персонал)."""
    return is_admin(user_id) or is_manager(user_id)

async def get_booking(session: AsyncSession, bid: int) -> Optional[Booking]:
    return await session.get(Booking, bid)

@dp.callback_query(F.data.startswith("admin:approve:"))
async def admin_approve(c: CallbackQuery):
    if not is_staff(c.from_user.id):
        await c.answer("Недостаточно прав", show_alert=True)
        return

    bid = int(c.data.split(":")[-1])

    async with SessionLocal() as s:
        async with s.begin():
            b = (
                await s.execute(
                    select(Booking)
                    .where(Booking.id == bid)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if not b:
                await c.answer("Бронь не найдена", show_alert=True)
                return

            now = datetime.now(TZ)
            expired = (b.expires_at and b.expires_at < now)

            if expired:
                # бронь протухла по expires_at
                b.status = "cancelled"

            elif b.status == "pending":
                # Лочим пересекающиеся, считаем занятость
                await s.execute(
                    text(
                        """
                        SELECT id FROM bookings
                        WHERE status IN ('pending','confirmed','block')
                          AND start_at < :end AND end_at > :start
                        FOR UPDATE
                        """
                    ),
                    {"start": b.start_at, "end": b.end_at},
                )

                taken = (
                    await s.execute(
                        select(func.coalesce(func.sum(Booking.sims), 0)).where(
                            Booking.status.in_(("pending", "confirmed", "block")),
                            Booking.start_at < b.end_at,
                            Booking.end_at > b.start_at,
                            Booking.id != b.id,
                        )
                    )
                ).scalar_one()

                free = MAX_SIMS - int(taken)

                if free >= b.sims:
                    b.status = "confirmed"
                    b.expires_at = None
                else:
                    b.status = "cancelled"

            else:
                # Уже не pending — оставляем как есть (idempotent)
                pass

        # читаем поля ПОСЛЕ транзакции
        status = b.status
        user_id = b.user_id
        start_at, end_at = b.start_at, b.end_at
        sims, dur, price = b.sims, b.duration, b.price
        client_name = b.client_name or "-"
        client_phone = b.client_phone or "-"

    # ===== Ответы и тексты =====
    if status == "confirmed":
        # Перерисовываем админскую карточку с новыми кнопками (пришёл / не пришёл)
        kb_after = build_admin_booking_kb_confirmed(bid)
        await safe_edit_text(
            c.message,
            (
                f"✅ Подтверждена заявка #{bid}\n\n"
                f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
                f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
                f"Клиент: {client_name}, {client_phone}\n\n"
                "После визита нажми: 🏁 «Пришёл» или 🚫 «Не пришёл»."
            ),
            reply_markup=kb_after,
        )

        # Сообщаем клиенту
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
                reply_markup=confirm_user_kb(bid),
            )
        except Exception:
            pass

    elif status == "pending":
        # Теоретически сюда не попадём, но оставим на будущее
        await safe_edit_text(
            c.message,
            f"⏳ Заявка #{bid} всё ещё в ожидании",
        )

    elif status == "cancelled":
        # Не смогли подтвердить (нет мощностей или протухла)
        await safe_edit_text(
            c.message,
            f"❌ Не удалось подтвердить заявку #{bid} (слот недоступен или заявка просрочена)",
            reply_markup=None,
        )
        try:
            await bot.send_message(
                user_id,
                (
                    f"⚠️ Бронь #{bid} не удалось подтвердить — окно занято или заявка просрочена.\n"
                    f"Попробуйте выбрать другое время: /start"
                ),
            )
        except Exception:
            pass

    else:
        # Уже была confirmed/cancelled/done/no_show/block — ничего не меняли
        await safe_edit_text(
            c.message,
            f"ℹ️ Заявка #{bid} уже в статусе: {human_status(status)}",
        )
        await safe_edit_reply_markup(c.message, reply_markup=None)

    await c.answer()

@dp.callback_query(F.data.startswith("admin:contact:"))
async def admin_contact_info(c: CallbackQuery):
    if not is_staff(c.from_user.id):
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
    if not is_staff(c.from_user.id):
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
    if not is_staff(c.from_user.id):
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

        # теперь требуем, чтобы слот уже закончился
        if now_local < b.end_at.astimezone(TZ):
            await c.answer("Слишком рано отмечать визит как завершённый 🙃", show_alert=True)
            return

        # фиксируем финальный статус
        b.status = "done"
        b.expires_at = None

        # бонусы за визит
        await apply_bonus_for_booking(s, b)

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
    if not is_staff(c.from_user.id):
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
    if not is_staff(c.from_user.id):
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

    client = await get_client_by_tg(c.from_user.id)

    if not rows:
        await c.message.answer("У вас нет активных заявок.")
        await c.answer()
        return

    await c.message.answer("Ваши активные заявки:")

    for b in rows:
        text = (
            f"#{b.id} — {human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')}\n"
            f"{b.sims} {sims_word(b.sims)} | {b.duration} мин | {b.price} ₽\n"
            f"Статус: {human_status(b.status)}\n"
            f"Контакт: {(b.client_name or '—')}, {(b.client_phone or '—')}"
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✏️ Изменить время", callback_data=f"edit:open:{b.id}")],
                [InlineKeyboardButton(text="📞 Обновить контакт", callback_data=f"contact:ask:{b.id}")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel:ask:{b.id}")],
            ]
        )

        await c.message.answer(text, reply_markup=kb, parse_mode="HTML")

    # бонусы одним сообщением
    if client and client.bonus_balance > 0:
        bonus_text = (
            f"\n🎁 Твой бонусный баланс: <b>{client.bonus_balance} ₽</b>\n"
            f"Ими можно оплатить до <b>50%</b> стоимости следующего визита."
        )
    else:
        bonus_text = (
            "\n🎁 У тебя пока нет бонусов.\n"
            "За каждую игру копится <b>5%</b> от суммы визита — "
            "ими можно будет оплатить до <b>50%</b> следующего."
        )

    await c.message.answer(bonus_text, parse_mode="HTML")
    await c.answer()

@dp.message(Command("my"))
async def my_cmd(m: Message):
    now_local = datetime.now(TZ)

    async with SessionLocal() as s:
        # заявки
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

    client = await get_client_by_tg(m.from_user.id)

    if not rows:
        await m.answer("У вас нет активных заявок.")
        return

    await m.answer("Ваши активные заявки:")

    for b in rows:
        text = (
            f"#{b.id} — {human(b.start_at)}–{b.end_at.astimezone(TZ).strftime('%H:%M')}\n"
            f"{b.sims} {sims_word(b.sims)} | {b.duration} мин | {b.price} ₽\n"
            f"Статус: {human_status(b.status)}\n"
            f"Контакт: {(b.client_name or '—')}, {(b.client_phone or '—')}"
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✏️ Изменить время", callback_data=f"edit:open:{b.id}")],
                [InlineKeyboardButton(text="📞 Обновить контакт", callback_data=f"contact:ask:{b.id}")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel:ask:{b.id}")],
            ]
        )

        await m.answer(text, reply_markup=kb, parse_mode="HTML")

    # бонусы отдельным сообщением
    if client and client.bonus_balance > 0:
        bonus_text = (
            f"\n\n🎁 На бонусном балансе сейчас: <b>{client.bonus_balance} ₽</b>.\n"
            "Ими можно оплатить до <b>50%</b> стоимости следующего визита."
        )
    else:
        bonus_text = (
            "\n\n🎁 У нас есть бонусная программа: после фактического посещения "
            "5% от суммы визита копятся на бонусный баланс. "
            "Ими можно оплатить до <b>50%</b> следующего визита."
        )

    await m.answer(bonus_text, parse_mode="HTML")

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
            await m.answer(
                "Эту заявку уже нельзя изменить, она уже подтверждена.\n"
                "Если нужно другое время — отмените её (/cancel ID) и создайте новую бронь."
            )
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

@dp.callback_query(F.data.startswith("edit:open:"))
async def edit_open_cb(c: CallbackQuery):
    # edit:open:{bid}
    _, _, bid_str = c.data.split(":")
    bid = int(bid_str)

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b or b.user_id != c.from_user.id:
            await c.answer("Заявка не найдена.", show_alert=True)
            return

        if b.status != "pending":
            await c.answer("Эту заявку уже нельзя изменить (она не в ожидании).", show_alert=True)
            return

        if datetime.now(TZ) >= b.start_at.astimezone(TZ):
            await c.answer("Эту заявку уже нельзя изменить, время скоро начинается или уже началось.", show_alert=True)
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

    await c.message.answer(msg_text, reply_markup=kb)
    await c.answer()

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

        start_at = b.start_at
        end_at = b.end_at
        sims = b.sims
        dur = b.duration
        price = b.price

        await s.commit()

    await m.answer(f"❌ Заявка #{bid} отменена.")

    uname = m.from_user.username or m.from_user.full_name
    text = (
        f"❌ Пользователь @{uname} отменил заявку #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Освободилось: {sims} {sims_word(sims)}"
    )
    for staff_id in STAFF_IDS:
        try:
            await bot.send_message(staff_id, text)
        except Exception:
            pass

@dp.message(Command("help"))
async def help_cmd(m: Message):
    base = (
        "🧭 <b>Доступные команды</b>\n\n"
        "👤 <b>Пользователи:</b>\n"
        "• /start — главное меню\n"
        "• /book — начать бронирование\n"
        "• /my — ваши активные заявки\n"
        "• /map — как нас найти\n"
        "• /support — связаться с админом\n"
        "• /bonus — бонусы (если есть)\n"
    )

    if is_admin(m.from_user.id):
        admin_part = (
            "\n👮 <b>Администраторы:</b>\n"
            "• /day [YYYY-MM-DD] — расписание на день\n"
            "• /report [YYYY-MM-DD] — отчёт по дню\n"
            "• /csv YYYY-MM|YYYY-MM-DD — выгрузка CSV\n"
            "• /block — создать техперерыв\n"
            "• /unblock ID — удалить техперерыв\n"
            "• /no_show ID — отметить неявку клиента\n"
            "• /promo — управление промокодами\n"
        )
    else:
        admin_part = ""

    await m.answer(base + admin_part, parse_mode="HTML")

# -------- Reminder worker --------
async def complete_worker():
    """
    Автоматически помечает брони как 'done' ТОЛЬКО если:
    - статус по-прежнему 'confirmed'
    - слот закончился БОЛЕЕ ЧЕМ 2 ЧАСА НАЗАД
    Это даёт админу время отметить 'пришёл' / 'не пришёл' вручную.
    """
    AUTO_DONE_DELAY = timedelta(hours=2)

    while True:
        try:
            now_local = datetime.now(TZ)
            cutoff = now_local - AUTO_DONE_DELAY

            async with SessionLocal() as s:
                q = (
                    select(Booking)
                    .where(
                        Booking.status == "confirmed",
                        Booking.end_at < cutoff,
                    )
                )
                finished = (await s.execute(q)).scalars().all()

                if finished:
                    logger.info(
                        "complete_worker: авто-завершение %d брони(й), "
                        "которые закончились более %s назад",
                        len(finished),
                        AUTO_DONE_DELAY,
                    )

                    for b in finished:
                        b.status = "done"
                        b.expires_at = None
                        await apply_bonus_for_booking(s, b)

                    await s.commit()
        except Exception as e:
            logger.exception("complete_worker: ошибка в цикле: %s", e)

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

            if rows:
                logger.info("reminder_worker: отправляем напоминания по %d брони(ям)", len(rows))

            for b in rows:
                try:
                    await bot.send_message(
                        b.user_id,
                        f"⏰ Напоминание!\n"
                        f"Ваша бронь #{b.id} в {human(b.start_at)} "
                        f"({b.sims} {sims_word(b.sims)}, {b.duration} мин). Ждём вас!"
                    )
                except Exception as e:
                    logger.exception("reminder_worker: не удалось отправить напоминание по брони #%d: %s", b.id, e)

        except Exception as e:
            logger.exception("reminder_worker: ошибка в цикле: %s", e)

        await asyncio.sleep(60)

async def autoconfirm_worker():
    while True:
        try:
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

            if pendings:
                logger.debug("autoconfirm_worker: найдено %d pending-заявок в окне автоподтверждения", len(pendings))

            for b in pendings:
                async with SessionLocal() as s:
                    b = await s.get(Booking, b.id)
                    if not b:
                        continue

                    if b.status != "pending":
                        continue

                    if b.expires_at and b.expires_at < datetime.now(TZ):
                        logger.info("autoconfirm_worker: бронь #%d протухла по expires_at", b.id)
                        continue

                    free = await free_sims_for_interval(b.start_at, b.end_at, exclude_id=b.id)
                    if free < b.sims:
                        logger.info(
                            "autoconfirm_worker: бронь #%d не автоподтверждена, не хватает симов (нужно %d, свободно %d)",
                            b.id, b.sims, free
                        )
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

                logger.info("autoconfirm_worker: автоподтверждена бронь #%d для user_id=%d", b_id, b_user_id)

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
                except Exception as e:
                    logger.exception("autoconfirm_worker: не удалось отправить клиенту уведомление по брони #%d: %s", b_id, e)

                note_for_admins = (
                    f"🤖 Автоподтверждение заявки #{b_id}\n"
                    f"{human(b_start)}–{b_end.astimezone(TZ).strftime('%H:%M')} | "
                    f"{b_sims} {sims_word(b_sims)} | {b_dur} мин | {b_price} ₽\n"
                    f"Имя: {b_name}\n"
                    f"Тел: {b_phone}"
                )
                for staff_id in STAFF_IDS:
                    try:
                        await bot.send_message(staff_id, text)
                    except Exception as e:
                        logger.exception("autoconfirm_worker: не удалось отправить уведомление админу %d: %s", admin_id, e)

        except Exception as e:
            logger.exception("autoconfirm_worker: ошибка в основном цикле: %s", e)

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
        for staff_id in STAFF_IDS:
            try:
                await bot.send_message(staff_id, text)
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
            await c.answer("Заявка не найдена", show_alert=True)
            return
        if b.status not in ("confirmed", "done"):
            await c.answer("ICS доступен после подтверждения.", show_alert=True)
            return

    await send_ics(bot, c.from_user.id, b)
    await c.answer("Файл календаря отправлен ✅")

@dp.callback_query(F.data == "promo:open")
async def promo_open_cb(c: CallbackQuery, state: FSMContext):
    await state.set_state(PromoForm.waiting_code)
    await c.message.answer(
        "Введи промокод одним сообщением (только код, без /promo).\n"
        "Например: <code>WELCOME10</code>",
        parse_mode="HTML"
    )
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
        cleaned = await cleanup_expired_pending(s)
        if cleaned:
            logger.info("day_cmd: отменено %d протухших pending-брони(й) перед построением расписания", cleaned)

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

@dp.message(PromoForm.waiting_code)
async def promo_from_button(m: Message, state: FSMContext):
    code = m.text.strip().upper()
    rule = PROMO_RULES.get(code)
    if not rule:
        await m.answer("Промокод не найден 😕")
        await state.clear()
        return

    # запрет использования своего реф-кода
    if rule.get("owner_id") == m.from_user.id:
        await m.answer("Нельзя использовать свой реферальный код.")
        await state.clear()
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

    await m.answer("\n".join(lines), parse_mode="HTML")
    await state.clear()

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

@dp.message(Command("help"))
async def help_cmd(m: Message):
    base = (
        "🧭 <b>Доступные команды</b>\n\n"
        "👤 <b>Пользователи:</b>\n"
        "• /start — главное меню\n"
        "• /book — начать бронирование\n"
        "• /my — ваши активные заявки\n"
        "• /map — как нас найти\n"
        "• /support — связаться с админом\n"
        "• /bonus — бонусы (если есть)\n"
    )

    if is_admin(m.from_user.id):
        admin_part = (
            "\n👮 <b>Администраторы:</b>\n"
            "• /day [YYYY-MM-DD] — расписание на день\n"
            "• /report [YYYY-MM-DD] — отчёт по дню\n"
            "• /csv YYYY-MM|YYYY-MM-DD — выгрузка CSV\n"
            "• /block — создать техперерыв\n"
            "• /unblock ID — удалить техперерыв\n"
            "• /no_show ID — отметить неявку клиента\n"
            "• /promo — управление промокодами\n"
        )
    elif is_manager(m.from_user.id):
        admin_part = (
            "\n👨‍💼 <b>Персонал (менеджеры):</b>\n"
            "Ты получаешь заявки в чат с кнопками:\n"
            "• ✅ Подтвердить / ❌ Отклонить\n"
            "• 🏁 Пришёл / 🚫 Не пришёл\n"
            "Работай только через эти кнопки, чтобы не мешать клиентам 🙂\n"
        )
    else:
        admin_part = ""

    await m.answer(base + admin_part, parse_mode="HTML")

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

    # 1) если юзер отправил Telegram-контакт — используем его
    if m.contact:
        client_name = m.contact.first_name
        if m.contact.last_name:
            client_name += f" {m.contact.last_name}"
        client_name = client_name.strip()
        client_phone = m.contact.phone_number
    else:
        # 2) если нет текста или текст не похож на контакт — игнорим
        if not m.text or not looks_like_contact(m.text):
            return
        client_name, client_phone = split_contact(m.text)

    bid = PENDING_CONTACTS.pop(m.from_user.id)

    async with SessionLocal() as s:
        b = await s.get(Booking, bid)

        if not b or b.user_id != m.from_user.id:
            await m.answer("Не получилось обновить контакт по заявке. Если что, можно написать администратору напрямую 🙌")
            return

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
        "Спасибо! Администратор получил новые данные 👌",
        reply_markup=ReplyKeyboardRemove()
    )

    admin_text = (
        f"✏️ Обновлён контакт в заявке #{bid}\n"
        f"{human(start_at)}–{end_at.astimezone(TZ).strftime('%H:%M')} | "
        f"{sims} {sims_word(sims)} | {dur} мин | {price} ₽\n"
        f"Новый контакт: {client_name}, {client_phone}"
    )

    for staff_id in STAFF_IDS:
        try:
            await bot.send_message(staff_id, text)
        except Exception:
            pass

    # дублируем логику update_contact_finish: парсим текст, пишем в БД,
    # отвечаем юзеру, шлём админам.

async def cleanup_pending_worker():
    while True:
        try:
            now_local = datetime.now(TZ)
            async with SessionLocal() as s:
                cleaned = await cleanup_expired_pending(s, now_local)
            if cleaned:
                logger.info(
                    "cleanup_pending_worker: отменено %d протухших pending-брони(й) на %s",
                    cleaned, now_local.isoformat()
                )
            # если cleaned == 0 — молчим, чтобы не спамить лог
        except Exception:
            logger.exception("cleanup_pending_worker: ошибка при очистке pending")
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

    # Просто ждём polling; startup/shutdown сами поднимут/погасят BG_TASKS
    await dp.start_polling(bot, polling_timeout=60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user ⏹")