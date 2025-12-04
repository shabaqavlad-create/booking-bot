#keyboards.py       # клавиатуры
# keyboards.py
from __future__ import annotations

import calendar
from datetime import date, timedelta

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import PRICES, MAX_SIMS
from utils import sims_word, today_local, within_booking_window, price_for, RU_MONTHS


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Забронировать", callback_data="book:start")],
            [InlineKeyboardButton(text="📄 Мои заявки", callback_data="my:list")],
            [
                InlineKeyboardButton(text="💳 Тарифы", callback_data="tariffs"),
                InlineKeyboardButton(text="🕒 Часы работы", callback_data="hours"),
            ],
            [InlineKeyboardButton(text="📚 Помощь", callback_data="help:open")],
            [InlineKeyboardButton(text="📞 Связаться", callback_data="contact")],
            [InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo:open")],
            [InlineKeyboardButton(text="🎁 Мои бонусы", callback_data="bonus:open")],
        ]
    )


def build_month_kb(year: int, month: int, duration: int) -> InlineKeyboardMarkup:
    """
    Календарь для выбора даты брони.
    callback: book:date:YYYY-MM-DD:DURATION
    навигация: cal:page:YYYY-M:DURATION
    """
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdayscalendar(year, month)

    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=f"{RU_MONTHS[month]} {year}", callback_data="noop")]
    ]
    rows.append([InlineKeyboardButton(text=t, callback_data="noop")
                for t in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

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
                        callback_data=f"book:date:{d.isoformat()}:{duration}",
                    )
                )
            else:
                row.append(InlineKeyboardButton(text="·", callback_data="noop"))
        rows.append(row)

    cur_first = date(year, month, 1)
    prev_month = (cur_first - timedelta(days=1)).replace(day=1)
    next_month = (cur_first + timedelta(days=32)).replace(day=1)

    nav: list[InlineKeyboardButton] = []
    if prev_month >= today_local().replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"cal:page:{prev_month.year}-{prev_month.month}:{duration}",
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
                callback_data=f"cal:page:{next_month.year}-{next_month.month}:{duration}",
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_month_kb_edit(year: int, month: int, bid: int, duration: int, sims: int) -> InlineKeyboardMarkup:
    """
    Календарь для редактирования заявки.
    callback: edit:date:BID:YYYY-MM-DD:DURATION:SIMS
    навигация: editcal:page:BID:YYYY-M:DURATION:SIMS
    """
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdayscalendar(year, month)

    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=f"{RU_MONTHS[month]} {year}", callback_data="noop")]
    ]
    rows.append([InlineKeyboardButton(text=t, callback_data="noop")
                for t in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

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
                        callback_data=f"edit:date:{bid}:{d.isoformat()}:{duration}:{sims}",
                    )
                )
            else:
                row.append(InlineKeyboardButton(text="·", callback_data="noop"))
        rows.append(row)

    cur_first = date(year, month, 1)
    prev_month = (cur_first - timedelta(days=1)).replace(day=1)
    next_month = (cur_first + timedelta(days=32)).replace(day=1)

    nav: list[InlineKeyboardButton] = []
    if prev_month >= today_local().replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"editcal:page:{bid}:{prev_month.year}-{prev_month.month}:{duration}:{sims}",
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    nav.append(InlineKeyboardButton(text="Закрыть", callback_data="noop"))

    last_allowed = today_local() + timedelta(days=30)
    if next_month <= last_allowed.replace(day=1):
        nav.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"editcal:page:{bid}:{next_month.year}-{next_month.month}:{duration}:{sims}",
            )
        )
    else:
        nav.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_booking_kb(bid: int) -> InlineKeyboardMarkup:
    """Единая клавиатура для админского сообщения о заявке."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin:approve:{bid}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin:reject:{bid}"),
            ],
            [
                InlineKeyboardButton(text="📞 Контакт", callback_data=f"admin:contact:{bid}"),
                InlineKeyboardButton(text="✏ Запросить контакт", callback_data=f"admin:askcontact:{bid}"),
            ],
            [
                InlineKeyboardButton(text="🚫 Не пришёл", callback_data=f"admin:noshow:{bid}"),
                InlineKeyboardButton(text="🏁 Пришёл", callback_data=f"admin:done:{bid}"),
            ],
        ]
    )

def build_admin_booking_kb_confirmed(bid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура для уже подтверждённой заявки:
    - контакт
    - запросить новый контакт
    - отметить пришёл / не пришёл
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📞 Контакт",
                    callback_data=f"admin:contact:{bid}"
                ),
                InlineKeyboardButton(
                    text="✏️ Уточнить контакт",
                    callback_data=f"admin:askcontact:{bid}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🏁 Пришёл",
                    callback_data=f"admin:done:{bid}"
                ),
                InlineKeyboardButton(
                    text="🚫 Не пришёл",
                    callback_data=f"admin:noshow:{bid}"
                ),
            ],
        ]
    )

def build_tariffs_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{d} мин — {PRICES[d]} ₽/сим", callback_data=f"tariffs:dur:{d}")]
            for d in (30, 60, 90, 120)
        ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home")]]
    )


def build_tariffs_qty_kb(duration: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(
            text=f"{n} — {price_for(duration, n)} ₽ итого",
            callback_data=f"tariffs:qty:{duration}:{n}"
        )]
        for n in range(1, MAX_SIMS + 1)
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="tariffs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
