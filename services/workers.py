# services/workers.py

import asyncio
from datetime import datetime, timedelta

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func

from db import SessionLocal, Booking, Waitlist
from config import TZ, MAX_SIMS, SAFETY_GAP, REMIND_BEFORE, AUTOCONFIRM_BEFORE, ADDRESS_FULL, ADDRESS_AREA, ADMINS
from booking_service import free_sims_for_interval, cleanup_expired_pending
from utils import human, sims_word, human_status
from .bonus_runtime import BONUS_RATE
from botsim import bot  # см. комментарий ниже
import logging

logger = logging.getLogger("workers")


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


async def complete_worker(apply_bonus_for_booking):
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
                for admin_id in ADMINS:
                    try:
                        await bot.send_message(admin_id, note_for_admins)
                    except Exception as e:
                        logger.exception("autoconfirm_worker: не удалось отправить уведомление админу %d: %s", admin_id, e)

        except Exception as e:
            logger.exception("autoconfirm_worker: ошибка в основном цикле: %s", e)

        await asyncio.sleep(60)


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
        except Exception:
            logger.exception("cleanup_pending_worker: ошибка при очистке pending")
        await asyncio.sleep(60)
