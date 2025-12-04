# services/ics_service.py

from datetime import datetime, timezone
import uuid
import os
import tempfile

from aiogram import Bot
from aiogram.types import FSInputFile

from db import Booking
from config import ADDRESS_FULL
from utils import sims_word


def _ics_text_for_booking(b: Booking) -> str:
    uid = uuid.uuid4().hex
    now_utc = datetime.now(timezone.utc)
    return (
        "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//simclub//ru//\nBEGIN:VEVENT\n"
        f"UID:{uid}\nDTSTAMP:{now_utc.strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTSTART:{b.start_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}\n"
        f"DTEND:{b.end_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}\n"
        f"SUMMARY:Симрейсинг — {b.sims} {sims_word(b.sims)}\n"
        f"LOCATION:{ADDRESS_FULL}\n"
        f"DESCRIPTION:{b.sims} {sims_word(b.sims)}, {b.duration} мин\nEND:VEVENT\nEND:VCALENDAR\n"
    )


async def send_ics(bot: Bot, chat_id: int, b: Booking):
    ics = _ics_text_for_booking(b)
    fd, path = tempfile.mkstemp(prefix=f"booking_{b.id}_", suffix=".ics")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(ics)
        await bot.send_document(chat_id, FSInputFile(path), caption=f"Календарь для брони #{b.id}")
    finally:
        if os.path.exists(path):
            os.remove(path)
