# booking_service.py  # бизнес-логика брони
from datetime import datetime, timedelta

from sqlalchemy import select, text

from utils import _ensure_tz
from config import TZ, MAX_SIMS, HOLD_MINUTES
from db import SessionLocal, Booking
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
# какие статусы считаем "занимающими симы"
ACTIVE_STATUSES = ("pending", "confirmed", "block")

async def cleanup_expired_pending(session: AsyncSession, now: datetime | None = None) -> int:
    """
    Переводит просроченные pending-заявки в cancelled.
    Возвращает количество изменённых записей.
    """
    if now is None:
        now = datetime.now(TZ)

    result = await session.execute(
        text("""
            UPDATE bookings
            SET status = 'cancelled'
            WHERE status = 'pending'
              AND expires_at IS NOT NULL
              AND expires_at < :now
        """),
        {"now": now}
    )
    await session.commit()
    return result.rowcount or 0

async def free_sims_for_interval(
    start: datetime,
    end: datetime,
    exclude_id: int | None = None,
) -> int:
    """
    Сколько свободных симов в интервале [start, end).
    Открывает свою сессию к БД.
    Если передан exclude_id — не учитывает эту бронь в рассчёте.
    """
    async with SessionLocal() as s:
        conditions = [
            Booking.status.in_(ACTIVE_STATUSES),
            Booking.start_at < end,
            Booking.end_at > start,
        ]
        if exclude_id is not None:
            conditions.append(Booking.id != exclude_id)

        q = select(func.coalesce(func.sum(Booking.sims), 0)).where(*conditions)
        result = await s.execute(q)
        bookings = result.scalars().all()
        busy = (await s.execute(q)).scalar_one()

    free = MAX_SIMS - busy
    return max(free, 0)


async def create_pending_booking(
    *,
    user_id: int,
    client_name: str,
    client_phone: str,
    start: datetime,
    end: datetime,
    sims: int,
    duration: int,
    price: int,
) -> Booking:
    """
    Создаёт запись Booking в статусе pending с таймаутом HOLD_MINUTES.
    Открывает свою сессию к БД и возвращает объект Booking.
    """
    expires_at = datetime.now(TZ) + timedelta(minutes=HOLD_MINUTES)

    async with SessionLocal() as s:
        b = Booking(
            user_id=user_id,
            client_name=client_name,
            client_phone=client_phone,
            start_at=start,
            end_at=end,
            sims=sims,
            duration=duration,
            price=price,
            status="pending",
            expires_at=expires_at,
        )
        s.add(b)
        await s.commit()
        await s.refresh(b)
        return b
