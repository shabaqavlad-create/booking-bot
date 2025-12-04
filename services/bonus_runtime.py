# services/bonus_runtime.py

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import Client
from utils import normalize_phone

BONUS_RATE = 0.05        # 5% от суммы брони в бонусы
BONUS_MAX_SHARE = 0.5    # бонусами можно оплатить до 50% визита


async def upsert_client_stats(
    session: AsyncSession,
    tg_user_id: int,
    name: str | None,
    phone: str | None,
    add_spent: int,
):
    """
    Находит или создаёт клиента и обновляет статистику/бонусы.
    Возвращает (client, earned_bonus).
    """
    phone_norm = normalize_phone(phone) if phone else None

    q = select(Client)
    if phone_norm:
        q = q.where(Client.phone == phone_norm)
    else:
        q = q.where(Client.tg_user_id == tg_user_id)

    result = await session.execute(q.order_by(Client.id))
    client = result.scalars().first()

    if not client:
        client = Client(
            tg_user_id=tg_user_id,
            phone=phone_norm,
            name=name or "",
            total_bookings=0,
            total_spent=0,
            bonus_balance=0,
        )
        session.add(client)

    client.tg_user_id = tg_user_id
    if phone_norm:
        client.phone = phone_norm
    if name:
        client.name = name

    client.total_bookings += 1
    client.total_spent += add_spent

    earned = int(add_spent * BONUS_RATE)
    client.bonus_balance += earned

    return client, earned
