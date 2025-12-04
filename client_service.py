# client_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db import SessionLocal, Client  # или как у тебя называются

async def get_client_balance(session, tg_user_id: int) -> int:
    result = await session.execute(
        select(Client.bonus_balance)
        .where(Client.tg_user_id == tg_user_id)
        .order_by(Client.id.desc())
    )
    row = result.first()           # берём первую строку, даже если их несколько
    return row[0] if row else 0

async def get_client_by_tg(user_id: int) -> Client | None:
    async with SessionLocal() as s:
        q = (
            select(Client)
            .where(Client.tg_user_id == user_id)
            .order_by(Client.id.desc())
        )
        res = await s.execute(q)
        return res.scalars().first()

async def ensure_client(
    session: AsyncSession,
    tg_user_id: int,
    name: str | None,
    phone: str | None,
) -> Client:
    result = await session.execute(
        select(Client)
        .where(Client.tg_user_id == tg_user_id)
        .order_by(Client.id)
    )
    client = result.scalars().first()

    if client is None:
        client = Client(
            tg_user_id=tg_user_id,
            name=name,
            phone=phone,
            total_spent=0,
            bonus_balance=0,
        )
        session.add(client)
        await session.flush()
    else:
        changed = False
        if name and name != client.name:
            client.name = name
            changed = True
        if phone and phone != client.phone:
            client.phone = phone
            changed = True
        if changed:
            await session.flush()
    return client
