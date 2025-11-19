# db.py
import os
from datetime import datetime

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
    Boolean,
)

from config import DATABASE_URL, MAX_SIMS  # <-- вот это добавляем
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан. Добавь его в .env")

# --- общие константы для моделей ---


# ================== BASE ==================
class Base(DeclarativeBase):
    pass


# ================== MODELS =================
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
        CheckConstraint(
            f"sims_needed >= 1 AND sims_needed <= {MAX_SIMS}",
            name="ck_waitlist_sims_range"
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration: Mapped[int] = mapped_column(Integer, nullable=False)
    sims_needed: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )


class Booking(Base):
    __tablename__ = "bookings"
    __table_args__ = (
        Index("ix_bookings_start_end", "start_at", "end_at"),
        Index("ix_bookings_user_active", "user_id", "status", "end_at"),
        Index("ix_bookings_status_start", "status", "start_at"),
        Index("ix_bookings_status_end", "status", "end_at"),
        Index("ix_bookings_status_time", "status", "start_at", "end_at"),
        Index("ix_bookings_user_active_future", "user_id", "status", "end_at"),
        CheckConstraint("sims >= 1", name="ck_sims_ge_1"),
        CheckConstraint("duration IN (30,60,90,120)", name="ck_duration_allowed"),
        CheckConstraint("end_at > start_at", name="ck_end_gt_start"),
        CheckConstraint("price >= 0", name="ck_price_ge_0"),
        CheckConstraint(
            "status IN ('pending','confirmed','cancelled','done','no_show','block')",
            name="ck_status_enum",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)

    client_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    client_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)

    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sims: Mapped[int] = mapped_column(Integer, nullable=False)
    duration: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


# ================== ENGINE & SESSION ==================
engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def ensure_tables() -> None:
    """Создаёт таблицы, если их ещё нет."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
