# commands_service.py
from typing import List

from aiogram import Bot
from aiogram.types import BotCommand, BotCommandScopeChat
from sqlalchemy import select

from db import SessionLocal, Client
from config import ADMINS


async def refresh_user_commands(user_id: int):
    """
    Обновляет список команд в меню (нижняя левая кнопка) для КОНКРЕТНОГО юзера.

    - Для менеджеров показываем только служебные команды (без /start, /my и т.п.).
    - Для обычных пользователей показываем базовый набор + /bonus, если есть бонусы.
    - Для админов добавляем /day и /csv.
    """

    # ---- Менеджеры (но не админы) ----
    if is_manager(user_id) and not is_admin(user_id):
        manager_cmds: list[BotCommand] = [
            BotCommand(command="day",  description="Расписание по дням"),
            BotCommand(command="help", description="Подсказка по кнопкам"),
        ]
        try:
            await bot.set_my_commands(
                commands=manager_cmds,
                scope=BotCommandScopeChat(chat_id=user_id),
            )
        except Exception:
            pass
        return

    # ---- Обычные пользователи / админы ----
    cmds: list[BotCommand] = [
        BotCommand(command="start",   description="Главное меню"),
        BotCommand(command="my",      description="Мои активные заявки"),
        BotCommand(command="map",     description="Как нас найти"),
        BotCommand(command="support", description="Связаться"),
        BotCommand(command="help",    description="Помощь"),
    ]

    has_bonus = False
    async with SessionLocal() as s:
        res = await s.execute(
            select(Client)
            .where(Client.tg_user_id == user_id)
            .order_by(Client.id.desc())
        )
        client = res.scalars().first()
        if client and client.bonus_balance > 0:
            has_bonus = True

    if has_bonus:
        # вставим /bonus после /my
        cmds.insert(2, BotCommand(command="bonus", description="Мои бонусы"))

    # админу — плюс служебные команды
    if is_admin(user_id):
        cmds.append(BotCommand(command="day", description="Расписание по дням"))
        cmds.append(BotCommand(command="csv", description="Экспорт отчёта CSV"))

    try:
        await bot.set_my_commands(
            commands=cmds,
            scope=BotCommandScopeChat(chat_id=user_id),
        )
    except Exception:
        pass
