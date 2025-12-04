# services/promo_runtime.py

from typing import Optional

from config import TZ
from utils import today_local
from promo_service import PROMO_RULES  # уже есть у тебя

# user_id -> {"code": str, "rule": dict}
PROMOS_PENDING: dict[int, dict] = {}

# учёт применений
PROMO_USAGE_TOTAL: dict[str, int] = {}                # code -> total uses
PROMO_USAGE_PER_USER: dict[str, dict[int, int]] = {}  # code -> {user_id: n}


def _promo_can_use(code: str, rule: dict, user_id: int, base_price: int) -> tuple[bool, str | None]:
    if rule.get("until") and today_local() > rule["until"]:
        return False, "Срок действия промокода истёк."

    owner_id = rule.get("owner_id")
    if owner_id is not None and owner_id == user_id:
        return False, "Нельзя использовать свой реферальный код."

    if base_price < int(rule.get("min_total", 0)):
        return False, f"Минимальная сумма для этого промокода: {rule['min_total']} ₽."

    total_used = PROMO_USAGE_TOTAL.get(code, 0)
    total_limit = rule.get("total_limit")
    if total_limit is not None and total_used >= total_limit:
        return False, "Лимит промокода исчерпан."

    per_user_limit = int(rule.get("per_user_limit", 0)) or None
    if per_user_limit:
        used_by_user = PROMO_USAGE_PER_USER.get(code, {}).get(user_id, 0)
        if used_by_user >= per_user_limit:
            return False, "Лимит использования на пользователя исчерпан."

    return True, None


def apply_promo(base_price: int, user_id: int) -> tuple[int, Optional[str]]:
    promo = PROMOS_PENDING.get(user_id)
    if not promo:
        return base_price, None

    code = promo["code"]
    rule = promo["rule"]
    ok, reason = _promo_can_use(code, rule, user_id, base_price)
    if not ok:
        PROMOS_PENDING.pop(user_id, None)
        return base_price, None

    if rule["kind"] == "percent":
        new_price = int(round(base_price * (100 - int(rule["value"])) / 100))
    elif rule["kind"] == "fixed":
        new_price = max(0, base_price - int(rule["value"]))
    else:
        new_price = base_price

    return new_price, code


def _promo_mark_used(code: str, user_id: int, rule: dict):
    PROMO_USAGE_TOTAL[code] = PROMO_USAGE_TOTAL.get(code, 0) + 1
    per_user = PROMO_USAGE_PER_USER.setdefault(code, {})
    per_user[user_id] = per_user.get(user_id, 0) + 1

    if rule.get("one_time"):
        PROMOS_PENDING.pop(user_id, None)
