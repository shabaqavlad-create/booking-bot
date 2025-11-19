# promo_service.py
from datetime import date
from typing import Optional

PROMO_RULES = {
    "WELCOME10": {
        "kind": "percent", "value": 10,
        "until": date(2099, 1, 1),
        "one_time": True,
        "per_user_limit": 1,
        "total_limit": 500,
        "min_total": 0,
    },
    "FIX100": {
        "kind": "fixed", "value": 100,
        "until": date(2099, 1, 1),
        "one_time": False,
        "per_user_limit": 10,
        "total_limit": 1000,
        "min_total": 600,
    },
}

def apply_promo(
    code: str,
    base_amount: int,
    *,
    used_by_user: int = 0,
    used_total: int = 0,
) -> tuple[int, Optional[str]]:
    """
    Возвращает (final_amount, error_message).
    error_message = None, если промо успешно применён.
    """
    code = code.upper().strip()
    rule = PROMO_RULES.get(code)
    if not rule:
        return base_amount, "❌ Промокод не найден."

    today = date.today()
    if today > rule["until"]:
        return base_amount, "❌ Срок действия промокода истёк."

    if base_amount < rule["min_total"]:
        return base_amount, f"❌ Промокод действует от {rule['min_total']} ₽."

    if rule.get("one_time") and used_by_user >= 1:
        return base_amount, "❌ Промокод уже использован вами."

    if used_total >= rule["total_limit"]:
        return base_amount, "❌ Лимит промокода исчерпан."

    kind = rule["kind"]
    value = rule["value"]

    if kind == "percent":
        discount = base_amount * value // 100
    elif kind == "fixed":
        discount = value
    else:
        return base_amount, "❌ Некорректный тип промокода."

    final = max(base_amount - discount, 0)
    return final, None
