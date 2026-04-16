"""
Telegram Push Notifications — Energy Intelligence Agent
"""

import os
import logging
import httpx

log = logging.getLogger(__name__)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


async def send_telegram(summary: dict, tickers: list, extras: list,
                        slot_label: str, lang: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured — skipping")
        return

    he    = lang == "he"
    items = summary.get("items", [])
    n     = len(items)
    n_new = len([i for i in items if i.get("badge") in ("חדש", "New")])

    # Tickers
    ticker_lines = []
    for t in tickers:
        arrow = "🟢" if t.change_pct >= 0 else "🔴"
        sign  = "+" if t.change_pct >= 0 else ""
        name  = t.name_he if he else t.name_en
        ticker_lines.append(f"{arrow} {name}: {sign}{t.change_pct:.2f}%")
    for e in extras:
        arrow = "🟢" if e.change_pct >= 0 else "🔴"
        sign  = "+" if e.change_pct >= 0 else ""
        name  = e.name_he if he else e.name_en
        price_str = f"₪{e.price:.3f}" if e.currency == "ILS" else f"${e.price:.2f}"
        ticker_lines.append(f"{arrow} {name}: {price_str} ({sign}{e.change_pct:.2f}%)")

    # Top 3 items
    items_text = ""
    for item in items[:3]:
        items_text += f"\n*{item.get('badge','')}* — {item.get('title','')}\n↳ {item.get('delta','')}\n"

    if he:
        msg = (
            f"*סיכום אנרגיה וגז | {slot_label}*\n"
            f"{'─'*28}\n\n"
            f"📋 *תמצית מנהלים*\n{summary.get('executive_summary','')}\n\n"
            f"📊 *{n} פרסומים | {n_new} חדשים*\n{items_text}\n"
            f"{'─'*28}\n"
            f"💹 *שערי שוק*\n" + "\n".join(ticker_lines) +
            f"\n\n⚠️ _נוצר באמצעות AI. אינו ייעוץ פיננסי._"
        )
    else:
        msg = (
            f"*Energy & Gas | {slot_label}*\n"
            f"{'─'*28}\n\n"
            f"📋 *Executive Summary*\n{summary.get('executive_summary','')}\n\n"
            f"📊 *{n} updates | {n_new} new*\n{items_text}\n"
            f"{'─'*28}\n"
            f"💹 *Market Data*\n" + "\n".join(ticker_lines) +
            f"\n\n⚠️ _AI-generated. Not financial advice._"
        )

    async with httpx.AsyncClient() as client:
        r = await client.post(f"{API}/sendMessage", json={
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       msg,
            "parse_mode": "Markdown",
        })
    if r.status_code == 200:
        log.info("Telegram sent")
    else:
        log.error(f"Telegram failed: {r.status_code} {r.text[:200]}")


async def send_telegram_test() -> None:
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{API}/sendMessage", json={
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       "✅ *סוכן האנרגיה מחובר בהצלחה!*\nעדכונים: 10:00 · 13:30 · 18:00 · כל יום",
            "parse_mode": "Markdown",
        })
    print("Telegram OK" if r.status_code == 200 else f"Error: {r.text}")
