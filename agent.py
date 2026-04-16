"""
Energy & Gas Intelligence Agent
================================
Runs on GitHub Actions — no local machine required.
Schedule: 10:00 / 13:30 / 18:00 Israel time, 7 days a week.
"""

import os
import json
import hashlib
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup
from anthropic import Anthropic
from gmail_sender import send_email_gmail
from telegram_sender import send_telegram

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

IL_TZ = ZoneInfo("Asia/Jerusalem")

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SENDER_EMAIL      = os.environ.get("SENDER_EMAIL", "")
RECIPIENTS_HE     = [r for r in os.environ.get("RECIPIENTS_HE", "").split(",") if r.strip()]
RECIPIENTS_EN     = [r for r in os.environ.get("RECIPIENTS_EN", "").split(",") if r.strip()]
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")

SEND_SLOTS = [
    (10,  0, "עדכון בוקר",   "Morning Update"),
    (13, 30, "עדכון צהריים", "Midday Update"),
    (18,  0, "סיכום יומי",   "End-of-Day Summary"),
]

# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------

TICKERS = [
    {"symbol": "ISRAMCO.TA", "name_he": "ישראמקו נגב",   "name_en": "Isramco Negev",   "currency": "ILS"},
    {"symbol": "TMRP.TA",    "name_he": "תמר פטרוליום",  "name_en": "Tamar Petroleum",  "currency": "ILS"},
    {"symbol": "NWMD.TA",    "name_he": "ניומד אנרגי",   "name_en": "Newmed Energy",    "currency": "ILS"},
    {"symbol": "RATIO.TA",   "name_he": "רציו פטרוליום", "name_en": "Ratio Petroleum",  "currency": "ILS"},
    {"symbol": "DLEKG.TA",   "name_he": "קבוצת דלק",     "name_en": "Delek Group",      "currency": "ILS"},
    {"symbol": "CVX",        "name_he": "שברון",          "name_en": "Chevron (CVX)",    "currency": "USD"},
    {"symbol": "ENOG",       "name_he": "אנרגיאן",       "name_en": "Energean (ENOG)",  "currency": "USD"},
]

MARKET_EXTRAS = [
    {"symbol": "USDILS=X", "name_he": "דולר / שקל",        "name_en": "USD / ILS",           "currency": "ILS"},
    {"symbol": "BZ=F",     "name_he": "נפט ברנט ($/חבית)", "name_en": "Brent Crude ($/bbl)", "currency": "USD"},
]

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

SOURCES = [
    {"url": "https://www.magna.isa.gov.il/",                                 "name": 'מאיה (MAGNA)',      "cat": "israel_gas"},
    {"url": "https://www.gov.il/he/departments/natural_gas_authority",        "name": 'נתג"ז',             "cat": "israel_gas"},
    {"url": "https://www.gas-association.org.il/",                            "name": "איגוד הגז",         "cat": "israel_gas"},
    {"url": "https://www.gov.il/he/departments/ministry_of_energy",           "name": "משרד האנרגיה",      "cat": "israel_gas"},
    {"url": "https://www.isramco.co.il/",                                     "name": "ישראמקו",           "cat": "israel_gas"},
    {"url": "https://www.energean.com/media/press-releases/",                 "name": "אנרגיאן",           "cat": "israel_gas"},
    {"url": "https://www.delek-group.com/news/",                              "name": "קבוצת דלק",         "cat": "israel_gas"},
    {"url": "https://www.gov.il/he/departments/israel_electricity_authority", "name": "רשות החשמל",        "cat": "israel_electric"},
    {"url": "https://www.noga-iso.co.il/",                                    "name": "נגה",               "cat": "israel_electric"},
    {"url": "https://www.iec.co.il/",                                         "name": "חברת החשמל",        "cat": "israel_electric"},
    {"url": "https://www.bdo.co.il/",                                         "name": "BDO ישראל",         "cat": "analysis"},
    {"url": "https://www.chevron.com/newsroom",                               "name": "Chevron Newsroom",  "cat": "international"},
    {"url": "https://www.reuters.com/business/energy/",                       "name": "Reuters Energy",    "cat": "international"},
    {"url": "https://www.mees.com/",                                          "name": "MEES",              "cat": "international"},
    {"url": "https://www.globes.co.il/news/home.aspx?fid=596",               "name": "גלובס אנרגיה",      "cat": "media"},
    {"url": "https://www.calcalist.co.il/markets/",                           "name": "כלכליסט",           "cat": "media"},
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Article:
    title: str
    url: str
    source_name: str
    cat: str
    snippet: str
    content_hash: str

@dataclass
class TickerData:
    symbol: str
    name_he: str
    name_en: str
    price: float
    change_pct: float
    currency: str

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

async def fetch_source(client: httpx.AsyncClient, source: dict) -> list[Article]:
    articles: list[Article] = []
    try:
        r = await client.get(source["url"], timeout=15, follow_redirects=True)
        soup = BeautifulSoup(r.text, "html.parser")
        seen: set[str] = set()
        from urllib.parse import urlparse
        base = urlparse(source["url"])
        for tag in soup.find_all("a", href=True):
            title = tag.get_text(strip=True)
            href  = tag["href"]
            if len(title) < 25 or title in seen:
                continue
            seen.add(title)
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = f"{base.scheme}://{base.netloc}{href}"
            else:
                continue
            snippet = ""
            if tag.parent:
                snippet = tag.parent.get_text(separator=" ", strip=True)[:300]
            h = hashlib.md5(title.encode()).hexdigest()
            articles.append(Article(
                title=title, url=full_url,
                source_name=source["name"], cat=source["cat"],
                snippet=snippet, content_hash=h,
            ))
            if len(articles) >= 8:
                break
    except Exception as e:
        log.warning(f"Fetch failed [{source['url']}]: {e}")
    return articles


async def scrape_all(prev_hashes: set[str]) -> list[Article]:
    async with httpx.AsyncClient(headers={"User-Agent": "EnergyIntelBot/1.0"}) as client:
        results = await asyncio.gather(*[fetch_source(client, s) for s in SOURCES])
    all_articles = [a for batch in results for a in batch]
    new_articles  = [a for a in all_articles if a.content_hash not in prev_hashes]
    log.info(f"Scraped {len(all_articles)} total, {len(new_articles)} new")
    return new_articles

# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------

async def fetch_ticker(client: httpx.AsyncClient, t: dict) -> Optional[TickerData]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t['symbol']}?interval=1d&range=2d"
    try:
        r    = await client.get(url, timeout=10)
        meta = r.json()["chart"]["result"][0]["meta"]
        price = meta.get("regularMarketPrice", 0)
        prev  = meta.get("previousClose") or meta.get("chartPreviousClose", price)
        chg   = ((price - prev) / prev * 100) if prev else 0
        return TickerData(
            symbol=t["symbol"], name_he=t["name_he"], name_en=t["name_en"],
            price=round(price, 4), change_pct=round(chg, 2), currency=t["currency"],
        )
    except Exception as e:
        log.warning(f"Ticker failed [{t['symbol']}]: {e}")
        return None


async def fetch_all_tickers() -> tuple[list[TickerData], list[TickerData]]:
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        rt, rm = await asyncio.gather(
            asyncio.gather(*[fetch_ticker(client, t) for t in TICKERS]),
            asyncio.gather(*[fetch_ticker(client, t) for t in MARKET_EXTRAS]),
        )
    return [t for t in rt if t], [t for t in rm if t]

# ---------------------------------------------------------------------------
# Claude summarizer
# ---------------------------------------------------------------------------

CAT_LABELS = {
    "israel_gas":      ("ישראל — גז טבעי",  "Israel — Natural Gas"),
    "israel_electric": ("ישראל — חשמל",      "Israel — Electricity"),
    "international":   ("בינלאומי",           "International"),
    "analysis":        ("ניתוח וסקירות",      "Analysis & Research"),
    "media":           ("מדיה עסקית",         "Business Media"),
}

CAT_DOT = {
    "israel_gas":      "#378ADD",
    "israel_electric": "#7F77DD",
    "international":   "#1D9E75",
    "analysis":        "#BA7517",
    "media":           "#BA7517",
}

BADGE_COLORS = {
    "חדש": ("#E1F5EE","#085041"), "עדכון": ("#FAEEDA","#633806"),
    "רגולציה": ("#EEEDFE","#3C3489"), "ניתוח": ("#E6F1FB","#0C447C"), "פיננסי": ("#E6F1FB","#0C447C"),
    "New": ("#E1F5EE","#085041"), "Update": ("#FAEEDA","#633806"),
    "Regulatory": ("#EEEDFE","#3C3489"), "Analysis": ("#E6F1FB","#0C447C"), "Financial": ("#E6F1FB","#0C447C"),
}


def build_prompt(articles: list[Article], lang: str, slot_label: str, prev_hashes: set[str]) -> str:
    grouped: dict[str, list[Article]] = {}
    for a in articles:
        grouped.setdefault(a.cat, []).append(a)
    lines: list[str] = []
    for cat, arts in grouped.items():
        he, en = CAT_LABELS.get(cat, (cat, cat))
        lines.append(f"\n## {he if lang=='he' else en}")
        for a in arts[:6]:
            status = "NEW" if a.content_hash not in prev_hashes else "UPDATE"
            lines.append(f"[{status}] {a.title}\nURL: {a.url}\nSource: {a.source_name}\nSnippet: {a.snippet}\n")
    body = "\n".join(lines)
    if lang == "he":
        return f"""אתה אנליסט אנרגיה בכיר. סכם את הפרסומים הבאים בפורמט JSON בלבד.

{{"executive_summary": "3-4 משפטים","items": [{{"category": "israel_gas|israel_electric|international|analysis|media","badge": "חדש|עדכון|רגולציה|ניתוח|פיננסי","company": "שם","title": "כותרת","summary": "2-3 משפטים","delta": "מה השתנה","url": "כתובת","source_name": "מקור"}}]}}

פרסומים ({slot_label}):\n{body}\n\nJSON בלבד."""
    else:
        return f"""You are a senior energy analyst. Summarize in valid JSON only.

{{"executive_summary": "3-4 sentences","items": [{{"category": "israel_gas|israel_electric|international|analysis|media","badge": "New|Update|Regulatory|Analysis|Financial","company": "name","title": "headline","summary": "2-3 sentences","delta": "what changed","url": "url","source_name": "source"}}]}}

Publications ({slot_label}):\n{body}\n\nJSON only."""


def summarize(articles: list[Article], lang: str, slot_label: str, prev_hashes: set[str]) -> dict:
    client   = Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt   = build_prompt(articles, lang, slot_label, prev_hashes)
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip().replace("```json","").replace("```","").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        log.error("JSON parse error")
        return {"executive_summary": "שגיאה" if lang=="he" else "Error", "items": []}

# ---------------------------------------------------------------------------
# HTML email builder
# ---------------------------------------------------------------------------

def fmt_price(t: TickerData) -> str:
    if t.currency == "ILS":
        return f"&#8362; {t.price:,.2f}" if t.price >= 1 else f"&#8362; {t.price:.4f}"
    return f"$ {t.price:,.2f}"


def ticker_cell(t: TickerData, lang: str) -> str:
    up    = t.change_pct >= 0
    color = "#0F6E56" if up else "#993C1D"
    arrow = "&#9650;" if up else "&#9660;"
    name  = t.name_he if lang == "he" else t.name_en
    return f"""<td style="padding:4px"><div style="background:#fff;border:0.5px solid #ddd;border-radius:7px;padding:8px 10px;min-width:88px">
        <div style="font-size:10px;color:#666;margin-bottom:2px">{name}</div>
        <div style="font-size:12px;font-weight:bold;color:#1a1a1a">{fmt_price(t)}</div>
        <div style="font-size:10px;font-weight:bold;color:{color}">{arrow} {abs(t.change_pct):.2f}%</div>
    </div></td>"""


def build_email(summary: dict, tickers: list[TickerData], extras: list[TickerData],
                lang: str, slot_label: str, now: datetime) -> str:
    rtl      = "direction:rtl;" if lang == "he" else ""
    he       = lang == "he"
    date_str = now.strftime("%d.%m.%Y") if he else now.strftime("%b %d, %Y")
    time_str = now.strftime("%H:%M")
    n        = len(summary.get("items", []))
    n_new    = len([i for i in summary.get("items",[]) if i.get("badge") in ("חדש","New")])
    n_ent    = len(set(i.get("company","") for i in summary.get("items",[])))

    sources_line = ('מאיה · נתג"ז · רשות החשמל · נגה · BDO · איגוד הגז · גלובס · כלכליסט · Reuters · S&P Global · MEES · שברון · SEC'
                    if he else "MAGNA · Natgas Auth · Electricity Auth · NOGA · BDO · Gas Assoc · Globes · Reuters · S&P Global · MEES · Chevron · SEC")

    disc = ("סיכום זה נוצר באמצעות בינה מלאכותית. המידע למטרות מודיעין עסקי בלבד ואינו ייעוץ פיננסי. יש לאמת מול המקור המקורי."
            if he else "AI-generated from public sources. For business intelligence only. Not financial advice. Verify before decisions.")

    grouped: dict[str, list] = {}
    for item in summary.get("items", []):
        grouped.setdefault(item.get("category","media"), []).append(item)

    sections = ""
    auto_m = "margin-right:auto" if he else "margin-left:auto"
    for cat, items in grouped.items():
        he_lbl, en_lbl = CAT_LABELS.get(cat, (cat, cat))
        dot  = CAT_DOT.get(cat, "#888")
        lbl  = he_lbl if he else en_lbl
        cnt  = f"{len(items)} פרסומים" if he else f"{len(items)} items"
        rows = ""
        for item in items:
            bg, fg = BADGE_COLORS.get(item.get("badge",""), ("#eee","#333"))
            delta_lbl = "מה השתנה" if he else "What changed"
            src_lbl   = "מקור" if he else "Source"
            rows += f"""<div style="margin-bottom:12px;padding-bottom:12px;border-bottom:0.5px solid #eee">
              <div style="display:flex;align-items:flex-start;gap:6px;margin-bottom:4px;{rtl}">
                <span style="font-size:9px;padding:2px 7px;border-radius:7px;font-weight:bold;background:{bg};color:{fg};flex-shrink:0;margin-top:2px">{item.get('badge','')}</span>
                <strong style="font-size:12px;line-height:1.4"><a href="{item.get('url','#')}" style="color:#1a1a1a;text-decoration:none;border-bottom:1px solid #bbb">{item.get('title','')}</a></strong>
                <span style="font-size:10px;color:#888;{auto_m}">{item.get('company','')}</span>
              </div>
              <p style="font-size:11px;color:#444;line-height:1.6;margin:0 0 6px">{item.get('summary','')}</p>
              <div style="display:flex;gap:5px;background:#f5f4f0;border-radius:7px;padding:6px 9px;margin-bottom:6px">
                <div style="width:3px;background:#5DCAA5;border-radius:2px;flex-shrink:0"></div>
                <span style="font-size:10px;color:#444;line-height:1.5"><strong style="color:#1a1a1a">{delta_lbl}:</strong> {item.get('delta','')}</span>
              </div>
              <a href="{item.get('url','#')}" style="font-size:10px;color:#185FA5;text-decoration:none">{src_lbl}: {item.get('source_name','')} &#8599;</a>
            </div>"""
        sections += f"""<div style="margin:0 24px 14px;{rtl}">
          <div style="display:flex;align-items:center;gap:7px;border-bottom:0.5px solid #ddd;padding-bottom:7px;margin-bottom:10px">
            <span style="width:7px;height:7px;border-radius:50%;background:{dot};display:inline-block;flex-shrink:0"></span>
            <strong style="font-size:12px;color:#1a1a1a">{lbl}</strong>
            <span style="font-size:10px;color:#888;{auto_m}">{cnt}</span>
          </div>{rows}</div>
        <div style="height:0.5px;background:#eee;margin:0 24px 14px"></div>"""

    ticker_rows = ""
    cells = ""
    for i, t in enumerate(tickers):
        cells += ticker_cell(t, lang)
        if (i+1) % 3 == 0:
            ticker_rows += f"<tr>{cells}</tr>"
            cells = ""
    if cells:
        ticker_rows += f"<tr>{cells}</tr>"
    extra_row = "<tr>" + "".join(ticker_cell(e, lang) for e in extras) + "</tr>"

    title_str  = f"{n} עדכונים — {slot_label}" if he else f"{n} updates — {slot_label}"
    exec_label = "תמצית מנהלים" if he else "Executive Summary"
    mkt_label  = "שערי שוק" if he else "Market data"
    disc_label = "גילוי נאות — AI" if he else "AI Disclosure"
    cta_txt    = "לפלטפורמה המלאה &#8599;" if he else "Full platform &#8599;"
    footer_txt = f"אוטומטי · {slot_label} {time_str} · 7 ימים · ישראל, מצרים, ירדן, שברון" if he else f"Automated · {slot_label} {time_str} · 7 days · Israel, Egypt, Jordan, Chevron"

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:20px;background:#f0efe9;font-family:Arial,Helvetica,sans-serif;font-size:13px">
<div style="max-width:640px;margin:0 auto;background:#fff;border:0.5px solid #ccc;border-radius:12px;overflow:hidden;color:#1a1a1a;{rtl}">
  <div style="background:#042C53;padding:22px 28px 16px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span style="font-size:11px;color:#B5D4F4;font-weight:500">{'סיכום אנרגיה וגז · הנהלה' if he else 'Energy & Gas Intelligence · Executive'}</span>
      <span style="font-size:10px;background:rgba(255,255,255,.12);color:#85B7EB;padding:3px 9px;border-radius:9px">{slot_label} · {time_str} · {date_str}</span>
    </div>
    <div style="font-size:18px;font-weight:bold;color:#fff;margin-bottom:3px">{title_str}</div>
    <div style="font-size:10px;color:#378ADD;margin-top:5px">{sources_line}</div>
  </div>
  <div style="margin:16px 24px;background:#EBF4FC;border:0.5px solid #85B7EB;border-radius:8px;padding:12px 14px">
    <div style="font-size:10px;font-weight:bold;color:#185FA5;letter-spacing:.5px;margin-bottom:5px;text-transform:uppercase">{exec_label}</div>
    <div style="font-size:12px;color:#0C447C;line-height:1.65">{summary.get('executive_summary','')}</div>
  </div>
  <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:0 24px 16px">
    <div style="background:#f5f4f0;border-radius:7px;padding:9px 10px;text-align:center"><div style="font-size:20px;font-weight:bold">{n}</div><div style="font-size:10px;color:#666">{'פרסומים' if he else 'Publications'}</div></div>
    <div style="background:#f5f4f0;border-radius:7px;padding:9px 10px;text-align:center"><div style="font-size:20px;font-weight:bold">{n_new}</div><div style="font-size:10px;color:#666">{'חדשים' if he else 'New'}</div></div>
    <div style="background:#f5f4f0;border-radius:7px;padding:9px 10px;text-align:center"><div style="font-size:20px;font-weight:bold">{n_ent}</div><div style="font-size:10px;color:#666">{'גופים' if he else 'Entities'}</div></div>
  </div>
  {sections}
  <a href="#" style="display:block;margin:0 24px 16px;background:#042C53;color:#fff;text-align:center;padding:10px;border-radius:8px;font-size:12px;font-weight:bold;text-decoration:none">{cta_txt}</a>
  <div style="background:#f5f4f0;border-top:0.5px solid #ddd;padding:12px 24px">
    <div style="font-size:10px;color:#888;margin-bottom:8px;font-weight:bold">{mkt_label} · {time_str}</div>
    <table style="width:100%;border-collapse:collapse">{ticker_rows}{extra_row}</table>
  </div>
  <div style="background:#fff3cd;border:0.5px solid #ffc107;border-radius:7px;margin:10px 24px;padding:10px 12px">
    <div style="font-size:9px;font-weight:bold;color:#856404;letter-spacing:.4px;margin-bottom:3px;text-transform:uppercase">{disc_label}</div>
    <div style="font-size:10px;color:#5a4500;line-height:1.55">{disc}</div>
  </div>
  <div style="background:#f5f4f0;border-top:0.5px solid #ddd;padding:12px 24px;text-align:center">
    <div style="font-size:10px;color:#888">{footer_txt}</div>
  </div>
</div></body></html>"""

# ---------------------------------------------------------------------------
# State (GitHub Actions — uses file in repo or temp)
# ---------------------------------------------------------------------------

STATE_FILE = "/tmp/energy_agent_state.json"

def load_state() -> set[str]:
    try:
        with open(STATE_FILE) as f:
            return set(json.load(f).get("hashes", []))
    except FileNotFoundError:
        return set()

def save_state(hashes: set[str]) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump({"hashes": list(hashes)[-5000:], "updated": datetime.now().isoformat()}, f)

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run_slot(label_he: str, label_en: str) -> None:
    now = datetime.now(IL_TZ)
    log.info(f"=== {label_he} / {label_en} at {now.strftime('%H:%M')} ===")

    prev_hashes     = load_state()
    articles        = await scrape_all(prev_hashes)
    tickers, extras = await fetch_all_tickers()

    if not articles:
        log.info("Nothing new — skipping.")
        return

    log.info("Summarizing Hebrew…")
    summary_he = summarize(articles, "he", label_he, prev_hashes)
    log.info("Summarizing English…")
    summary_en = summarize(articles, "en", label_en, prev_hashes)

    html_he = build_email(summary_he, tickers, extras, "he", label_he, now)
    html_en = build_email(summary_en, tickers, extras, "en", label_en, now)

    date_str = now.strftime("%d.%m.%Y")

    # Send Gmail
    send_email_gmail(f"סיכום אנרגיה וגז | {label_he} | {date_str}", html_he, RECIPIENTS_HE)
    send_email_gmail(f"Energy & Gas Intelligence | {label_en} | {now.strftime('%b %d, %Y')}", html_en, RECIPIENTS_EN)

    # Send Telegram (Hebrew)
    await send_telegram(summary_he, tickers, extras, label_he, "he")

    save_state(prev_hashes | {a.content_hash for a in articles})
    log.info("=== Done ===")


async def main() -> None:
    now = datetime.now(IL_TZ)
    for h, m, lhe, len_ in SEND_SLOTS:
        if now.hour == h and abs(now.minute - m) <= 4:
            await run_slot(lhe, len_)
            return
    log.info(f"No slot at {now.strftime('%H:%M')} — idle.")


if __name__ == "__main__":
    asyncio.run(main())
