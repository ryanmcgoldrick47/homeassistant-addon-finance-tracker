from __future__ import annotations

"""
Market Pulse — RBA rate + history, key indices, CPI, Fear/Greed, ticker news,
AU finance news, AI briefing.
"""

import os
import re
from datetime import datetime, date
from email.utils import parsedate_to_datetime

import httpx
from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from database import get_session, ShareHolding, User
from deps import get_current_user, get_setting, set_setting

router = APIRouter(prefix="/api/market-pulse", tags=["market-pulse"])

_INDICES = [
    {"symbol": "^AXJO",    "name": "ASX 200",  "currency": "AUD"},
    {"symbol": "^GSPC",    "name": "S&P 500",  "currency": "USD"},
    {"symbol": "AUDUSD=X", "name": "AUD/USD",  "currency": ""},
    {"symbol": "GC=F",     "name": "Gold",     "currency": "USD/oz"},
]

_YF_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; FinanceTracker/1.0)"}

_AU_NEWS_FEEDS = [
    ("SMH Business",    "https://www.smh.com.au/rss/business.xml"),
    ("ABC News",        "https://www.abc.net.au/news/feed/51120/rss.xml"),
    ("RBA News",        "https://www.rba.gov.au/rss/rss-cb-speeches.xml"),
]

_MONTHS = ["January","February","March","April","May","June",
           "July","August","September","October","November","December"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _yahoo_quote(symbol: str) -> dict | None:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2d"
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(url, headers=_YF_HEADERS)
        data = r.json()
        meta = data["chart"]["result"][0]["meta"]
        price = meta.get("regularMarketPrice", 0)
        # Prefer regularMarketChange/Percent — directly provided by Yahoo, more reliable
        change = meta.get("regularMarketChange")
        change_pct = meta.get("regularMarketChangePercent")
        if change is None or change_pct is None:
            prev = meta.get("previousClose") or meta.get("chartPreviousClose") or price
            change = round(price - prev, 4)
            change_pct = round(change / prev * 100, 2) if prev else 0
        else:
            change = round(change, 4)
            change_pct = round(change_pct, 2)
        return {"price": price, "change": change, "change_pct": change_pct}
    except Exception:
        return None


def _parse_rba_history(html: str) -> list[dict]:
    """Parse the RBA cash rate history table from the statistics page."""
    months_pat = "|".join(_MONTHS)
    history: list[dict] = []

    # Strategy: find <tr> blocks that contain a date like "4 February 2025"
    # and a rate like "4.10" nearby
    row_re = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    date_re = re.compile(
        r"\b(\d{1,2})\s+(" + months_pat + r")\s+(\d{4})\b", re.IGNORECASE
    )
    rate_re = re.compile(r"\b(\d{1,2}\.\d{2})\b")

    for m in row_re.finditer(html):
        row = m.group(1)
        dm = date_re.search(row)
        if not dm:
            continue
        try:
            dt = datetime.strptime(
                f"{dm.group(1)} {dm.group(2)} {dm.group(3)}", "%d %B %Y"
            ).date()
        except ValueError:
            continue

        # Find all numeric values in the row that look like a cash rate (0–20%)
        rates = [float(r) for r in rate_re.findall(row) if 0.0 < float(r) < 20.0]
        if not rates:
            continue

        # The rightmost reasonable value is usually the new rate target
        history.append({"date": str(dt), "rate": rates[-1]})

    history.sort(key=lambda x: x["date"], reverse=True)
    return history


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/indices")
async def get_indices(current_user: User = Depends(get_current_user)):
    results = []
    for idx in _INDICES:
        d = await _yahoo_quote(idx["symbol"])
        results.append({
            "symbol":     idx["symbol"],
            "name":       idx["name"],
            "currency":   idx["currency"],
            "price":      d["price"]      if d else None,
            "change":     d["change"]     if d else None,
            "change_pct": d["change_pct"] if d else None,
        })
    return results


@router.get("/rba")
async def get_rba(current_user: User = Depends(get_current_user)):
    result: dict = {
        "cash_rate":        None,
        "next_meeting":     None,
        "days_until":       None,
        "last_change_date": None,
        "last_change_bps":  None,
        "history":          [],
    }

    # --- scrape rate history ---
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://www.rba.gov.au/statistics/cash-rate/",
                headers=_YF_HEADERS,
            )
        history = _parse_rba_history(r.text)

        if history:
            result["cash_rate"] = history[0]["rate"]
            result["history"]   = history[:24]

            # Find last rate change (first row where rate differs from current)
            current_rate = history[0]["rate"]
            for i in range(1, len(history)):
                if history[i]["rate"] != current_rate:
                    bps = round((current_rate - history[i]["rate"]) * 100)
                    # history[0]["date"] is when the current rate started
                    result["last_change_date"] = history[0]["date"]
                    result["last_change_bps"]  = bps
                    break
        else:
            # Fallback: simple scrape for the rate number
            for m in re.findall(r"<td[^>]*>\s*(\d{1,2}\.\d{2})\s*</td>", r.text):
                val = float(m)
                if 0.0 < val < 20.0:
                    result["cash_rate"] = val
                    break
    except Exception:
        pass

    # --- scrape next meeting date ---
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://www.rba.gov.au/monetary-policy/schedule.html",
                headers=_YF_HEADERS,
            )
        today   = date.today()
        pattern = r"\b(\d{1,2})\s+(" + "|".join(_MONTHS) + r")\s+(\d{4})\b"
        for day, month, year in re.findall(pattern, r.text):
            try:
                d = datetime.strptime(f"{day} {month} {year}", "%d %B %Y").date()
                if d >= today:
                    result["next_meeting"] = str(d)
                    result["days_until"]   = (d - today).days
                    break
            except ValueError:
                continue
    except Exception:
        pass

    return result


@router.get("/cpi")
async def get_cpi(current_user: User = Depends(get_current_user)):
    """Latest Australian CPI annual rate from ABS."""
    result: dict = {
        "rate":        None,
        "period":      None,
        "target_low":  2.0,
        "target_high": 3.0,
    }
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            r = await client.get(
                "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation"
                "/consumer-price-index-australia/latest-release",
                headers=_YF_HEADERS,
            )
        html = r.text
        # Look for the headline annual change figure (e.g. "2.4 per cent")
        # ABS pages often have "X.X per cent" in the summary paragraph
        pct_matches = re.findall(
            r"(\d+\.\d+)\s*(?:per cent|%)", html, re.IGNORECASE
        )
        for m in pct_matches:
            val = float(m)
            if 0.0 < val < 20.0:
                result["rate"] = val
                break
        # Try to extract the quarter period
        qtr = re.search(
            r"((?:March|June|September|December)\s+quarter\s+\d{4})",
            html, re.IGNORECASE,
        )
        if qtr:
            result["period"] = qtr.group(1)
    except Exception:
        pass
    return result


@router.get("/fear-greed")
async def get_fear_greed(current_user: User = Depends(get_current_user)):
    """
    CNN Fear & Greed Index as a global market sentiment proxy.
    Falls back to a simple ASX momentum score if CNN is unavailable.
    """
    result: dict = {
        "score":      None,
        "rating":     None,
        "prev_score": None,
        "source":     "CNN Fear & Greed",
    }
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; FinanceTracker/1.0)",
                    "Referer":    "https://www.cnn.com/markets/fear-and-greed",
                },
            )
        data = r.json()
        fg = data.get("fear_and_greed", {})
        score = fg.get("score")
        if score is not None:
            result["score"]      = round(float(score), 1)
            result["rating"]     = fg.get("rating", "")
            prev = fg.get("previous_close")
            if prev is not None:
                result["prev_score"] = round(float(prev), 1)
            return result
    except Exception:
        pass

    # Fallback: compute from ASX 200 daily move
    try:
        d = await _yahoo_quote("^AXJO")
        if d:
            chg = d["change_pct"]
            # Map -3..+3% range to 0..100
            score = min(100, max(0, 50 + chg * 10))
            result["score"]  = round(score, 1)
            result["rating"] = _fg_label(score)
            result["source"] = "ASX Momentum"
    except Exception:
        pass
    return result


def _fg_label(score: float) -> str:
    if score <= 24:  return "Extreme Fear"
    if score <= 44:  return "Fear"
    if score <= 55:  return "Neutral"
    if score <= 75:  return "Greed"
    return "Extreme Greed"


def _rss_field(item_text: str, tag: str) -> str:
    """Extract a field from an RSS <item> block using regex."""
    # Handle both <tag>value</tag> and <tag><![CDATA[value]]></tag>
    m = re.search(
        rf"<{tag}[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{tag}>",
        item_text, re.DOTALL | re.IGNORECASE,
    )
    if m:
        return re.sub(r"<[^>]+>", "", m.group(1)).strip()
    return ""


@router.get("/aus-news")
async def get_aus_news(current_user: User = Depends(get_current_user)):
    """Latest Australian finance news from RSS feeds."""
    articles: list[dict] = []
    for source_name, feed_url in _AU_NEWS_FEEDS:
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                r = await client.get(feed_url, headers=_YF_HEADERS)
            # Split into <item> blocks (works for both RSS 2.0 and Atom-style)
            item_blocks = re.findall(r"<item[^>]*>(.*?)</item>", r.text, re.DOTALL | re.IGNORECASE)
            for block in item_blocks[:5]:
                title = _rss_field(block, "title")
                link  = _rss_field(block, "link") or _rss_field(block, "guid")
                pub   = _rss_field(block, "pubDate") or _rss_field(block, "published")
                desc  = _rss_field(block, "description") or _rss_field(block, "summary")
                desc  = desc[:180]
                ts = 0
                try:
                    ts = int(parsedate_to_datetime(pub).timestamp())
                except Exception:
                    try:
                        ts = int(datetime.fromisoformat(
                            pub.replace("Z", "+00:00")
                        ).timestamp())
                    except Exception:
                        pass
                if title and link:
                    articles.append({
                        "source":       source_name,
                        "title":        title,
                        "url":          link,
                        "desc":         desc,
                        "published_at": ts,
                    })
        except Exception:
            continue
    articles.sort(key=lambda x: x["published_at"], reverse=True)
    return articles[:15]


@router.get("/ticker-news")
async def get_ticker_news(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    holdings = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
    tickers = list({h.ticker for h in holdings})[:6]

    all_news: list[dict] = []
    for ticker in tickers:
        try:
            url = (
                f"https://query2.finance.yahoo.com/v1/finance/search"
                f"?q={ticker}&newsCount=3&enableFuzzyQuery=false"
            )
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(url, headers=_YF_HEADERS)
            for item in r.json().get("news", [])[:3]:
                all_news.append({
                    "ticker":       ticker,
                    "title":        item.get("title", ""),
                    "url":          item.get("link", ""),
                    "source":       item.get("publisher", ""),
                    "published_at": item.get("providerPublishTime", 0),
                })
        except Exception:
            continue

    all_news.sort(key=lambda x: x["published_at"], reverse=True)
    return all_news[:12]


@router.get("/briefing")
async def get_briefing(
    refresh: bool = False,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    # Check 6-hour cache (unless refresh=true)
    if not refresh:
        cached_ts  = get_setting(session, "market_pulse_briefing_ts", "")
        cached_txt = get_setting(session, "market_pulse_briefing", "")
        if cached_ts and cached_txt:
            try:
                if (datetime.now() - datetime.fromisoformat(cached_ts)).total_seconds() < 6 * 3600:
                    return {"briefing": cached_txt, "cached": True, "generated_at": cached_ts}
            except ValueError:
                pass

    # API key: check settings DB first, then environment
    api_key = get_setting(session, "anthropic_api_key", "") or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"briefing": None, "cached": False, "error": "No Anthropic API key configured — add it in Settings."}

    # Gather live data
    idx_lines: list[str] = []
    for idx in _INDICES:
        d = await _yahoo_quote(idx["symbol"])
        if d:
            idx_lines.append(f"  {idx['name']}: {d['price']:.4g} ({d['change_pct']:+.2f}%)")

    rba      = await _fetch_rba_raw()
    rba_line = ""
    if rba.get("cash_rate"):
        rba_line = f"  RBA cash rate: {rba['cash_rate']}%"
        if rba.get("next_meeting"):
            rba_line += f", next meeting {rba['next_meeting']} ({rba['days_until']} days away)"

    holdings    = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
    port_value  = sum(h.value_aud for h in holdings)
    port_gain   = sum(h.gain_aud  for h in holdings)
    tickers_str = ", ".join(h.ticker for h in holdings[:8]) or "none"

    today_str = date.today().strftime("%-d %B %Y")
    prompt = f"""Today is {today_str}. Write a concise 3-sentence market briefing for an Australian investor.

Market data:
{chr(10).join(idx_lines) if idx_lines else "  (unavailable)"}
{rba_line}

User portfolio:
  Holdings: {tickers_str}
  Total value: A${port_value:,.0f}  |  Unrealised gain: A${port_gain:+,.0f}

Write in second person ("Your portfolio…", "The ASX…"). Reference specific numbers. Focus on what's relevant to this person. No bullet points or headings. 3 sentences max."""

    try:
        import anthropic
        client  = anthropic.Anthropic(api_key=api_key)
        msg     = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=220,
            messages=[{"role": "user", "content": prompt}],
        )
        briefing = msg.content[0].text.strip()
        now_str  = datetime.now().isoformat(timespec="seconds")
        set_setting(session, "market_pulse_briefing",    briefing)
        set_setting(session, "market_pulse_briefing_ts", now_str)
        return {"briefing": briefing, "cached": False, "generated_at": now_str}
    except Exception as e:
        return {"briefing": None, "cached": False, "error": str(e)[:120]}


async def _fetch_rba_raw() -> dict:
    """Standalone RBA fetch for use inside briefing."""
    result: dict = {"cash_rate": None, "next_meeting": None, "days_until": None}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://www.rba.gov.au/statistics/cash-rate/", headers=_YF_HEADERS)
        history = _parse_rba_history(r.text)
        if history:
            result["cash_rate"] = history[0]["rate"]
        else:
            for m in re.findall(r"<td[^>]*>\s*(\d{1,2}\.\d{2})\s*</td>", r.text):
                val = float(m)
                if 0.0 < val < 20.0:
                    result["cash_rate"] = val
                    break
    except Exception:
        pass
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://www.rba.gov.au/monetary-policy/schedule.html", headers=_YF_HEADERS)
        today   = date.today()
        pattern = r"\b(\d{1,2})\s+(" + "|".join(_MONTHS) + r")\s+(\d{4})\b"
        for day, month, year in re.findall(pattern, r.text):
            try:
                d = datetime.strptime(f"{day} {month} {year}", "%d %B %Y").date()
                if d >= today:
                    result["next_meeting"] = str(d)
                    result["days_until"]   = (d - today).days
                    break
            except ValueError:
                continue
    except Exception:
        pass
    return result
