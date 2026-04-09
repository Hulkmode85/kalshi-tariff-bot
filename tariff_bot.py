"""
Kalshi Tariff/Trade Policy Bot
Trade Kalshi markets around tariff announcements, trade deal news, and trade war escalation.
Free data sources: RSS feeds (Reuters, AP, Bloomberg, WSJ trade sections) + keyword detection.
"""

import asyncio
import os
from flask import Flask, jsonify
import threading
import json
import time
import logging
import base64
import re
import uuid
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, ec
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Multi-strike: scan ALL strikes per event/series, not just one ────────────
MULTI_STRIKE = os.getenv("MULTI_STRIKE", "true").lower() == "true"
# When fetching markets, iterate through ALL contracts in each series/event
# and evaluate each strike independently. No single-ticker filtering.

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tariff_bot")

from risk_guard import RiskManager
risk_manager = RiskManager()

# ── Shadow Logging ────────────────────────────────────────────────────────────
SHADOW_LOG_FILE = os.getenv("SHADOW_LOG_FILE", "shadow_log.jsonl")

def shadow_log(opportunity: dict, taken: bool, reason: str = ""):
    entry = {"ts": time.time(), "taken": taken, "reason": reason, **opportunity}
    try:
        with open(SHADOW_LOG_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except:
        pass

# ── CONFIG ────────────────────────────────────────────────────────────────────
KALSHI_BASE       = os.getenv("KALSHI_BASE", "https://api.elections.kalshi.com")
KALSHI_API_URL    = os.getenv("KALSHI_API_URL", f"{KALSHI_BASE}/trade-api/v2")
KALSHI_API_KEY    = os.getenv("KALSHI_API_KEY", "")
KALSHI_KEY_ID     = os.getenv("KALSHI_KEY_ID", "")
PAPER_MODE        = os.getenv("PAPER_MODE", "true").lower() == "true"
PAPER_BALANCE     = float(os.getenv("PAPER_BALANCE", "5000"))
BET_SIZE_USD      = float(os.getenv("BET_SIZE_USD", "15"))
MAX_BET_USD       = float(os.getenv("MAX_BET_USD", "40"))
KELLY_FRACTION    = float(os.getenv("KELLY_FRACTION", "1.0"))
MIN_EDGE          = float(os.getenv("MIN_EDGE", "0.06"))
MAKER_FEE         = float(os.getenv("MAKER_FEE", "0.0175"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "600"))   # 10 min
LOOKBACK_HOURS    = int(os.getenv("LOOKBACK_HOURS", "6"))

# ── RSS FEEDS (trade/tariff focused) ─────────────────────────────────────────
RSS_FEEDS = [
    "https://feeds.apnews.com/rss/APNewsTopHeadlines",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://rss.politico.com/congress.xml",
    "https://rss.politico.com/economy.xml",
    "https://feeds.npr.org/1006/rss.xml",   # NPR economy
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",  # CNBC economy
]

# ── SIGNAL KEYWORDS ───────────────────────────────────────────────────────────
# Maps keyword group → (signal_type, direction, confidence_base)
TARIFF_SIGNALS = {
    # Escalation signals → economy worse, markets price lower growth
    "tariff_hike": {
        "keywords": ["tariff increase", "tariff hike", "raises tariffs", "new tariffs", "tariffs on",
                     "25% tariff", "50% tariff", "100% tariff", "tariff war", "trade war escalat"],
        "signal": "tariff_escalation",
        "confidence": 0.68,
    },
    "tariff_threat": {
        "keywords": ["threatens tariff", "tariff threat", "tariff warning", "will impose tariff",
                     "considering tariff", "tariff retaliation", "retaliatory tariff"],
        "signal": "tariff_threat",
        "confidence": 0.62,
    },
    # De-escalation signals → economy better, markets price higher growth
    "tariff_cut": {
        "keywords": ["tariff cut", "reduces tariffs", "removes tariffs", "tariff exemption",
                     "tariff relief", "tariff pause", "tariff delay", "trade truce",
                     "trade deal signed", "trade agreement", "trade pact"],
        "signal": "tariff_relief",
        "confidence": 0.70,
    },
    "trade_deal": {
        "keywords": ["trade deal", "free trade agreement", "FTA signed", "trade negotiations breakthrough",
                     "trade talks progress", "trade framework", "bilateral trade", "trade normalization"],
        "signal": "trade_positive",
        "confidence": 0.65,
    },
    # Country-specific signals
    "china_tariff": {
        "keywords": ["china tariff", "chinese goods tariff", "tariffs on china", "section 301",
                     "us-china trade", "china trade war", "decoupling"],
        "signal": "china_trade",
        "confidence": 0.67,
    },
    "mexico_canada": {
        "keywords": ["usmca", "nafta", "mexico tariff", "canada tariff", "steel tariff",
                     "aluminum tariff", "section 232"],
        "signal": "north_america_trade",
        "confidence": 0.63,
    },
}

# ── KALSHI MARKET SERIES FOR TRADE/TARIFF TOPICS ─────────────────────────────
# Series that may be affected by tariff/trade news
TRADE_SERIES = [
    "KXSPY",    # S&P 500 price markets
    "KXSPYX",   # S&P 500 extended
    "KXQQQ",    # QQQ / tech index
    "KXNASDAQ", # Nasdaq
    "KXDOW",    # Dow Jones
    "KXINFL",   # Inflation markets
    "KXCPI",    # CPI markets (tariffs → inflation)
    "KXCPIM",
    "KXGDP",    # GDP markets (tariffs → growth)
    "KXRECESSION", # Recession probability
    "KXFED",    # Fed rate (tariff-driven inflation → rate hikes)
    "KXFEDRATE",
    "KXTRADE",  # Direct trade policy markets
    "KXTARIFF", # Direct tariff markets
    "KXCHINA",  # China-related markets
]

# ── AUTH ──────────────────────────────────────────────────────────────────────
def _sign_request(method: str, path: str, ts: int, body: str = "") -> str:
    if not KALSHI_API_KEY:
        return ""
    try:
        pem_str = os.getenv("KALSHI_PRIVATE_KEY", "")
        if "\\n" in pem_str:
            pem_str = pem_str.replace("\\n", "\n")
        private_key = serialization.load_pem_private_key(pem_str.encode(), password=None)
        msg = f"{ts}{method.upper()}{path}{body}".encode()
        if isinstance(private_key, ec.EllipticCurvePrivateKey):
            sig = private_key.sign(msg, ec.ECDSA(hashes.SHA256()))
        else:
            sig = private_key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32), hashes.SHA256())
        return base64.b64encode(sig).decode()
    except Exception:
        return ""

def _auth_headers(method: str, path: str, body: str = "") -> dict:
    ts = int(time.time() * 1000)
    sig = _sign_request(method, path, ts, body)
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": str(ts),
        "KALSHI-ACCESS-SIGNATURE": sig,
    }

# ── PAPER LEDGER ──────────────────────────────────────────────────────────────
@dataclass
class PaperLedger:
    balance: float = PAPER_BALANCE
    trades: list = field(default_factory=list)
    wins: int = 0
    losses: int = 0

    def record(self, market: str, side: str, contracts: int, price_cents: int, signal: str):
        cost = contracts * price_cents / 100
        self.balance -= cost
        self.trades.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "market": market, "side": side, "contracts": contracts,
            "price_cents": price_cents, "cost": cost, "signal": signal,
        })
        log.info(f"[PAPER] {side} {contracts}ct @ {price_cents}¢ on {market} | {signal} | balance=${self.balance:.2f}")

# ── RSS FETCHING ──────────────────────────────────────────────────────────────
@dataclass
class NewsItem:
    title: str
    summary: str
    published: datetime
    url: str
    signal_type: Optional[str] = None
    confidence: float = 0.0
    matched_keywords: list = field(default_factory=list)

async def fetch_rss(client: httpx.AsyncClient, url: str) -> list[NewsItem]:
    try:
        r = await client.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        # Simple XML parse without feedparser
        items = []
        content = r.text
        # Extract <item> blocks
        item_blocks = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        for block in item_blocks[:20]:
            title = re.search(r'<title[^>]*>(.*?)</title>', block, re.DOTALL)
            desc  = re.search(r'<description[^>]*>(.*?)</description>', block, re.DOTALL)
            pub   = re.search(r'<pubDate>(.*?)</pubDate>', block, re.DOTALL)
            link  = re.search(r'<link>(.*?)</link>', block, re.DOTALL)

            title_str = re.sub(r'<[^>]+>', '', title.group(1) if title else '').strip()
            desc_str  = re.sub(r'<[^>]+>', '', desc.group(1)  if desc  else '').strip()[:300]
            link_str  = link.group(1).strip() if link else ''

            # Parse date
            pub_dt = datetime.now(timezone.utc)
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(pub.group(1).strip())
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            if pub_dt < cutoff:
                continue

            if title_str:
                items.append(NewsItem(title=title_str, summary=desc_str,
                                      published=pub_dt, url=link_str))
        return items
    except Exception as e:
        log.debug(f"RSS fetch error {url}: {e}")
        return []

def analyze_news_item(item: NewsItem) -> Optional[NewsItem]:
    """Check if news item matches any tariff/trade signal keywords."""
    text = (item.title + " " + item.summary).lower()
    best_signal = None
    best_conf = 0.0
    best_keywords = []

    for group_name, group in TARIFF_SIGNALS.items():
        matched = [kw for kw in group["keywords"] if kw.lower() in text]
        if matched:
            conf = group["confidence"]
            # Boost confidence if multiple keywords match
            conf = min(conf + 0.02 * (len(matched) - 1), 0.85)
            if conf > best_conf:
                best_conf = conf
                best_signal = group["signal"]
                best_keywords = matched

    if best_signal:
        item.signal_type = best_signal
        item.confidence = best_conf
        item.matched_keywords = best_keywords
        return item
    return None

# ── KALSHI MARKET FETCHING ────────────────────────────────────────────────────
async def get_kalshi_markets(client: httpx.AsyncClient, series_ticker: str) -> list:
    path = f"/markets?series_ticker={series_ticker}&status=open&limit=20"
    headers = _auth_headers("GET", path) if KALSHI_KEY_ID else {"Content-Type": "application/json"}
    try:
        r = await client.get(f"{KALSHI_API_URL}{path}", headers=headers, timeout=10)
        if r.status_code == 200:
            return r.json().get("markets", [])
        return []
    except Exception:
        return []

def find_best_trade(markets: list, signal: NewsItem) -> Optional[dict]:
    """
    Map trade/tariff signal to Kalshi market trade.

    Tariff escalation → economy worse → lower growth/inflation → prices drop:
      - S&P/QQQ/Nasdaq: bearish → buy NO on "above X" or YES on "below X"
      - CPI/Inflation: bullish (tariffs = inflation) → buy YES on "above X" CPI
      - Recession: bullish → buy YES on recession markets
      - Fed rate: bullish (tariffs force rate hikes) → buy YES on "above X" rate

    Tariff relief / trade deal → economy better → growth up, stable inflation:
      - S&P/QQQ/Nasdaq: bullish → buy YES on "above X"
      - Recession: bearish → buy NO on recession markets
    """
    is_negative = signal.signal_type in ("tariff_escalation", "tariff_threat", "china_trade")
    is_positive = signal.signal_type in ("tariff_relief", "trade_positive")
    is_inflation = signal.signal_type in ("tariff_escalation", "china_trade")

    best = None
    best_edge = 0.0

    for m in markets:
        title  = m.get("title", "").lower()
        ticker = m.get("ticker", "")
        yes_ask = m.get("yes_ask", 0)
        no_ask  = m.get("no_ask", 0)
        series  = m.get("series_ticker", "")

        if not yes_ask or not no_ask:
            continue

        # Check time remaining
        close_ts = m.get("close_time") or m.get("expiration_time") or ""
        if close_ts:
            try:
                close_dt = datetime.fromisoformat(close_ts.replace("Z", "+00:00"))
                remaining = (close_dt - datetime.now(timezone.utc)).total_seconds()
                if remaining < 3600:
                    continue
            except Exception:
                pass

        is_above = any(w in title for w in ["above", "over", "exceed", "higher", "reach"])
        is_below = any(w in title for w in ["below", "under", "fall", "drop", "less"])
        is_recession = any(w in title for w in ["recession", "contraction", "gdp negative"])

        conf = signal.confidence
        side = None
        price = None

        if series in ("KXRECESSION",) or is_recession:
            if is_negative:
                side, price = "yes", yes_ask
                true_prob = min(conf * 0.8, 0.80)
            elif is_positive:
                side, price = "no", no_ask
                true_prob = min(1 - conf * 0.6, 0.80)
            else:
                continue

        elif series in ("KXCPI", "KXCPIM", "KXINFL") or any(w in title for w in ["cpi", "inflation", "price index"]):
            if is_inflation and is_above:
                side, price = "yes", yes_ask
                true_prob = min(conf * 0.85, 0.82)
            elif is_positive and is_below:
                side, price = "yes", yes_ask
                true_prob = min(conf * 0.75, 0.78)
            else:
                continue

        elif series in ("KXSPY", "KXSPYX", "KXQQQ", "KXNASDAQ", "KXDOW"):
            if is_negative:
                if is_below:
                    side, price = "yes", yes_ask
                    true_prob = min(conf * 0.80, 0.80)
                elif is_above:
                    side, price = "no", no_ask
                    true_prob = min((1 - conf) + 0.15, 0.78)
                else:
                    continue
            elif is_positive:
                if is_above:
                    side, price = "yes", yes_ask
                    true_prob = min(conf * 0.80, 0.80)
                elif is_below:
                    side, price = "no", no_ask
                    true_prob = min((1 - conf) + 0.15, 0.78)
                else:
                    continue
            else:
                continue

        elif series in ("KXFED", "KXFEDRATE"):
            if is_inflation and is_above:
                side, price = "yes", yes_ask
                true_prob = min(conf * 0.75, 0.78)
            else:
                continue

        elif series in ("KXTRADE", "KXTARIFF", "KXCHINA"):
            # Direct trade policy markets — follow signal directly
            if is_negative and is_above:
                side, price = "no", no_ask  # tariff goes higher? maybe but uncertain
                true_prob = min(conf * 0.7, 0.75)
            elif is_positive and is_above:
                side, price = "yes", yes_ask
                true_prob = min(conf * 0.75, 0.78)
            else:
                continue
        else:
            continue

        if side and price:
            edge = true_prob - price / 100
            ev_after_fees = edge - MAKER_FEE
            if ev_after_fees <= 0:
                continue
            if edge > best_edge and edge >= MIN_EDGE:
                best_edge = edge
                best = {
                    "market": m, "side": side, "price": price, "edge": edge,
                    "note": f"{signal.signal_type} [{','.join(signal.matched_keywords[:2])}] → {side.upper()} | '{m.get('title','')[:60]}'"
                }

    return best

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
async def place_order(client: httpx.AsyncClient, ticker: str, side: str,
                      price_cents: int, contracts: int, ledger: PaperLedger,
                      note: str) -> bool:
    if PAPER_MODE:
        ledger.record(ticker, side, contracts, price_cents, note)
        return True
    body_obj = {
        "ticker": ticker, "action": "buy", "side": side, "type": "limit", "count": contracts,
        "yes_price" if side == "yes" else "no_price": price_cents,
        "client_order_id": str(uuid.uuid4()),
    }
    body_str = json.dumps(body_obj)
    path = "/portfolio/orders"
    headers = _auth_headers("POST", path, body_str)
    try:
        r = await client.post(f"{KALSHI_API_URL}{path}", headers=headers,
                              content=body_str, timeout=10)
        if r.status_code in (200, 201):
            log.info(f"[ORDER] {side} {contracts}ct @ {price_cents}¢ on {ticker}")
            return True
        log.warning(f"[ORDER] Failed {ticker}: {r.status_code} {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"[ORDER] Exception: {e}")
        return False

# ── DEDUP ─────────────────────────────────────────────────────────────────────
class SeenTracker:
    def __init__(self, max_age_hours: int = 24):
        self.max_age = max_age_hours * 3600
        self._seen: dict[str, float] = {}

    def is_new(self, key: str) -> bool:
        now = time.time()
        # Evict old entries
        self._seen = {k: v for k, v in self._seen.items() if now - v < self.max_age}
        return key not in self._seen

    def mark(self, key: str):
        self._seen[key] = time.time()

# ── COOLDOWN ──────────────────────────────────────────────────────────────────
class CooldownTracker:
    def __init__(self, minutes: int = 120):
        self.minutes = minutes
        self._last: dict[str, datetime] = {}

    def can_trade(self, key: str) -> bool:
        if key not in self._last:
            return True
        elapsed = (datetime.now(timezone.utc) - self._last[key]).total_seconds()
        return elapsed > self.minutes * 60

    def mark(self, key: str):
        self._last[key] = datetime.now(timezone.utc)

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
# ── Stats HTTP server ─────────────────────────────────────────────────────────
_stats_app = Flask(__name__)
_bot_stats = {"trades": 0, "wins": 0, "pnl": 0.0, "balance": 0.0, "start": time.time()}

@_stats_app.route("/stats")
def _stats_endpoint():
    t = _bot_stats
    total = t["trades"]
    return jsonify({"bot": "kalshi-tariff-bot", "paper_mode": True,
        "balance": t["balance"], "trades": total, "wins": t["wins"],
        "losses": total - t["wins"], "win_rate": round(t["wins"]/max(total,1), 4),
        "pnl": t["pnl"], "uptime_hours": round((time.time()-t["start"])/3600, 2)})

@_stats_app.route("/health")
def _health_endpoint():
    return jsonify({"status": "ok"})

def _run_stats_server():
    _stats_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


async def main():
    log.info(f"=== Kalshi Tariff/Trade Bot starting (paper={PAPER_MODE}) ===")
    log.info(f"Monitoring {len(RSS_FEEDS)} RSS feeds, {len(TRADE_SERIES)} Kalshi series")
    log.info(f"MIN_EDGE={MIN_EDGE*100:.0f}%, BET_SIZE=${BET_SIZE_USD}, poll={POLL_INTERVAL_SEC}s")

    paper    = PaperLedger()
    _bot_stats['balance'] = paper.balance
    threading.Thread(target=_run_stats_server, daemon=True).start()
    seen     = SeenTracker(max_age_hours=24)
    cooldown = CooldownTracker(minutes=120)
    trades   = 0

    async with httpx.AsyncClient() as client:
        while True:
            _bot_stats["balance"] = paper.balance
            _bot_stats["trades"] = len(paper.trades)
            _bot_stats["wins"] = paper.wins
            _bot_stats["losses"] = paper.losses
            log.info(f"--- Scan | balance=${paper.balance:.2f} | trades={trades} ---")

            # 1. Fetch all RSS feeds
            all_items: list[NewsItem] = []
            for feed_url in RSS_FEEDS:
                items = await fetch_rss(client, feed_url)
                all_items.extend(items)
                await asyncio.sleep(0.5)

            log.info(f"Fetched {len(all_items)} items from RSS feeds")

            # 2. Analyze for trade/tariff signals
            signals: list[NewsItem] = []
            for item in all_items:
                key = item.title[:80]
                if not seen.is_new(key):
                    continue
                seen.mark(key)
                analyzed = analyze_news_item(item)
                if analyzed:
                    signals.append(analyzed)
                    log.info(f"[SIGNAL] {analyzed.signal_type} conf={analyzed.confidence:.2f} | {analyzed.title[:80]}")

            log.info(f"Found {len(signals)} trade/tariff signals")

            # 3. For each signal, find and execute trade
            for signal in signals:
                cd_key = signal.signal_type
                if not cooldown.can_trade(cd_key):
                    log.info(f"Cooldown active for {cd_key}, skipping")
                    continue

                # Fetch relevant markets
                all_markets = []
                for series in TRADE_SERIES:
                    markets = await get_kalshi_markets(client, series)
                    all_markets.extend(markets)
                    await asyncio.sleep(0.3)

                if not all_markets:
                    log.info("No open Kalshi markets found")
                    continue

                trade = find_best_trade(all_markets, signal)
                if not trade:
                    log.info(f"No edge found for {signal.signal_type} in {len(all_markets)} markets")
                    shadow_log({"bot": "tariff", "signal_type": signal.signal_type}, taken=False, reason="no edge found")
                    continue

                price     = trade["price"]
                # Kelly criterion sizing
                market_prob = price / 100
                model_prob = min(0.95, market_prob + trade["edge"])
                kelly_f = max(0, (model_prob - market_prob) / (1 - market_prob)) if market_prob < 1 else 0
                kelly_bet = max(1, min(ledger.balance * kelly_f * KELLY_FRACTION, MAX_BET_USD))
                contracts = max(1, int(kelly_bet * 100 / price))
                mkt_ticker = trade["market"].get("ticker", "?")

                log.info(f"[TRADE] {mkt_ticker} | {trade['side'].upper()} {contracts}ct @ {price}¢ | "
                         f"edge={trade['edge']*100:.1f}% | {trade['note'][:80]}")

                # ── Risk Guard check ──
                if not PAPER_MODE:
                    allowed, reason, capped = risk_manager.pre_trade_check(mkt_ticker, price, contracts, trade["side"], bot_name="tariff-bot")
                    if not allowed:
                        log.warning(f"Risk guard blocked: {reason}")
                        continue
                    contracts = capped
                else:
                    allowed, reason, capped = risk_manager.pre_trade_check(mkt_ticker, price, contracts, trade["side"], bot_name="tariff-bot")
                    if not allowed:
                        log.info(f"[PAPER] Risk guard would block: {reason}")

                success = await place_order(client, mkt_ticker, trade["side"],
                                            price, contracts, paper, trade["note"])
                if success:
                    shadow_log({"bot": "tariff", "ticker": mkt_ticker, "side": trade["side"], "price": price, "edge": trade["edge"], "signal_type": signal.signal_type}, taken=True)
                    cooldown.mark(cd_key)
                    trades += 1

                await asyncio.sleep(1.0)

            log.info(f"--- Scan complete | sleeping {POLL_INTERVAL_SEC}s ---")
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
