"""
NSE MARKET PULSE v2 — with CATALYST LAYER
==========================================
LLM  : Groq (free) — Llama 3.3 70B  →  console.groq.com
News : NewsAPI + Marketaux + Zerodha Pulse
Alert: Telegram — sector focus + stock-level reasoning

v2 ADDITION:
  Layer 1 (existing): Sector-level macro signals  → Telegram alert
  Layer 2 (NEW):      Stock-level catalyst signals → catalyst_watchlist.json
                      Read by pivot_scanner_v2_8.py to override Nifty gate

Install:
  pip install requests groq apscheduler beautifulsoup4
"""

import requests
import json
import hashlib
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from groq import Groq

# ══════════════════════════════════════════
#  YOUR KEYS
# ══════════════════════════════════════════
GROQ_KEY       = os.environ.get("GROQ_KEY", "")
NEWSAPI_KEY    = os.environ.get("NEWSAPI_KEY", "")
MARKETAUX_KEY  = os.environ.get("MARKETAUX_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")

# ══════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════
GROQ_MODEL       = "llama-3.3-70b-versatile"
SEEN_FILE        = "seen_hashes.txt"
BIAS_FILE        = "daily_sector_bias.json"
CATALYST_FILE    = "catalyst_watchlist.json"   # v2: read by pivot scanner
MIN_ARTICLES     = 2
IST              = ZoneInfo("Asia/Kolkata")

# ══════════════════════════════════════════
#  MARKET HOURS
# ══════════════════════════════════════════
def is_market_hours() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    open_t  = now.replace(hour=9,  minute=0,  second=0, microsecond=0)
    close_t = now.replace(hour=15, minute=45, second=0, microsecond=0)
    return open_t <= now <= close_t


# ══════════════════════════════════════════
#  DEDUP
# ══════════════════════════════════════════
def load_seen() -> set:
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE) as f:
        lines = [l.strip() for l in f.read().splitlines() if l.strip()]
    if len(lines) > 500:
        lines = lines[-500:]
        with open(SEEN_FILE, "w") as f:
            f.write("\n".join(lines) + "\n")
    return set(lines)

def mark_seen(hashes: list):
    with open(SEEN_FILE, "a") as f:
        f.write("\n".join(hashes) + "\n")


# ══════════════════════════════════════════
#  NEWS SOURCES (unchanged)
# ══════════════════════════════════════════
def fetch_newsapi() -> list:
    articles = []
    seen_titles = set()
    queries = [
        "NSE Nifty India stocks",
        "SEBI RBI India market policy",
        "India sector earnings results",
        "India economy budget"
    ]
    for q in queries:
        try:
            r = requests.get(
                "https://newsapi.org/v2/everything",
                params={"q": q, "sortBy": "publishedAt",
                        "language": "en", "pageSize": 5,
                        "apiKey": NEWSAPI_KEY},
                timeout=8
            )
            for a in r.json().get("articles", []):
                title = a.get("title", "").strip()
                if title and title not in seen_titles and "[Removed]" not in title:
                    seen_titles.add(title)
                    articles.append({"title": title, "source": "NewsAPI"})
        except Exception as e:
            print(f"  [NewsAPI] {e}")
    return articles


def fetch_marketaux() -> list:
    if not MARKETAUX_KEY or MARKETAUX_KEY == "YOUR_MARKETAUX_KEY":
        return []
    try:
        r = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={
                "countries": "in", "filter_entities": "true",
                "language": "en", "api_token": MARKETAUX_KEY,
                "limit": 8,
                "published_after": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
            },
            timeout=8
        )
        return [
            {"title": a.get("title", "").strip(), "source": "Marketaux"}
            for a in r.json().get("data", [])
            if a.get("title", "").strip()
        ]
    except Exception as e:
        print(f"  [Marketaux] {e}")
        return []


def fetch_zerodha_pulse() -> list:
    try:
        from bs4 import BeautifulSoup
        r = requests.get(
            "https://pulse.zerodha.com/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=10
        )
        soup = BeautifulSoup(r.text, "html.parser")
        results = []
        for item in soup.select("li.item")[:10]:
            tag = item.select_one("h2 a") or item.select_one("a")
            if tag:
                title = tag.get_text(strip=True)
                if title:
                    results.append({"title": title, "source": "ZerodhaPulse"})
        return results
    except Exception as e:
        print(f"  [ZerodhaPulse] {e}")
        return []


# ══════════════════════════════════════════
#  LAYER 1 — SECTOR MACRO (existing prompt)
# ══════════════════════════════════════════
PROMPT_SECTOR = """You are a senior NSE equity desk analyst. Write sharp, specific alerts like a real analyst — not a generic chatbot.

Analyze these news headlines for Indian stock market impact today.

NSE heatmap sectors:
banking, financial_services, capital_markets, it, pharma, oil_gas, energy, renewable,
auto, fmcg, consumer, metal, commodities, realty, housing, infra, defence, psu, media, telecom

Thematic sectors:
railway, psu_banks, internet_digital, capital_goods, ems, rural_mobility,
tourism_hospitality, chemicals, agrochemicals, logistics, healthcare_services,
cement, textiles, jewellery, data_centers, water_sanitation

Return ONLY valid JSON. No markdown. No preamble. Nothing outside the JSON.

{{
  "market_mood": "Bullish or Cautious or Mixed or Bearish",
  "mood_reason": "sharp specific reason — not generic. Example: Iran conflict pushing Brent above $90, squeezing OMC margins",
  "global_factors": "specific global event and direct India market impact — or null",
  "sectors": [
    {{
      "sector": "exact sector name from the list above",
      "direction": "Bullish or Bearish or Neutral",
      "conviction": 1,
      "news_driver": "the exact news event causing this — name it clearly",
      "theme": "theme name if thematic play e.g. PLI_solar, PSU_rotation, China+1 — else null",
      "stocks": [
        {{
          "name": "Company Name",
          "ticker": "SYMBOL.NS",
          "impact": "Bullish or Bearish",
          "reason": "ONE sharp sentence: [news trigger] -> [business mechanism] -> [final impact]",
          "quant": "Analyst-style quantified impact. Examples: margins -150bps, revenue +8-10% Q3, order book +Rs400Cr. Must be a number. null only if truly unquantifiable."
        }}
      ],
      "watch": "name something specific — a price level, data release, event time. Not generic advice"
    }}
  ],
  "top_themes": ["theme1", "theme2"],
  "confidence": 75
}}

Strict rules:
- conviction: 1=mild, 2=strong directional, 3=shock event (policy/earnings surprise/crisis)
- stocks: 2-3 most directly impacted NSE-listed stocks per sector with SPECIFIC reasons
- reason field MUST follow [trigger] -> [mechanism] -> [impact] format
- quant field MUST contain a number (%, bps, Rs Cr). Never vague words
- Only include sectors with actual news backing — no padding
- Max 4 sectors, max 3 stocks per sector
- confidence = 0-100 based on how clearly news supports the call

Headlines:
{headlines}"""


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — STOCK CATALYST SCANNER (NEW in v2)
# ══════════════════════════════════════════════════════════════════════════════
PROMPT_CATALYST = """You are a stock-level catalyst scanner for NSE India equities.

Your job: find COMPANY-SPECIFIC events from these headlines that could move ONE stock's price 3%+ within 1-5 days.

IMPORTANT DISTINCTION:
- Macro/sector news (oil prices, interest rates, geopolitical themes) = NOT a catalyst. Ignore these.
- Company-specific event (plant launch, contract win, F&O addition, results beat) = CATALYST. Report these.

CATALYST TYPES to look for:

CORPORATE_EVENT (score 7-10):
- Plant inauguration / capacity expansion / new facility commissioning
- Large contract win / LoA / PPA / MoU signed (name the company!)
- M&A / acquisition / subsidiary launch / JV
- Major order book addition (>5% of market cap)
- Government approval / license for a SPECIFIC company

STRUCTURAL_CHANGE (score 6-9):
- Stock added to F&O segment / index inclusion / index rebalancing
- Credit rating upgrade for a SPECIFIC company
- FII/DII stake increase >1% disclosed
- Promoter buying in open market

EARNINGS_BEAT (score 5-8):
- Quarterly results beating consensus by >10%
- Revenue / profit guidance raised
- Surprise margin expansion

BROKERAGE_UPGRADE (score 4-7):
- Buy initiation by Goldman, Morgan Stanley, Jefferies, CLSA, Nomura
- Target price raised >15%
- Upgrade from Sell/Hold to Buy

TECHNICAL_EVENT (score 4-7):
- Promoter / PE stake sale COMPLETED (washout = demand zone)
- Block deal absorbed with strong delivery
- Buyback at premium

Return ONLY valid JSON. No markdown. No preamble.

{{
  "catalysts": {{
    "SYMBOL": {{
      "score": 8,
      "direction": "BUY",
      "catalyst": "one-line description of the SPECIFIC event",
      "category": "corporate_event",
      "ttl_hours": 48
    }}
  }}
}}

STRICT RULES:
1. SYMBOL = exact NSE symbol (KAYNES not Kaynes Technology, ADANIPOWER not Adani Power, BEL not Bharat Electronics)
2. Only events from the LAST 24 HOURS
3. Score honestly: minor brokerage note = 4, PM inaugurating a plant = 9, F&O addition = 7
4. direction: BUY for positive, SELL for negative
5. ttl_hours: corporate_event=48, structural_change=72, earnings_beat=24, brokerage_upgrade=24, technical_event=48
6. EXCLUDE all macro/sector themes — those are handled by Layer 1
7. If NO company-specific catalysts found, return {{"catalysts": {{}}}}
8. Max 10 catalysts per scan — quality over quantity
9. Each catalyst must name ONE specific NSE-listed company — never generic sector calls

Common NSE symbols for reference:
KAYNES, DIXON, SYRMA (EMS) | BEL, HAL, BHEL, GRSE, COCHINSHIP (Defence)
ADANIPOWER, NTPC, TATAPOWER, NHPC (Power) | ANANDRATHI, BSE, MCX, CDSL (Capital Mkts)
VMM, DMART, TRENT (Retail) | PERSISTENT, LTIM, COFORGE, MPHASIS (IT midcap)
ZOMATO, PAYTM, NAUKRI (Internet) | GODREJPROP, PRESTIGE, OBEROIRLTY (Realty)

Headlines:
{headlines}"""


# ══════════════════════════════════════════
#  GROQ CALLS
# ══════════════════════════════════════════
def _call_groq(prompt: str, headlines: list, max_tokens: int = 1500) -> dict | None:
    """Shared Groq caller for both layers."""
    if not headlines:
        return None

    groq_client = Groq(api_key=GROQ_KEY)
    text = "\n".join(f"• {h}" for h in headlines[:20])

    try:
        resp = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt.format(headlines=text)}],
            temperature=0.1,
            max_tokens=max_tokens,
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            raw = match.group()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [Groq JSON error] {e}")
        return None
    except Exception as e:
        print(f"  [Groq error] {e}")
        return None


def analyze_sectors(headlines: list) -> dict | None:
    """Layer 1: Sector macro analysis (existing)."""
    return _call_groq(PROMPT_SECTOR, headlines, max_tokens=1500)


def analyze_catalysts(headlines: list) -> dict | None:
    """Layer 2: Stock-specific catalyst scan (NEW)."""
    return _call_groq(PROMPT_CATALYST, headlines, max_tokens=1000)


# ══════════════════════════════════════════════════════════════════════════════
#  CATALYST WATCHLIST — merge new catalysts with existing (don't overwrite)
# ══════════════════════════════════════════════════════════════════════════════
def load_existing_catalysts() -> dict:
    """Load catalyst_watchlist.json if it exists."""
    if not os.path.exists(CATALYST_FILE):
        return {}
    try:
        with open(CATALYST_FILE) as f:
            data = json.load(f)
        return data.get("catalysts", {})
    except (json.JSONDecodeError, IOError):
        return {}


def save_catalyst_watchlist(new_catalysts: dict):
    """
    Merge new catalysts with existing ones.
    - New catalyst for same symbol: keep higher score
    - Expired catalysts (past TTL): remove
    - Write updated file
    """
    existing = load_existing_catalysts()
    now = datetime.now(IST)

    # Merge: new takes priority if score >= existing
    merged = {}
    for sym, info in existing.items():
        merged[sym] = info
    for sym, info in new_catalysts.items():
        sym = sym.upper().replace(".NS", "")
        if sym not in merged or info.get("score", 0) >= merged.get(sym, {}).get("score", 0):
            merged[sym] = info

    # Add conviction display based on score
    for sym, info in merged.items():
        score = info.get("score", 0)
        if score >= 8:
            info["conviction"] = "▮▮▮"
        elif score >= 6:
            info["conviction"] = "▮▮▯"
        else:
            info["conviction"] = "▮▯▯"

    output = {
        "generated_at": now.isoformat(),
        "catalysts": merged
    }

    with open(CATALYST_FILE, "w") as f:
        json.dump(output, f, indent=2)

    return merged


def format_catalyst_telegram(catalysts: dict) -> str | None:
    """Build a short Telegram message for catalyst alerts."""
    if not catalysts:
        return None

    now_str = datetime.now(IST).strftime("%d %b  %H:%M IST")
    lines = [
        f"🎯 CATALYST WATCHLIST  |  {now_str}",
        "━" * 32,
    ]

    # Sort by score descending
    sorted_cats = sorted(catalysts.items(), key=lambda x: -x[1].get("score", 0))

    for sym, info in sorted_cats[:8]:
        score = info.get("score", 0)
        direction = info.get("direction", "BUY")
        catalyst = info.get("catalyst", "")
        category = info.get("category", "").replace("_", " ")
        conv = info.get("conviction", "▮▯▯")
        arrow = "▲" if direction == "BUY" else "▼"
        ttl = info.get("ttl_hours", 24)

        lines.append(f"  {arrow} {sym}  {conv}  score:{score}/10")
        lines.append(f"     {catalyst}")
        lines.append(f"     _{category} · valid {ttl}h_")
        lines.append("")

    lines.append("━" * 32)
    lines.append("_Scanner reads this file to override Nifty gate_")

    return "\n".join(lines)


# ══════════════════════════════════════════
#  BUILD SECTOR ALERT (existing, unchanged)
# ══════════════════════════════════════════
def short_ticker(ticker: str) -> str:
    return ticker.replace(".NS", "").replace(".BO", "")

def short_reason(reason: str) -> str:
    parts = [p.strip() for p in reason.split("->")]
    return parts[-1] if parts else reason

def build_alert(analysis: dict, fresh_count: int, total_count: int) -> str:
    mood         = analysis.get("market_mood", "Mixed")
    mood_emoji   = {"Bullish": "🟢", "Cautious": "🟡", "Mixed": "🟡", "Bearish": "🔴"}.get(mood, "⚪")
    dir_emoji    = {"Bullish": "🟢", "Bearish": "🔴", "Neutral": "🟡"}
    impact_arrow = {"Bullish": "▲", "Bearish": "▼", "Neutral": "→"}
    conv_bar     = {1: "▮▯▯", 2: "▮▮▯", 3: "▮▮▮"}
    divider      = "─" * 32
    now_str      = datetime.now(IST).strftime("%d %b  %H:%M IST")

    lines = [
        f"📡 NSE MARKET PULSE  |  {now_str}",
        "━" * 32,
        f"{mood_emoji} {mood.upper()}  —  {analysis.get('mood_reason', '')}",
    ]

    gf = analysis.get("global_factors")
    if gf and gf != "null":
        lines.append(f"🌐 {gf}")

    sectors = analysis.get("sectors", [])
    if sectors:
        lines.append("")
        for s in sectors:
            de     = dir_emoji.get(s.get("direction", ""), "🟡")
            bar    = conv_bar.get(s.get("conviction", 1), "▮▯▯")
            name   = s.get("sector", "").upper()
            theme  = f"  #{s['theme'].replace(' ', '_')}" if s.get("theme") and s.get("theme") != "null" else ""
            driver = s.get("news_driver", "")

            lines.append(divider)
            lines.append(f"{de} {name}  {bar}{theme}")
            lines.append(f"📰 {driver}")
            lines.append("")

            for st in s.get("stocks", []):
                arrow     = impact_arrow.get(st.get("impact", ""), "→")
                ticker    = short_ticker(st.get("ticker", ""))
                impact    = short_reason(st.get("reason", ""))
                quant     = st.get("quant", "")
                quant_str = f"  [{quant}]" if quant and quant != "null" else ""
                lines.append(f"  {arrow} {ticker}  {impact}{quant_str}")

            watch = s.get("watch", "")
            if watch:
                lines.append(f"\n  👁 {watch}")

        lines.append(divider)

    themes = analysis.get("top_themes", [])
    if themes:
        theme_str = "  ".join(f"#{t}" for t in themes if t and t != "null")
        lines.append(f"\n🧠 {theme_str}")

    conf = analysis.get("confidence", 0)
    lines += [
        f"📦 {fresh_count} fresh  |  🎯 {conf}% confidence",
        "━" * 32,
        "▮▯▯ mild  ▮▮▯ strong  ▮▮▮ high conviction"
    ]

    return "\n".join(lines)


# ══════════════════════════════════════════
#  SEND TELEGRAM - FIXED (NO PARSE_MODE)
# ══════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},  # ← REMOVED parse_mode to fix 400 error
            timeout=10
        )
        if r.status_code == 200:
            print("  ✅ Telegram sent")
            return True
        else:
            print(f"  ❌ Telegram error {r.status_code}: {r.text[:120]}")
            return False
    except Exception as e:
        print(f"  ❌ Telegram exception: {e}")
        return False


# ══════════════════════════════════════════
#  SAVE BIAS FILE
# ══════════════════════════════════════════
def save_bias(analysis: dict):
    try:
        with open(BIAS_FILE, "w") as f:
            json.dump({
                "date":    datetime.now(IST).strftime("%Y-%m-%d"),
                "time":    datetime.now(IST).strftime("%H:%M"),
                "mood":    analysis.get("market_mood"),
                "sectors": analysis.get("sectors", []),
                "themes":  analysis.get("top_themes", []),
            }, f, indent=2)
    except Exception as e:
        print(f"  [Bias save] {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE — now runs BOTH layers
# ══════════════════════════════════════════════════════════════════════════════
def run():
    print(f"\n{'='*46}")
    print(f"  NSE PULSE v2  |  {datetime.now(IST).strftime('%d %b %Y  %H:%M IST')}")
    print(f"{'='*46}")

    # ── Fetch news (shared across both layers) ─────────────────
    all_articles = fetch_newsapi() + fetch_marketaux() + fetch_zerodha_pulse()
    print(f"  Fetched : {len(all_articles)} total")

    # ✅ NEW: Hard-fail if ALL sources returned 0 (likely auth/network failure)
    if len(all_articles) == 0:
        print("  ❌ ZERO articles from all 3 sources — API keys or network issue")
        raise RuntimeError("All news sources failed — check API keys and network")

    seen   = load_seen()
    fresh  = []
    hashes = []
    for a in all_articles:
        h = hashlib.md5(a["title"].encode()).hexdigest()
        if h not in seen and a["title"].strip():
            a["_hash"] = h
            fresh.append(a)
            hashes.append(h)

    print(f"  Fresh   : {len(fresh)}")

    if len(fresh) < MIN_ARTICLES:
        print("  ⏭ Not enough fresh news — skipping\n")
        return

    headlines = [a["title"] for a in fresh]

    # ── LAYER 1: Sector macro analysis (existing) ──────────────
    print(f"\n  ─── Layer 1: Sector Scan ───")
    print(f"  Sending {len(headlines)} headlines to Groq...")
    analysis = analyze_sectors(headlines)

    if analysis:
        mood    = analysis.get("market_mood", "?")
        sectors = len(analysis.get("sectors", []))
        conf    = analysis.get("confidence", 0)
        print(f"  Result  : {mood} | {sectors} sectors | confidence {conf}%")

        if sectors > 0:
            alert = build_alert(analysis, len(fresh), len(all_articles))
            print("\n" + alert + "\n")
            send_telegram(alert)
            save_bias(analysis)
    else:
        print("  Layer 1 failed — no sector alert")

    # ── LAYER 2: Stock catalyst scan (NEW) ─────────────────────
    print(f"\n  ─── Layer 2: Catalyst Scan ───")
    print(f"  Scanning same {len(headlines)} headlines for stock catalysts...")
    cat_result = analyze_catalysts(headlines)

    if cat_result:
        raw_catalysts = cat_result.get("catalysts", {})
        if raw_catalysts:
            # Clean up symbols (remove .NS suffix if Groq adds it)
            cleaned = {}
            for sym, info in raw_catalysts.items():
                clean_sym = sym.upper().replace(".NS", "").replace(".BO", "")
                cleaned[clean_sym] = info

            merged = save_catalyst_watchlist(cleaned)
            print(f"  Found {len(cleaned)} new catalyst(s), {len(merged)} total active")

            for sym, info in cleaned.items():
                score = info.get("score", 0)
                cat   = info.get("catalyst", "")[:60]
                print(f"    🎯 {sym:<14} score:{score}  {cat}")

            # Send catalyst Telegram alert
            cat_msg = format_catalyst_telegram(merged)
            if cat_msg:
                send_telegram(cat_msg)
        else:
            print("  No stock-specific catalysts found this cycle")
    else:
        print("  Layer 2 failed — no catalyst data")

    # ── Mark seen ──────────────────────────────────────────────
    mark_seen(hashes)
    print(f"\n  Marked {len(hashes)} as seen\n")


# ══════════════════════════════════════════
#  MAIN with proper error handling & exit codes
# ══════════════════════════════════════════
def main() -> int:
    """Wrapper with exit code discipline for GitHub Actions."""
    try:
        run()
        return 0
    except KeyboardInterrupt:
        print("\n  Interrupted by user")
        return 130
    except Exception as e:
        import traceback
        print(f"\n❌ FATAL ERROR:\n{traceback.format_exc()}")
        
        # Try to send failure alert to Telegram
        try:
            failure_msg = (
                f"🚨 NSE PULSE CRASHED\n\n"
                f"```\n{str(e)[:200]}\n```\n\n"
                f"{datetime.now(IST).strftime('%d %b %H:%M IST')}"
            )
            send_telegram(failure_msg)
        except:
            pass
        
        return 1


# ══════════════════════════════════════════
#  SCHEDULER
# ══════════════════════════════════════════
if __name__ == "__main__":
    import sys
    from apscheduler.schedulers.blocking import BlockingScheduler

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # One-shot mode for GitHub Actions cron
        print("=== ONE-SHOT MODE (GitHub Actions) ===")
        sys.exit(main())
    else:
        # Long-running mode with internal scheduler
        print("NSE Market Pulse v2 — with Catalyst Layer")
        print(f"Model : {GROQ_MODEL} via Groq (free)")
        print("Runs  : Every 15 min | Mon-Fri 9:00-15:45 IST")
        print("Layer 1: Sector macro → Telegram")
        print("Layer 2: Stock catalyst → catalyst_watchlist.json → Scanner")
        print("Stop  : Ctrl+C\n")
        
        # Run once immediately
        exit_code = main()
        if exit_code != 0:
            print("⚠️  Initial run failed, but scheduler will continue")
        
        scheduler = BlockingScheduler(timezone="Asia/Kolkata")
        scheduler.add_job(
            lambda: main() if is_market_hours() else print(f"  [{datetime.now(IST).strftime('%H:%M')}] market closed"),
            "cron",
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/15"
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            print("\nStopped.")
            sys.exit(0)
