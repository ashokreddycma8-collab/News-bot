"""
NSE MARKET PULSE — 100% FREE VERSION
══════════════════════════════════════
LLM  : Groq (free) — Llama 3.3 70B  →  console.groq.com
News : NewsAPI + Marketaux + Zerodha Pulse
Alert: Telegram — sector focus + stock-level reasoning
 
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
from apscheduler.schedulers.blocking import BlockingScheduler
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
GROQ_MODEL   = "llama-3.3-70b-versatile"
SEEN_FILE    = "seen_hashes.txt"
BIAS_FILE    = "daily_sector_bias.json"
MIN_ARTICLES = 2
IST          = ZoneInfo("Asia/Kolkata")
 
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
#  DEDUP — reads/writes seen_hashes.txt
#  In GitHub Actions: file is committed back
#  to repo after each run (see nse_pulse.yml)
# ══════════════════════════════════════════
def load_seen() -> set:
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE) as f:
        lines = [l.strip() for l in f.read().splitlines() if l.strip()]
    # Keep only last 500 hashes to prevent file bloat
    if len(lines) > 500:
        lines = lines[-500:]
        with open(SEEN_FILE, "w") as f:
            f.write("\n".join(lines) + "\n")
    return set(lines)
 
def mark_seen(hashes: list):
    with open(SEEN_FILE, "a") as f:
        f.write("\n".join(hashes) + "\n")
 
 
# ══════════════════════════════════════════
#  NEWS SOURCE 1 — NewsAPI
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
 
 
# ══════════════════════════════════════════
#  NEWS SOURCE 2 — Marketaux (optional)
# ══════════════════════════════════════════
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
 
 
# ══════════════════════════════════════════
#  NEWS SOURCE 3 — Zerodha Pulse (scrape)
# ══════════════════════════════════════════
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
#  GROQ PROMPT — analyst-grade stock reasoning
# ══════════════════════════════════════════
PROMPT = """You are a senior NSE equity desk analyst. Write sharp, specific alerts like a real analyst — not a generic chatbot.
 
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
          "reason": "ONE sharp sentence following this format exactly: [news trigger] -> [business mechanism] -> [stock impact on margins/revenue/sentiment]. BAD example: impacted by global uncertainty. GOOD example: Iran conflict blocks spare parts imports via Gulf -> Ashok Leyland sources 30% components from Middle East -> supply disruption hits production margins Q1"
        }}
      ],
      "watch": "name something specific — a price level, data release, event time. Not generic advice like monitor the sector"
    }}
  ],
  "top_themes": ["theme1", "theme2"],
  "confidence": 75
}}
 
Strict rules:
- conviction: 1=mild, 2=strong directional, 3=shock event (policy/earnings surprise/crisis)
- stocks: 2-3 most directly impacted NSE-listed stocks per sector with SPECIFIC reasons
- reason field MUST follow [trigger] -> [mechanism] -> [impact] format
- Generic reasons are not acceptable — be specific to the company's business
- Only include sectors with actual news backing — no padding
- Max 4 sectors, max 3 stocks per sector
- confidence = 0-100 based on how clearly news supports the call
 
Headlines:
{headlines}"""
 
 
# ══════════════════════════════════════════
#  GROQ CALL
# ══════════════════════════════════════════
def analyze_with_groq(headlines: list) -> dict | None:
    if not headlines:
        return None
 
    groq_client = Groq(api_key=GROQ_KEY)
    text = "\n".join(f"• {h}" for h in headlines[:20])
 
    try:
        resp = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": PROMPT.format(headlines=text)}],
            temperature=0.1,
            max_tokens=1500,
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
 
 
# ══════════════════════════════════════════
#  BUILD TELEGRAM ALERT
# ══════════════════════════════════════════
def build_alert(analysis: dict, fresh_count: int, total_count: int) -> str:
    mood         = analysis.get("market_mood", "Mixed")
    mood_emoji   = {"Bullish": "🟢", "Cautious": "🟡", "Mixed": "🟡", "Bearish": "🔴"}.get(mood, "⚪")
    dir_emoji    = {"Bullish": "🟢", "Bearish": "🔴", "Neutral": "🟡"}
    impact_arrow = {"Bullish": "▲", "Bearish": "▼"}
    conv_bar     = {1: "▮▯▯", 2: "▮▮▯", 3: "▮▮▮"}
    now_str      = datetime.now(IST).strftime("%d %b  %H:%M IST")
 
    lines = [
        f"📡 NSE MARKET PULSE  —  {now_str}",
        "━" * 34,
        f"{mood_emoji} Mood: {mood}",
        f"   {analysis.get('mood_reason', '')}",
    ]
 
    gf = analysis.get("global_factors")
    if gf and gf != "null":
        lines.append(f"🌐 {gf}")
 
    sectors = analysis.get("sectors", [])
    if sectors:
        lines.append("\n📊 SECTOR FOCUS")
        for s in sectors:
            de    = dir_emoji.get(s.get("direction", ""), "🟡")
            bar   = conv_bar.get(s.get("conviction", 1), "▮▯▯")
            name  = s.get("sector", "").upper()
            theme = f"  #{s['theme'].replace(' ', '_')}" if s.get("theme") and s.get("theme") != "null" else ""
 
            lines.append(f"\n{de} {name}  {bar}{theme}")
            lines.append(f"   📰 {s.get('news_driver', '')}")
 
            for st in s.get("stocks", []):
                arrow  = impact_arrow.get(st.get("impact", ""), "→")
                ticker = st.get("ticker", "")
                sname  = st.get("name", "")
                reason = st.get("reason", "")
                lines.append(f"   {arrow} {sname} ({ticker})")
                lines.append(f"      {reason}")
 
            lines.append(f"   👁 {s.get('watch', '')}")
 
    themes = analysis.get("top_themes", [])
    if themes:
        lines.append("\n🧠 " + "  ".join(f"#{t}" for t in themes if t and t != "null"))
 
    lines += [
        f"\n📦 {fresh_count} fresh / {total_count} scanned",
        "━" * 34,
        "▮▯▯ mild  ▮▮▯ strong  ▮▮▮ high conviction"
    ]
 
    return "\n".join(lines)
 
 
# ══════════════════════════════════════════
#  SEND TELEGRAM
# ══════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
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
#  SAVE BIAS FILE (for Excel link later)
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
 
 
# ══════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════
def run():
    print(f"\n{'='*46}")
    print(f"  NSE PULSE  |  {datetime.now(IST).strftime('%d %b %Y  %H:%M IST')}")
    print(f"{'='*46}")
 
    all_articles = fetch_newsapi() + fetch_marketaux() + fetch_zerodha_pulse()
    print(f"  Fetched : {len(all_articles)} total")
 
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
        print("  ⏭ Not enough fresh news — skipping (no duplicate alert)\n")
        return
 
    headlines = [a["title"] for a in fresh]
    print(f"  Sending {len(headlines)} headlines to Groq ({GROQ_MODEL})...")
    analysis = analyze_with_groq(headlines)
 
    if not analysis:
        print("  Analysis failed — skipping\n")
        return
 
    mood    = analysis.get("market_mood", "?")
    sectors = len(analysis.get("sectors", []))
    conf    = analysis.get("confidence", 0)
    print(f"  Result  : {mood} | {sectors} sectors | confidence {conf}%")
 
    if sectors == 0:
        print("  No sector conviction — skipping\n")
        return
 
    alert = build_alert(analysis, len(fresh), len(all_articles))
    print("\n" + alert + "\n")
    send_telegram(alert)
    save_bias(analysis)
    mark_seen(hashes)
    print(f"  Marked {len(hashes)} as seen\n")
 
 
# ══════════════════════════════════════════
#  SCHEDULER
# ══════════════════════════════════════════
if __name__ == "__main__":
    import sys
 
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        print("=== TEST MODE ===")
        run()
    else:
        print("NSE Market Pulse — Free Edition")
        print(f"Model : {GROQ_MODEL} via Groq (free)")
        print("Runs  : Every 15 min | Mon-Fri 9:00-15:45 IST")
        print("Stop  : Ctrl+C\n")
        run()
        scheduler = BlockingScheduler(timezone="Asia/Kolkata")
        scheduler.add_job(
            lambda: run() if is_market_hours() else print(f"  [{datetime.now(IST).strftime('%H:%M')}] market closed"),
            "cron",
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/15"
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            print("\nStopped.")
