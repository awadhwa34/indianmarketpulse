#!/usr/bin/env python3
"""
IndiaMarketPulse — Content Automation Engine
=============================================
Runs every 2 hours via cron. Fetches Indian economic news,
generates stock/MF impact analysis via Claude API, publishes
to the site, and removes stale content (>3 days, no traffic).

Dependencies:
  pip install anthropic requests feedparser python-dotenv psycopg2-binary
  pip install google-analytics-data google-auth

Cron entry (every 2 hours):
  0 */2 * * * /usr/bin/python3 /opt/indiamarketpulse/automation/content_engine.py >> /var/log/imp/engine.log 2>&1
"""

import os
import json
import time
import logging
import hashlib
import datetime
from pathlib import Path
from typing import Optional
import feedparser
import requests
from google import genai
from dotenv import load_dotenv

# ─── CONFIG ───────────────────────────────────────────────────────────────────

load_dotenv()

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "engine.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("IMP-Engine")

GEMINI_API_KEY  = os.environ["GEMINI_API_KEY"]
DATABASE_URL       = os.environ["DATABASE_URL"]          # PostgreSQL connection string
CONTENT_DIR        = Path(os.environ.get("CONTENT_DIR", "/var/www/indiamarketpulse/content"))
GA_PROPERTY_ID     = os.environ.get("GA_PROPERTY_ID", "")  # Google Analytics 4 property ID
STALE_DAYS         = int(os.environ.get("STALE_DAYS", 3))
MAX_ARTICLES_PAGE  = int(os.environ.get("MAX_ARTICLES_PAGE", 30))

# ─── NEWS SOURCES — Indian Economic & Financial Feeds ─────────────────────────

RSS_FEEDS = [
    # Tier-1 Sources
    {"url": "https://economictimes.indiatimes.com/rssfeedstopstories.cms",       "source": "Economic Times",   "tier": 1},
    {"url": "https://www.livemint.com/rss/money",                                "source": "Mint",             "tier": 1},
    {"url": "https://www.business-standard.com/rss/markets-106.rss",             "source": "Business Standard","tier": 1},
    {"url": "https://www.financialexpress.com/market/feed/",                     "source": "Financial Express","tier": 1},
    # RBI / SEBI / Ministry Official
    {"url": "https://www.rbi.org.in/scripts/rss.aspx",                           "source": "RBI",              "tier": 1},
    {"url": "https://www.sebi.gov.in/sebi_data/commondocs/rss/sebirss.xml",      "source": "SEBI",             "tier": 1},
    # Market / NSE / BSE
    {"url": "https://www.nseindia.com/rss.xml",                                   "source": "NSE India",        "tier": 2},
    {"url": "https://www.moneycontrol.com/rss/economy.xml",                      "source": "Moneycontrol",     "tier": 2},
    {"url": "https://www.zeebiz.com/rss.xml",                                    "source": "Zee Business",     "tier": 2},
]

# ─── SECTOR CLASSIFICATION KEYWORDS ──────────────────────────────────────────

SECTOR_KEYWORDS = {
    "Banking & Finance": ["rbi", "bank", "nbfc", "credit", "npa", "repo", "lending", "deposit", "hdfc", "sbi", "icici", "axis", "kotak"],
    "Infrastructure":    ["capex", "infrastructure", "railway", "highway", "metro", "l&t", "nhai", "port", "airport", "power grid"],
    "IT & Technology":   ["tcs", "infosys", "wipro", "hcl", "tech mahindra", "it sector", "software exports", "digital"],
    "FMCG & Retail":    ["fmcg", "hul", "dabur", "itc", "marico", "rural demand", "consumption", "retail", "fmcg inflation"],
    "Energy":            ["oil", "gas", "ongc", "reliance", "bpcl", "power", "renewable", "solar", "ntpc", "crude"],
    "Pharma & Health":  ["pharma", "sun pharma", "cipla", "drreddy", "drug", "fda", "api", "healthcare"],
    "Macro Economy":     ["gdp", "inflation", "cpi", "wpi", "iip", "fiscal deficit", "current account", "rbi policy", "budget"],
    "Metals & Mining":  ["steel", "tata steel", "jsw", "vedanta", "coal", "aluminium", "mining", "jspl"],
    "IPO & Markets":     ["ipo", "listing", "sebi", "qip", "fii", "dii", "fpi", "block deal"],
    "Mutual Funds":      ["mutual fund", "nfo", "sip", "aum", "nav", "equity fund", "debt fund", "elss"],
}

# ─── DATABASE HELPERS ─────────────────────────────────────────────────────────

def get_db_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Create tables if they don't exist."""
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           SERIAL PRIMARY KEY,
            slug         VARCHAR(255) UNIQUE NOT NULL,
            headline     TEXT NOT NULL,
            sector       VARCHAR(100),
            summary      TEXT,
            analysis     TEXT,
            impacts      JSONB,     -- {stocks: [...], mfs: [...]}
            source       VARCHAR(100),
            source_url   TEXT,
            published_at TIMESTAMPTZ DEFAULT NOW(),
            is_active    BOOLEAN DEFAULT TRUE,
            pageviews_3d INTEGER DEFAULT 0,
            last_ga_sync TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_articles_slug        ON articles(slug);
        CREATE INDEX IF NOT EXISTS idx_articles_active      ON articles(is_active);
        CREATE INDEX IF NOT EXISTS idx_articles_published   ON articles(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_articles_sector      ON articles(sector);
        """)
    conn.commit()
    conn.close()
    log.info("Database initialized.")

def article_exists(slug: str) -> bool:
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM articles WHERE slug=%s", (slug,))
        exists = cur.fetchone() is not None
    conn.close()
    return exists

def save_article(article: dict):
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO articles (slug, headline, sector, summary, analysis, impacts, source, source_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (slug) DO NOTHING
        """, (
            article["slug"], article["headline"], article["sector"],
            article["summary"], article["analysis"],
            json.dumps(article["impacts"]),
            article["source"], article["source_url"]
        ))
    conn.commit()
    conn.close()

def get_stale_articles(days: int = STALE_DAYS, min_views: int = 10) -> list:
    """Return articles older than `days` with fewer than `min_views` pageviews."""
    conn = get_db_conn()
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, slug, headline FROM articles
        WHERE is_active = TRUE
          AND published_at < %s
          AND pageviews_3d < %s
        """, (cutoff, min_views))
        rows = cur.fetchall()
    conn.close()
    return rows

def deactivate_articles(ids: list):
    if not ids:
        return
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE articles SET is_active=FALSE WHERE id=ANY(%s)", (ids,))
    conn.commit()
    conn.close()
    log.info(f"Deactivated {len(ids)} stale articles.")

# ─── GOOGLE ANALYTICS PAGEVIEW SYNC ──────────────────────────────────────────

def sync_ga_pageviews():
    """Fetch 3-day pageviews for all active articles and update DB."""
    if not GA_PROPERTY_ID:
        log.warning("No GA_PROPERTY_ID configured — skipping pageview sync.")
        return

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            RunReportRequest, DateRange, Metric, Dimension
        )
        client = BetaAnalyticsDataClient()
        request = RunReportRequest(
            property=f"properties/{GA_PROPERTY_ID}",
            dimensions=[Dimension(name="pagePath")],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date="3daysAgo", end_date="today")]
        )
        response = client.run_report(request)

        conn = get_db_conn()
        with conn.cursor() as cur:
            for row in response.rows:
                path = row.dimension_values[0].value
                views = int(row.metric_values[0].value)
                # Paths like /article/rbi-rate-hold-2025-04-29
                slug = path.strip("/").split("/")[-1]
                cur.execute("""
                UPDATE articles SET pageviews_3d=%s, last_ga_sync=NOW()
                WHERE slug=%s
                """, (views, slug))
        conn.commit()
        conn.close()
        log.info("GA pageview sync complete.")

    except Exception as e:
        log.error(f"GA sync failed: {e}")

# ─── NEWS FETCHING ─────────────────────────────────────────────────────────────

def fetch_all_news() -> list:
    """Fetch from all RSS feeds, deduplicate by title hash."""
    seen = set()
    items = []
    for feed_conf in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_conf["url"])
            for entry in feed.entries[:10]:
                title = getattr(entry, "title", "").strip()
                if not title:
                    continue
                h = hashlib.md5(title.encode()).hexdigest()[:8]
                if h in seen:
                    continue
                seen.add(h)
                items.append({
                    "title":      title,
                    "summary":    getattr(entry, "summary", ""),
                    "url":        getattr(entry, "link", ""),
                    "source":     feed_conf["source"],
                    "tier":       feed_conf["tier"],
                })
        except Exception as e:
            log.warning(f"Feed fetch failed [{feed_conf['source']}]: {e}")
    log.info(f"Fetched {len(items)} raw news items.")
    return items

def classify_sector(title: str, summary: str) -> str:
    text = (title + " " + summary).lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return sector
    return "Macro Economy"

def make_slug(headline: str, ts: datetime.datetime) -> str:
    base = headline.lower()
    for ch in ["&", "/", "\\", ":", ";", ",", ".", "'", '"', "!", "?", "%", "#", "@"]:
        base = base.replace(ch, "")
    base = "-".join(base.split())[:60]
    date_str = ts.strftime("%Y-%m-%d")
    return f"{base}-{date_str}"

# ─── AI ANALYSIS VIA CLAUDE ───────────────────────────────────────────────────



def generate_analysis(item: dict):
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    prompt = f"""You are a senior Indian equity research analyst.
Analyze this news for BSE/NSE market implications.

HEADLINE: {item['title']}
SUMMARY: {item.get('summary', '')}
SECTOR: {item['sector']}

Return ONLY valid JSON (no markdown, no extra text):
{{
  "professional_summary": "3-sentence analyst summary",
  "sector_outlook": "outlook sentence",
  "stocks": [
    {{
      "name": "Company Name",
      "ticker": "TICKER.NS",
      "impact": "BULLISH or BEARISH or NEUTRAL",
      "confidence": 8,
      "rationale": "one sentence"
    }}
  ],
  "mutual_funds": [
    {{
      "name": "Fund Name",
      "category": "Fund Category",
      "impact": "POSITIVE or NEGATIVE or NEUTRAL",
      "action": "ACCUMULATE or REVIEW or AVOID or HOLD",
      "rationale": "one sentence"
    }}
  ],
  "key_risk": "biggest risk",
  "advisor_note": "actionable note"
}}"""

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        raw = response.text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        log.error(f"Gemini error: {e}")
        return None

# ─── HTML ARTICLE FILE GENERATION ────────────────────────────────────────────

ARTICLE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{headline} | IndiaMarketPulse</title>
<meta name="description" content="{meta_desc}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="https://indiamarketpulse.in/article/{slug}/">
<script type="application/ld+json">
{schema_json}
</script>
<link rel="stylesheet" href="/assets/article.css">
</head>
<body>
  <!-- HEADER INCLUDE -->
  <div data-include="header"></div>

  <article class="article-body" itemscope itemtype="https://schema.org/NewsArticle">
    <header class="article-header">
      <span class="sector-tag">{sector}</span>
      <h1 itemprop="headline">{headline}</h1>
      <p class="byline">
        <span itemprop="publisher" itemscope itemtype="https://schema.org/Organization">
          <span itemprop="name">IndiaMarketPulse</span>
        </span>
        · <time itemprop="datePublished" datetime="{iso_date}">{display_date}</time>
        · Source: <a href="{source_url}" rel="nofollow noopener">{source}</a>
      </p>
    </header>

    <section class="summary-block">
      <p class="lede" itemprop="description">{summary}</p>
    </section>

    <section class="sector-outlook">
      <h2>Sector Outlook</h2>
      <p>{sector_outlook}</p>
    </section>

    <section class="impact-stocks">
      <h2>Stock Impact Analysis</h2>
      <table class="impact-table">
        <thead><tr><th>Stock</th><th>Ticker</th><th>Impact</th><th>Confidence</th><th>Rationale</th></tr></thead>
        <tbody>
          {stock_rows}
        </tbody>
      </table>
    </section>

    <section class="impact-mf">
      <h2>Mutual Fund Signals</h2>
      {mf_cards}
    </section>

    <section class="risk-note">
      <h3>Key Risk to Outlook</h3>
      <p>{key_risk}</p>
    </section>

    <section class="advisor-box">
      <h3>Advisor Note</h3>
      <p>{advisor_note}</p>
    </section>

    <div class="disclaimer">
      <strong>Disclaimer:</strong> This analysis is generated for informational purposes for financial professionals and does not constitute investment advice. Please refer to SEBI guidelines before recommending any security to clients.
    </div>
  </article>

  <div data-include="footer"></div>
</body>
</html>"""

def build_stock_rows(stocks: list) -> str:
    rows = []
    for s in stocks:
        css = {"BULLISH": "bull", "BEARISH": "bear", "NEUTRAL": "neutral"}.get(s.get("impact",""), "neutral")
        rows.append(
            f"<tr><td>{s.get('name','')}</td><td><code>{s.get('ticker','')}</code></td>"
            f"<td class='{css}'>{s.get('impact','')}</td>"
            f"<td>{s.get('confidence','')}/10</td>"
            f"<td>{s.get('rationale','')}</td></tr>"
        )
    return "\n".join(rows)

def build_mf_cards(mfs: list) -> str:
    cards = []
    for mf in mfs:
        action = mf.get("action", "HOLD")
        css = {"ACCUMULATE": "positive", "REVIEW": "warning", "AVOID": "negative", "HOLD": "neutral"}.get(action, "neutral")
        cards.append(
            f'<div class="mf-card {css}">'
            f'<strong>{mf.get("name","")}</strong>'
            f'<span class="category">{mf.get("category","")}</span>'
            f'<span class="action-badge">{action}</span>'
            f'<p>{mf.get("rationale","")}</p>'
            f'</div>'
        )
    return "\n".join(cards)

def write_article_file(article: dict):
    """Write the article HTML file to disk."""
    slug = article["slug"]
    ts   = article.get("published_at", datetime.datetime.utcnow())
    analysis = article.get("analysis_data", {})

    schema = {
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "headline": article["headline"],
        "description": article.get("summary", ""),
        "datePublished": ts.isoformat(),
        "publisher": {"@type": "Organization", "name": "IndiaMarketPulse", "url": "https://indiamarketpulse.in"},
        "url": f"https://indiamarketpulse.in/article/{slug}/",
        "about": {"@type": "Thing", "name": article.get("sector", "")}
    }

    html = ARTICLE_TEMPLATE.format(
        headline      = article["headline"],
        meta_desc     = article.get("summary", "")[:160],
        slug          = slug,
        sector        = article.get("sector", ""),
        iso_date      = ts.isoformat(),
        display_date  = ts.strftime("%d %B %Y, %I:%M %p IST"),
        source        = article["source"],
        source_url    = article.get("source_url", "#"),
        summary       = analysis.get("professional_summary", ""),
        sector_outlook= analysis.get("sector_outlook", ""),
        stock_rows    = build_stock_rows(analysis.get("stocks", [])),
        mf_cards      = build_mf_cards(analysis.get("mutual_funds", [])),
        key_risk      = analysis.get("key_risk", ""),
        advisor_note  = analysis.get("advisor_note", ""),
        schema_json   = json.dumps(schema, indent=2),
    )

    article_dir = CONTENT_DIR / "article" / slug
    article_dir.mkdir(parents=True, exist_ok=True)
    (article_dir / "index.html").write_text(html, encoding="utf-8")

def remove_article_file(slug: str):
    """Remove article from filesystem (or move to archive)."""
    article_dir = CONTENT_DIR / "article" / slug
    archive_dir = CONTENT_DIR / "_archive" / slug
    if article_dir.exists():
        archive_dir.parent.mkdir(parents=True, exist_ok=True)
        article_dir.rename(archive_dir)
        log.info(f"Archived: {slug}")

# ─── HOMEPAGE REGENERATION ────────────────────────────────────────────────────

def regenerate_homepage():
    """Pull latest active articles from DB and rebuild homepage article grid."""
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
        SELECT slug, headline, sector, summary, impacts, source, published_at
        FROM articles WHERE is_active=TRUE
        ORDER BY published_at DESC
        LIMIT %s
        """, (MAX_ARTICLES_PAGE,))
        rows = cur.fetchall()
    conn.close()

    # Build JSON data file consumed by homepage JS
    articles_json = []
    for row in rows:
        slug, headline, sector, summary, impacts, source, pub_at = row
        articles_json.append({
            "slug":      slug,
            "headline":  headline,
            "sector":    sector,
            "summary":   summary,
            "impacts":   impacts,
            "source":    source,
            "published": pub_at.isoformat() if pub_at else None,
            "url":       f"/article/{slug}/"
        })

    data_file = CONTENT_DIR / "data" / "articles.json"
    data_file.parent.mkdir(parents=True, exist_ok=True)
    data_file.write_text(json.dumps(articles_json, default=str, indent=2))
    log.info(f"Homepage data regenerated: {len(articles_json)} articles.")

# ─── SITEMAP GENERATION ───────────────────────────────────────────────────────

def regenerate_sitemap():
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT slug, published_at FROM articles WHERE is_active=TRUE ORDER BY published_at DESC")
        rows = cur.fetchall()
    conn.close()

    base = "https://indiamarketpulse.in"
    urls = [f"""  <url>
    <loc>{base}/</loc>
    <changefreq>hourly</changefreq>
    <priority>1.0</priority>
  </url>"""]

    for slug, pub_at in rows:
        date_str = pub_at.strftime("%Y-%m-%d") if pub_at else ""
        urls.append(f"""  <url>
    <loc>{base}/article/{slug}/</loc>
    <lastmod>{date_str}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>""")

    sitemap = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{chr(10).join(urls)}
</urlset>"""

    sitemap_path = CONTENT_DIR / "sitemap.xml"
    sitemap_path.write_text(sitemap)
    log.info(f"Sitemap regenerated: {len(rows)} URLs.")

# ─── STALE CONTENT CLEANUP ────────────────────────────────────────────────────

def cleanup_stale_content():
    """
    Remove articles that are:
    - Older than STALE_DAYS
    - AND have fewer than 10 pageviews in the last 3 days
    """
    log.info("Starting stale content cleanup…")
    stale = get_stale_articles(days=STALE_DAYS, min_views=10)

    if not stale:
        log.info("No stale articles to remove.")
        return

    ids_to_remove  = []
    for article_id, slug, headline in stale:
        log.info(f"  Removing stale: [{article_id}] {headline[:60]}…")
        remove_article_file(slug)
        ids_to_remove.append(article_id)

    deactivate_articles(ids_to_remove)
    log.info(f"Cleanup complete: {len(ids_to_remove)} articles removed.")

# ─── MAIN ORCHESTRATION ───────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("IndiaMarketPulse Content Engine — Starting Run")
    log.info(f"Timestamp: {datetime.datetime.utcnow().isoformat()} UTC")
    log.info("=" * 60)

    # 1. Init DB
    init_db()

    # 2. Sync GA pageviews (needed for stale detection)
    sync_ga_pageviews()

    # 3. Cleanup stale articles
    cleanup_stale_content()

    # 4. Fetch news
    news_items = fetch_all_news()

    # 5. Filter to financially relevant items (tier 1 priority)
    news_items.sort(key=lambda x: x["tier"])

    published = 0
    skipped   = 0

    for item in news_items[:25]:  # Process max 25 per run to respect API limits
        item["sector"] = classify_sector(item["title"], item.get("summary", ""))
        ts   = datetime.datetime.utcnow()
        slug = make_slug(item["title"], ts)

        if article_exists(slug):
            skipped += 1
            continue

        log.info(f"Analyzing: {item['title'][:70]}…")

        analysis = generate_analysis(item)
        if not analysis:
            log.warning("  → Analysis failed, skipping.")
            continue

        article = {
            "slug":          slug,
            "headline":      item["title"],
            "sector":        item["sector"],
            "summary":       analysis.get("professional_summary", item.get("summary", "")),
            "analysis":      json.dumps(analysis),
            "analysis_data": analysis,
            "impacts": {
                "stocks": analysis.get("stocks", []),
                "mfs":    analysis.get("mutual_funds", [])
            },
            "source":        item["source"],
            "source_url":    item["url"],
            "published_at":  ts,
        }

        save_article(article)
        write_article_file(article)
        published += 1

        log.info(f"  → Published ✓ [{item['sector']}] {slug}")

        # Rate limit: be gentle on Claude API
        time.sleep(2)

    # 6. Regenerate homepage data + sitemap
    regenerate_homepage()
    regenerate_sitemap()

    log.info("-" * 60)
    log.info(f"Run complete. Published: {published} | Skipped (duplicate): {skipped}")
    log.info("=" * 60)


if __name__ == "__main__":
    run()
