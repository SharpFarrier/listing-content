"""
Amazon Listing Checker — Multi-ASIN Scraper
============================================
Reads ASINs from asins.txt (one per line), cycles through them continuously,
stores history in SQLite, and exports data.js for the dashboards.

Usage:
    pip install requests beautifulsoup4 lxml
    python3 scraper.py

Optional flags:
    --min-delay 45      Min seconds between each ASIN scrape (default: 45)
    --max-delay 120     Max seconds between each ASIN scrape (default: 120)
    --cycle-delay 300   Extra seconds to wait after completing a full cycle (default: 300)
    --runs 0            Total full cycles; 0 = run forever (default: 0)
    --db listing.db     SQLite database file (default: listing.db)
    --asins asins.txt   Path to ASIN list file (default: asins.txt)
"""

import argparse
import json
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
}

DATA_JS_PATH = "data.js"
DOMAIN       = "www.amazon.in"


# ---------------------------------------------------------------------------
# ASIN list helpers
# ---------------------------------------------------------------------------

def load_asins(path: str) -> List[str]:
    """Read ASINs from file, strip blanks and comments."""
    if not os.path.exists(path):
        print(f"[!] {path} not found — creating a sample file.")
        with open(path, "w") as f:
            f.write("# Add one ASIN per line. Lines starting with # are ignored.\n")
            f.write("# Example:\n# B0BWS1QCYZ\n")
        return []
    asins = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                asins.append(line.upper())
    return list(dict.fromkeys(asins))  # deduplicate, preserve order


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS scrapes (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    asin             TEXT NOT NULL,
    scraped_at       TEXT NOT NULL,
    title            TEXT,
    brand            TEXT,
    model_number     TEXT,
    price            REAL,
    price_currency   TEXT,
    original_price   REAL,
    discount_pct     REAL,
    coupon_text      TEXT,
    rating           REAL,
    rating_count     INTEGER,
    stars_5          INTEGER,
    stars_4          INTEGER,
    stars_3          INTEGER,
    stars_2          INTEGER,
    stars_1          INTEGER,
    availability     TEXT,
    stock_hint       TEXT,
    sold_by          TEXT,
    ships_from       TEXT,
    prime_eligible   INTEGER,
    bsr_category     TEXT,
    bsr_rank         INTEGER,
    bsr_raw          TEXT,
    bullet_points    TEXT,
    images           TEXT,
    variants         TEXT,
    tech_specs       TEXT,
    breadcrumbs      TEXT,
    date_first_avail TEXT,
    answered_questions INTEGER,
    captcha_hit      INTEGER DEFAULT 0
);
"""


def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(SCHEMA)
    conn.commit()
    return conn


def insert_scrape(conn: sqlite3.Connection, record: dict) -> int:
    cols         = ", ".join(record.keys())
    placeholders = ", ".join(["?"] * len(record))
    sql          = f"INSERT INTO scrapes ({cols}) VALUES ({placeholders})"
    cur          = conn.execute(sql, list(record.values()))
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# Scraping helpers
# ---------------------------------------------------------------------------

def build_url(asin: str) -> str:
    return f"https://{DOMAIN}/dp/{asin}"


def fetch_page(asin: str) -> Tuple[Optional[BeautifulSoup], bool]:
    url     = build_url(asin)
    headers = {**HEADERS_BASE, "User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [!] Request failed: {exc}")
        return None, False

    soup = BeautifulSoup(resp.text, "lxml")

    if soup.find("form", {"action": re.compile(r"/errors/validateCaptcha")}):
        print("  [!] CAPTCHA hit — skipping this round.")
        return soup, True

    return soup, False


def _text(soup, *selectors, default="") -> str:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el.get_text(strip=True)
    return default


def _num(text: str, *, is_float=False):
    cleaned = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        return float(cleaned) if is_float else int(float(cleaned))
    except (ValueError, TypeError):
        return None


def parse_listing(soup: BeautifulSoup, asin: str) -> dict:
    record = {"asin": asin, "scraped_at": datetime.now(timezone.utc).isoformat()}

    record["title"] = _text(soup, "#productTitle", "span#productTitle")

    brand_el = soup.select_one("#bylineInfo, #brand, a#bylineInfo")
    record["brand"] = brand_el.get_text(strip=True).replace("Visit the", "").replace("Store", "").strip() if brand_el else ""

    record["model_number"] = ""
    for row in soup.select("#productDetails_techSpec_section_1 tr, #productDetails_detailBullets_sections1 tr"):
        header = row.select_one("th, td:first-child")
        value  = row.select_one("td:last-child")
        if header and value and "model" in header.get_text().lower():
            record["model_number"] = value.get_text(strip=True)
            break

    price_raw = _text(soup,
        "span.a-price[data-a-size='xl'] span.a-offscreen",
        "span#priceblock_ourprice",
        "span#priceblock_dealprice",
        ".a-price .a-offscreen",
    )
    record["price"]          = _num(price_raw, is_float=True)
    record["price_currency"] = "INR"

    original_raw = _text(soup,
        "span.a-price[data-a-strike='true'] span.a-offscreen",
        "#listPrice",
        "#priceblock_ourprice ~ .a-text-strike",
    )
    record["original_price"] = _num(original_raw, is_float=True)

    if record["price"] and record["original_price"] and record["original_price"] > 0:
        record["discount_pct"] = round((1 - record["price"] / record["original_price"]) * 100, 1)
    else:
        record["discount_pct"] = None

    coupon_el = soup.select_one("#couponBadgeRegularVpc, .couponBadge, #promoPriceBlockMessage")
    record["coupon_text"] = coupon_el.get_text(strip=True) if coupon_el else ""

    rating_raw = _text(soup, "span#acrPopover", "#averageCustomerReviews span.a-icon-alt")
    m = re.search(r"\b([0-9]\.[0-9])\b", rating_raw)
    record["rating"] = float(m.group(1)) if m else None

    rating_count_raw = _text(soup, "#acrCustomerReviewText", "span#acrCustomerReviewText")
    record["rating_count"] = _num(rating_count_raw)

    star_map = {}
    for bar in soup.select("table#histogramTable tr, .cr-widget-histogram tr"):
        label_el = bar.select_one("td:first-child a, .a-list-item a")
        pct_el   = bar.select_one("td:last-child .a-text-right, .a-meter-bar")
        if label_el and pct_el:
            label = label_el.get_text(strip=True)
            m2    = re.search(r"(\d+)%", pct_el.get_text(strip=True) + pct_el.get("aria-valuenow", ""))
            if m2:
                star_map[label] = int(m2.group(1))
    record["stars_5"] = star_map.get("5 star", star_map.get("5 stars"))
    record["stars_4"] = star_map.get("4 star", star_map.get("4 stars"))
    record["stars_3"] = star_map.get("3 star", star_map.get("3 stars"))
    record["stars_2"] = star_map.get("2 star", star_map.get("2 stars"))
    record["stars_1"] = star_map.get("1 star", star_map.get("1 stars"))

    avail_el = soup.select_one("#availability, #outOfStock, #deliveryMessageMirId")
    record["availability"] = avail_el.get_text(strip=True) if avail_el else "Unknown"

    stock_el  = soup.select_one("#availability span")
    hint_text = stock_el.get_text(strip=True) if stock_el else ""
    record["stock_hint"] = hint_text if "left" in hint_text.lower() else ""

    merchant_el = soup.select_one("#merchant-info, #sellerProfileTriggerId, #tabular-buybox-truncate-0")
    record["sold_by"] = merchant_el.get_text(strip=True)[:200] if merchant_el else ""

    ships_el = soup.select_one("#tabular-buybox-truncate-1, #shipsFromSoldBy_feature_div")
    record["ships_from"] = ships_el.get_text(strip=True)[:200] if ships_el else ""

    prime_el = soup.select_one(".a-icon-prime, #primeBadge_feature_div")
    record["prime_eligible"] = 1 if prime_el else 0

    bsr_text = ""
    for el in soup.select("#SalesRank, #productDetails_detailBullets_sections1 tr, li"):
        txt = el.get_text(" ", strip=True)
        if "best seller" in txt.lower() or "best sellers rank" in txt.lower():
            bsr_text = txt
            break
    record["bsr_raw"]  = bsr_text[:500]
    bsr_match = re.search(r"#([\d,]+)", bsr_text)
    record["bsr_rank"] = _num(bsr_match.group(1)) if bsr_match else None
    cat_match = re.search(r"in\s+(.+?)(?:\s*\(|$)", bsr_text, re.IGNORECASE)
    record["bsr_category"] = cat_match.group(1).strip()[:200] if cat_match else ""

    bullets = [li.get_text(strip=True) for li in soup.select("#feature-bullets ul li span")]
    record["bullet_points"] = json.dumps([b for b in bullets if b and "make sure" not in b.lower()])

    images      = []
    script_tags = soup.find_all("script", string=re.compile(r"colorImages|imageGalleryData"))
    for tag in script_tags:
        matches = re.findall(r'"hiRes"\s*:\s*"(https://[^"]+)"', tag.string or "")
        images.extend(matches)
    if not images:
        for img in soup.select("#altImages img, #main-image-container img"):
            src = img.get("src") or img.get("data-src", "")
            if src and "sprite" not in src:
                images.append(src)
    record["images"] = json.dumps(list(dict.fromkeys(images))[:20])

    record["variants"] = json.dumps([])

    specs = {}
    for row in soup.select("#productDetails_techSpec_section_1 tr, "
                            "#productDetails_detailBullets_sections1 tr, .prodDetTable tr"):
        th = row.select_one("th")
        td = row.select_one("td")
        if th and td:
            specs[th.get_text(strip=True)] = td.get_text(strip=True)
    for li in soup.select("#detailBullets_feature_div li"):
        parts = li.get_text(" ", strip=True).split(":", 1)
        if len(parts) == 2:
            specs[parts[0].strip()] = parts[1].strip()
    record["tech_specs"] = json.dumps(specs)

    crumbs = [a.get_text(strip=True) for a in soup.select(
        "#wayfinding-breadcrumbs_feature_div a, .a-breadcrumb a")]
    record["breadcrumbs"] = json.dumps(crumbs)

    record["date_first_avail"] = ""
    for li in soup.select("#detailBullets_feature_div li, #productDetails_detailBullets_sections1 tr"):
        txt = li.get_text(" ", strip=True)
        if "date first available" in txt.lower():
            record["date_first_avail"] = txt.split(":", 1)[-1].strip()[:100]
            break

    qa_text = _text(soup, "#askATFLink span, #questionsSummary")
    qa_m    = re.search(r"([\d,]+)", qa_text)
    record["answered_questions"] = _num(qa_m.group(1)) if qa_m else None

    record["captcha_hit"] = 0
    return record


# ---------------------------------------------------------------------------
# Export data.js — ALL ASINs in one file
# ---------------------------------------------------------------------------

def export_data_js(conn: sqlite3.Connection, all_asins: List[str], path: str = DATA_JS_PATH):
    col_names = [d[0] for d in conn.execute("SELECT * FROM scrapes WHERE 0").description]

    listings = {}
    for asin in all_asins:
        rows = conn.execute(
            "SELECT * FROM scrapes WHERE asin = ? ORDER BY scraped_at ASC", (asin,)
        ).fetchall()

        records = []
        for row in rows:
            r = dict(zip(col_names, row))
            for field in ("bullet_points", "images", "variants", "tech_specs", "breadcrumbs"):
                try:
                    r[field] = json.loads(r.get(field) or "[]")
                except Exception:
                    r[field] = []
            records.append(r)

        listings[asin] = {
            "scrape_count": len(records),
            "records":      records,
        }

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "asins":       all_asins,
        "listings":    listings,
    }

    with open(path, "w", encoding="utf-8") as f:
        f.write("const LISTING_DATA = ")
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write(";\n")

    total = sum(v["scrape_count"] for v in listings.values())
    print(f"  [✓] data.js updated — {len(all_asins)} ASINs, {total} total records")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Amazon multi-ASIN listing scraper")
    parser.add_argument("--asins",       default="asins.txt",  help="Path to ASIN list file")
    parser.add_argument("--min-delay",   type=int, default=45,  help="Min seconds between each ASIN")
    parser.add_argument("--max-delay",   type=int, default=120, help="Max seconds between each ASIN")
    parser.add_argument("--cycle-delay", type=int, default=300, help="Extra seconds after a full cycle")
    parser.add_argument("--runs",        type=int, default=0,   help="Full cycles to run; 0 = forever")
    parser.add_argument("--db",          default="listing.db",  help="SQLite database path")
    args = parser.parse_args()

    conn  = get_db(args.db)
    cycle = 0

    print(f"\n{'='*60}")
    print(f"  Amazon Multi-ASIN Listing Checker")
    print(f"  ASINs file : {args.asins}")
    print(f"  Database   : {args.db}")
    print(f"  Overview   : index.html  ← open this in your browser")
    print(f"  Delay      : {args.min_delay}–{args.max_delay}s per ASIN + {args.cycle_delay}s between cycles")
    print(f"  Cycles     : {'∞' if args.runs == 0 else args.runs}")
    print(f"{'='*60}\n")

    while True:
        # Re-read asins.txt each cycle — add ASINs any time without restarting
        asins = load_asins(args.asins)

        if not asins:
            print("[!] No ASINs in asins.txt. Add some and the scraper will pick them up in 30s.")
            time.sleep(30)
            continue

        cycle += 1
        print(f"\n{'─'*60}")
        print(f"  Cycle #{cycle}  —  {len(asins)} ASINs  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'─'*60}")

        for idx, asin in enumerate(asins, 1):
            print(f"\n  [{idx}/{len(asins)}] ASIN: {asin}")
            soup, captcha = fetch_page(asin)

            if soup is None:
                print("  [!] Could not fetch page. Skipping.")
            elif captcha:
                record = {
                    "asin":         asin,
                    "scraped_at":   datetime.now(timezone.utc).isoformat(),
                    "captcha_hit":  1,
                    "availability": "CAPTCHA",
                }
                insert_scrape(conn, record)
                export_data_js(conn, asins)
            else:
                record = parse_listing(soup, asin)
                insert_scrape(conn, record)
                export_data_js(conn, asins)
                print(f"  Title    : {record.get('title','')[:65]}")
                print(f"  Price    : ₹{record.get('price','N/A')}")
                print(f"  Rating   : {record.get('rating','N/A')} ({record.get('rating_count','N/A')} reviews)")
                print(f"  BSR      : #{record.get('bsr_rank','N/A')} — {str(record.get('bsr_category','N/A'))[:40]}")
                print(f"  Stock    : {record.get('availability','N/A')}")

            if idx < len(asins):
                delay = random.randint(args.min_delay, args.max_delay)
                print(f"  Sleeping {delay}s before next ASIN...")
                time.sleep(delay)

        if args.runs > 0 and cycle >= args.runs:
            print(f"\n[✓] Completed {cycle} cycles. Exiting.")
            break

        print(f"\n  Cycle #{cycle} complete. Waiting {args.cycle_delay}s before next cycle...")
        time.sleep(args.cycle_delay)


if __name__ == "__main__":
    main()
