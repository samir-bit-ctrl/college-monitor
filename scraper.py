"""
College Monitor — scraper.py
Three tables scraped per college:
  1. placement_data        → Particulars | Statistics (2023/2024/2025)
  2. ranking_data          → Category | 2023 | 2024 | 2025  (NIRF year-wise)
  3. rank_publisher_data   → Category | Publisher | Year | Rank  (.bc4a0d list)

Tweaks:
  - placement_url and ranking_url are both optional (skip if empty)
  - campus field passed through from colleges sheet
"""

import asyncio
import json
import os
import re
from datetime import datetime
from playwright.async_api import async_playwright


async def new_stealth_context(browser):
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900},
        locale="en-IN", timezone_id="Asia/Kolkata",
        extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"}
    )
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return context


async def load_page(page, url: str, retries: int = 2):
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(6000)
            return
        except Exception as e:
            if attempt < retries - 1:
                print(f"    [Retry {attempt+1}] {e}")
                await asyncio.sleep(5)
            else:
                raise


def normalize(val: str) -> str:
    return "" if re.match(r"^[\-–/\s]+$", val.strip()) else val.strip()


async def extract_table_by_js(page, keyword: str) -> list[list[str]]:
    return await page.evaluate(f"""() => {{
        const tables = document.querySelectorAll('table');
        for (let i = 0; i < tables.length; i++) {{
            if (tables[i].innerText.includes('{keyword}')) {{
                const rows = [];
                tables[i].querySelectorAll('tr').forEach(tr => {{
                    const cells = [];
                    tr.querySelectorAll('td, th').forEach(td => cells.push(td.innerText.trim()));
                    if (cells.some(c => c.length > 0)) rows.push(cells);
                }});
                return rows;
            }}
        }}
        return [];
    }}""")


# ─────────────────────────────────────────────
# 1. PLACEMENT TABLE
# ─────────────────────────────────────────────

async def scrape_placement_table(page, url: str) -> list[dict]:
    if not url:
        return []
    await load_page(page, url)
    rows = []
    try:
        raw = await extract_table_by_js(page, "Particulars")
        if not raw:
            raw = await extract_table_by_js(page, "Students Placed")
        if not raw:
            print(f"  [WARN] No placement table found")
            return []
        headers = raw[0]
        print(f"    Placement headers: {headers}")
        for row_cells in raw[1:]:
            if not any(row_cells):
                continue
            row_dict = {}
            for i, val in enumerate(row_cells):
                key = headers[i] if i < len(headers) else f"col_{i}"
                row_dict[key] = normalize(val)
            rows.append(row_dict)
    except Exception as e:
        print(f"  [ERROR] Placement scrape failed: {e}")
    return rows


# ─────────────────────────────────────────────
# 2. RANKING YEAR-WISE TABLE (NIRF Category | 2023 | 2024 | 2025)
# ─────────────────────────────────────────────

async def scrape_ranking_table(page, url: str) -> list[dict]:
    if not url:
        return []
    await load_page(page, url)
    rows = []
    try:
        raw = await page.evaluate("""() => {
            const tables = document.querySelectorAll('table');
            for (let i = 0; i < tables.length; i++) {
                const text = tables[i].innerText;
                if (text.includes('Category') && text.includes('B.E') && /20\d\d/.test(text)) {
                    const rows = [];
                    tables[i].querySelectorAll('tr').forEach(tr => {
                        const cells = [];
                        tr.querySelectorAll('td, th').forEach(td => cells.push(td.innerText.trim()));
                        if (cells.some(c => c.length > 0)) rows.push(cells);
                    });
                    return rows;
                }
            }
            return [];
        }""")
        if not raw:
            print(f"  [WARN] No year-wise ranking table found")
            return []
        headers = raw[0]
        print(f"    Ranking headers: {headers}")
        year_cols = sorted([h for h in headers if re.match(r"^\d{4}$", h)])
        latest_year = year_cols[-1] if year_cols else None
        for row_cells in raw[1:]:
            if not any(row_cells):
                continue
            row_dict = {}
            for i, val in enumerate(row_cells):
                key = headers[i] if i < len(headers) else f"col_{i}"
                row_dict[key] = normalize(val)
            if latest_year:
                row_dict["rank_value"] = re.sub(r"[^\d]", "", row_dict.get(latest_year, "")) or ""
            rows.append(row_dict)
    except Exception as e:
        print(f"  [ERROR] Year-wise ranking scrape failed: {e}")
    return rows


# ─────────────────────────────────────────────
# 3. RANK PUBLISHER & CATEGORY LIST
# Structure confirmed:
#   .bc4a0d  = wrapper row
#   .f1495c  = category  (BBA, B.Arch, B.E./B.Tech …)
#   .d8ca5d  = publisher + year  (India Today, 2024)
#   .a3ae6e  = rank  (#10, #18 …)
# ─────────────────────────────────────────────

async def scrape_rank_publisher_table(page, url: str) -> list[dict]:
    """Page already loaded by scrape_ranking_table — no reload needed."""
    if not url:
        return []
    rows = []
    try:
        raw = await page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('.bc4a0d').forEach(wrapper => {
                const categoryEl  = wrapper.querySelector('.f1495c');
                const publisherEl = wrapper.querySelector('.d8ca5d');
                const rankEl      = wrapper.querySelector('.a3ae6e');
                if (!categoryEl || !publisherEl) return;

                const pubRaw    = publisherEl.innerText.trim();
                const parts     = pubRaw.split(',');
                const publisher = parts[0]?.trim() || pubRaw;
                const year      = parts[1]?.trim() || '';
                const rank      = rankEl?.innerText.trim().replace('#','') || '';

                results.push({
                    category:  categoryEl.innerText.trim(),
                    publisher: publisher,
                    year:      year,
                    rank:      rank
                });
            });
            return results;
        }""")
        print(f"    Rank publisher entries: {len(raw)}")
        for entry in raw:
            rows.append({
                "Category":   entry.get("category", ""),
                "Publisher":  entry.get("publisher", ""),
                "Year":       entry.get("year", ""),
                "Rank":       f"#{entry['rank']}" if entry.get("rank") else "",
                "rank_value": entry.get("rank", "")
            })
    except Exception as e:
        print(f"  [ERROR] Rank publisher scrape failed: {e}")
    return rows


# ─────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────

async def scrape_college(college: dict) -> dict:
    result = {
        "name":                college["name"],
        "campus":              college.get("campus", ""),
        "placement_data":      [],
        "ranking_data":        [],
        "rank_publisher_data": [],
        "scraped_at":          datetime.now().isoformat(),
        "error":               None
    }

    placement_url = college.get("placement_url", "").strip()
    ranking_url   = college.get("ranking_url", "").strip()

    if not placement_url and not ranking_url:
        result["error"] = "No URLs provided"
        print(f"  [SKIP] No URLs for {college['name']}")
        return result

    async with async_playwright() as p:
        headless = os.getenv("HEADLESS", "false").lower() == "true"
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        context = await new_stealth_context(browser)
        page = await context.new_page()
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,otf}",
            lambda route: route.abort()
        )

        try:
            if placement_url:
                print(f"  Scraping placement: {placement_url}")
                result["placement_data"] = await scrape_placement_table(page, placement_url)
                print(f"    → {len(result['placement_data'])} rows found")
                await asyncio.sleep(3)
            else:
                print(f"  [SKIP] No placement URL")

            if ranking_url:
                print(f"  Scraping rankings (year-wise): {ranking_url}")
                result["ranking_data"] = await scrape_ranking_table(page, ranking_url)
                print(f"    → {len(result['ranking_data'])} rows found")

                print(f"  Scraping rank publisher list...")
                result["rank_publisher_data"] = await scrape_rank_publisher_table(page, ranking_url)
                print(f"    → {len(result['rank_publisher_data'])} entries found")
            else:
                print(f"  [SKIP] No ranking URL")

        except Exception as e:
            result["error"] = str(e)
            print(f"  [ERROR] {college['name']}: {e}")
        finally:
            await browser.close()

    return result


async def scrape_all_colleges(colleges: list[dict]) -> list[dict]:
    results = []
    for i, college in enumerate(colleges):
        label = college["name"]
        if college.get("campus"):
            label += f" ({college['campus']})"
        print(f"\n[{i+1}/{len(colleges)}] Scraping: {label}")
        result = await scrape_college(college)
        results.append(result)
        if i < len(colleges) - 1:
            delay = int(os.getenv("SCRAPE_DELAY_SECONDS", "4"))
            print(f"  Waiting {delay}s before next college...")
            await asyncio.sleep(delay)
    return results
