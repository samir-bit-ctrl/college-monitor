"""
Shiksha College Monitor — scraper.py
Scrapes placement stats table and rankings table from Shiksha college pages.
Uses Playwright for JS-rendered content.
"""

import asyncio
import json
import os
import re
import time
from datetime import datetime
from playwright.async_api import async_playwright


# ─────────────────────────────────────────────
# PLACEMENT TABLE SCRAPER
# Targets the multi-year stats table:
#   Particulars | Statistics (YYYY) | Statistics (YYYY) ...
# ─────────────────────────────────────────────

async def scrape_placement_table(page, url: str) -> list[dict]:
    """
    Navigates to the placement silo URL and extracts the main stats table.
    Returns a list of row dicts, e.g.:
      [{"particular": "Students Placed", "2023": "2754", "2024": "4210", ...}, ...]
    """
    await page.goto(url, wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(2000)  # extra buffer for hydration

    rows = []
    try:
        # The placement stats table has thead with "Particulars" header
        # and tbody rows with stat name + year-wise values
        table = await page.query_selector("table:has(th:text('Particulars'))")
        if not table:
            # fallback: grab first table on the placement section
            table = await page.query_selector(".placement-section table, #placement table, table")

        if not table:
            print(f"  [WARN] No placement table found at {url}")
            return []

        # Extract headers (year columns)
        headers = []
        header_cells = await table.query_selector_all("thead th, thead td")
        for cell in header_cells:
            text = (await cell.inner_text()).strip()
            headers.append(text)

        # If no thead, try first tr as header
        if not headers:
            first_row = await table.query_selector("tr:first-child")
            if first_row:
                cells = await first_row.query_selector_all("td, th")
                for cell in cells:
                    headers.append((await cell.inner_text()).strip())

        # Extract data rows
        body_rows = await table.query_selector_all("tbody tr")
        if not body_rows:
            all_rows = await table.query_selector_all("tr")
            body_rows = all_rows[1:]  # skip header row

        for tr in body_rows:
            cells = await tr.query_selector_all("td, th")
            cell_texts = [(await c.inner_text()).strip() for c in cells]

            # Skip empty rows or separator rows
            if not any(cell_texts):
                continue

            row_dict = {}
            for i, val in enumerate(cell_texts):
                key = headers[i] if i < len(headers) else f"col_{i}"
                # Normalize "–/–" or "- / -" missing values to empty string
                row_dict[key] = "" if re.match(r"^[\-–/\s]+$", val) else val

            rows.append(row_dict)

    except Exception as e:
        print(f"  [ERROR] Placement scrape failed: {e}")

    return rows


# ─────────────────────────────────────────────
# RANKINGS TABLE SCRAPER
# Targets the rankings list:
#   Rank Publisher & Category | Rank
#   e.g. B.E./B.Tech / NIRF, 2025 → #14
# ─────────────────────────────────────────────

async def scrape_ranking_table(page, url: str) -> list[dict]:
    """
    Navigates to the rankings silo URL and extracts the ranking list table.
    Returns a list of row dicts, e.g.:
      [{"category": "B.E. / B.Tech", "publisher_year": "NIRF, 2025", "rank": "#14"}, ...]
    """
    await page.goto(url, wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(2000)

    rows = []
    try:
        # Rankings page has a table/list with "Rank publisher & Category" and "Rank" columns
        table = await page.query_selector(
            "table:has(th:text-matches('Rank publisher', 'i')), "
            "table:has(th:text-matches('Rank Publisher', 'i'))"
        )

        if not table:
            # Fallback: look for any table with a "Rank" column heading
            table = await page.query_selector("table:has(th:text('Rank'))")

        if not table:
            print(f"  [WARN] No rankings table found at {url}")
            return []

        body_rows = await table.query_selector_all("tbody tr")
        if not body_rows:
            all_rows = await table.query_selector_all("tr")
            body_rows = all_rows[1:]

        for tr in body_rows:
            cells = await tr.query_selector_all("td")
            if len(cells) < 2:
                continue

            # First cell: publisher logo img (alt text) + category text + year text
            first_cell_text = (await cells[0].inner_text()).strip()
            rank_text = (await cells[-1].inner_text()).strip()

            # Parse first cell — usually: "Category\nPublisher, Year"
            lines = [l.strip() for l in first_cell_text.splitlines() if l.strip()]
            if len(lines) >= 2:
                category = lines[0]
                publisher_year = lines[1]
            elif len(lines) == 1:
                category = lines[0]
                publisher_year = ""
            else:
                continue

            # Clean rank (remove # symbol for numeric comparison later)
            rank_clean = rank_text.lstrip("#").strip()

            rows.append({
                "category": category,
                "publisher_year": publisher_year,
                "rank": rank_text,        # keep original e.g. "#14"
                "rank_value": rank_clean  # numeric string e.g. "14"
            })

    except Exception as e:
        print(f"  [ERROR] Rankings scrape failed: {e}")

    return rows


# ─────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────

async def scrape_college(college: dict) -> dict:
    """
    Scrapes both tables for a single college.
    college = {
        "name": "SRM University",
        "placement_url": "https://...",
        "ranking_url": "https://..."
    }
    Returns dict with placement_data and ranking_data lists.
    """
    result = {
        "name": college["name"],
        "placement_data": [],
        "ranking_data": [],
        "scraped_at": datetime.now().isoformat(),
        "error": None
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        # Block images/fonts to speed up loading
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,otf}",
            lambda route: route.abort()
        )

        try:
            print(f"  Scraping placement: {college['placement_url']}")
            result["placement_data"] = await scrape_placement_table(page, college["placement_url"])
            print(f"    → {len(result['placement_data'])} rows found")

            # Polite delay between pages
            await asyncio.sleep(3)

            print(f"  Scraping rankings: {college['ranking_url']}")
            result["ranking_data"] = await scrape_ranking_table(page, college["ranking_url"])
            print(f"    → {len(result['ranking_data'])} rows found")

        except Exception as e:
            result["error"] = str(e)
            print(f"  [ERROR] {college['name']}: {e}")
        finally:
            await browser.close()

    return result


async def scrape_all_colleges(colleges: list[dict]) -> list[dict]:
    """Scrapes all colleges sequentially with a polite delay between each."""
    results = []
    for i, college in enumerate(colleges):
        print(f"\n[{i+1}/{len(colleges)}] Scraping: {college['name']}")
        result = await scrape_college(college)
        results.append(result)
        if i < len(colleges) - 1:
            delay = int(os.getenv("SCRAPE_DELAY_SECONDS", "4"))
            print(f"  Waiting {delay}s before next college...")
            await asyncio.sleep(delay)
    return results


if __name__ == "__main__":
    # Quick test with SRM
    test_colleges = [
        {
            "name": "SRM Institute of Science and Technology",
            "placement_url": "https://www.shiksha.com/university/srm-institute-of-science-and-technology-kattankulathur-chennai-24749/placement",
            "ranking_url": "https://www.shiksha.com/university/srm-institute-of-science-and-technology-kattankulathur-chennai-24749/ranking"
        }
    ]
    results = asyncio.run(scrape_all_colleges(test_colleges))
    print("\n=== RESULT ===")
    print(json.dumps(results, indent=2))
