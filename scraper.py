"""
College Monitor — scraper.py
Uses curl_cffi with Chrome impersonation to bypass Akamai bot detection.
BeautifulSoup parses the HTML — no Playwright needed on GitHub Actions.
"""

import os
import re
import time
import json
from datetime import datetime
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as curl_requests
    CURL_AVAILABLE = True
except ImportError:
    CURL_AVAILABLE = False
    print("[WARN] curl_cffi not available")


def fetch_html(url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            resp = curl_requests.get(
                url,
                impersonate="chrome120",
                timeout=30,
                headers={
                    "Accept-Language": "en-IN,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                }
            )
            if resp.status_code == 200:
                return resp.text
            else:
                print(f"  [WARN] HTTP {resp.status_code} for {url}")
        except Exception as e:
            print(f"  [Retry {attempt+1}] fetch error: {e}")
            time.sleep(3 * (attempt + 1))
    return None


def normalize(val: str) -> str:
    v = val.strip()
    return "" if re.match(r"^[\-–/\s]+$", v) else v


def parse_placement_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    target = None
    for table in soup.find_all("table"):
        ths = [th.get_text(strip=True) for th in table.find_all("th")]
        if any("Particulars" in h for h in ths):
            target = table
            break
        if "Students Placed" in table.get_text():
            target = table
            break
    if not target:
        return []
    headers = [th.get_text(strip=True) for th in target.find_all("th")]
    if not headers:
        first_tr = target.find("tr")
        if first_tr:
            headers = [td.get_text(strip=True) for td in first_tr.find_all(["td","th"])]
    print(f"    Placement headers: {headers}")
    tbody = target.find("tbody")
    trs = tbody.find_all("tr") if tbody else target.find_all("tr")[1:]
    for tr in trs:
        cells = [normalize(td.get_text(strip=True)) for td in tr.find_all(["td","th"])]
        if not any(cells):
            continue
        row_dict = {headers[i] if i < len(headers) else f"col_{i}": v for i, v in enumerate(cells)}
        rows.append(row_dict)
    return rows


def parse_ranking_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    target = None
    for table in soup.find_all("table"):
        ths = [th.get_text(strip=True) for th in table.find_all("th")]
        if (any("Category" in h for h in ths) and
                any(re.match(r"^\d{4}$", h) for h in ths) and
                ("B.E" in table.get_text() or "B.Arch" in table.get_text())):
            target = table
            break
    if not target:
        return []
    headers = [th.get_text(strip=True) for th in target.find_all("th")]
    print(f"    Ranking headers: {headers}")
    year_cols = sorted([h for h in headers if re.match(r"^\d{4}$", h)])
    latest_year = year_cols[-1] if year_cols else None
    tbody = target.find("tbody")
    trs = tbody.find_all("tr") if tbody else target.find_all("tr")[1:]
    for tr in trs:
        cells = [normalize(td.get_text(strip=True)) for td in tr.find_all(["td","th"])]
        if not any(cells):
            continue
        row_dict = {headers[i] if i < len(headers) else f"col_{i}": v for i, v in enumerate(cells)}
        if latest_year:
            row_dict["rank_value"] = re.sub(r"[^\d]", "", row_dict.get(latest_year, "")) or ""
        rows.append(row_dict)
    return rows


def parse_rank_publisher_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    wrappers = soup.find_all(class_="bc4a0d")
    print(f"    Rank publisher entries: {len(wrappers)}")
    for wrapper in wrappers:
        cat_el  = wrapper.find(class_="f1495c")
        pub_el  = wrapper.find(class_="d8ca5d")
        rank_el = wrapper.find(class_="a3ae6e")
        if not cat_el or not pub_el:
            continue
        pub_raw   = pub_el.get_text(strip=True)
        parts     = pub_raw.split(",", 1)
        publisher = parts[0].strip()
        year      = parts[1].strip() if len(parts) > 1 else ""
        rank_raw  = rank_el.get_text(strip=True).lstrip("#") if rank_el else ""
        rows.append({
            "Category":   cat_el.get_text(strip=True),
            "Publisher":  publisher,
            "Year":       year,
            "Rank":       f"#{rank_raw}" if rank_raw else "",
            "rank_value": rank_raw
        })
    return rows


def scrape_college_sync(college: dict) -> dict:
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
        return result

    try:
        if placement_url:
            print(f"  Fetching placement: {placement_url}")
            html = fetch_html(placement_url)
            if html:
                result["placement_data"] = parse_placement_table(html)
                print(f"    → {len(result['placement_data'])} rows found")
            else:
                print(f"  [WARN] Failed to fetch placement page")
        else:
            print(f"  [SKIP] No placement URL")

        if ranking_url:
            time.sleep(2)
            print(f"  Fetching rankings: {ranking_url}")
            html = fetch_html(ranking_url)
            if html:
                result["ranking_data"]        = parse_ranking_table(html)
                result["rank_publisher_data"] = parse_rank_publisher_table(html)
                print(f"    → {len(result['ranking_data'])} ranking rows, "
                      f"{len(result['rank_publisher_data'])} publisher entries")
            else:
                print(f"  [WARN] Failed to fetch ranking page")
        else:
            print(f"  [SKIP] No ranking URL")

    except Exception as e:
        result["error"] = str(e)
        print(f"  [ERROR] {college['name']}: {e}")

    return result


def scrape_all_colleges(colleges: list[dict]) -> list[dict]:
    results = []
    delay = int(os.getenv("SCRAPE_DELAY_SECONDS", "4"))
    for i, college in enumerate(colleges):
        label = college["name"]
        if college.get("campus"):
            label += f" ({college['campus']})"
        print(f"\n[{i+1}/{len(colleges)}] Scraping: {label}")
        result = scrape_college_sync(college)
        results.append(result)
        if i < len(colleges) - 1:
            print(f"  Waiting {delay}s...")
            time.sleep(delay)
    return results


if __name__ == "__main__":
    test = [{"name": "SRM University", "campus": "",
        "placement_url": "https://www.shiksha.com/university/srm-institute-of-science-and-technology-kattankulathur-chennai-24749/placement",
        "ranking_url":   "https://www.shiksha.com/university/srm-institute-of-science-and-technology-kattankulathur-chennai-24749/ranking"}]
    results = scrape_all_colleges(test)
    print(json.dumps(results, indent=2, ensure_ascii=False))
