from dotenv import load_dotenv
load_dotenv()

import asyncio
import os
import sys
from datetime import datetime

from scraper import scrape_all_colleges
from sheets_manager import (
    open_spreadsheet, ensure_sheets,
    get_active_colleges, load_snapshot, save_snapshot,
    log_changes, update_college_timestamps
)
from change_detector import detect_changes
from alerts import (
    send_telegram_alert, send_telegram_summary,
    send_teams_alert, send_teams_summary
)


def main():
    print("=" * 55)
    print(f"  College Monitor — {datetime.now().strftime('%d %b %Y %H:%M')}")
    print("=" * 55)

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("[FATAL] SPREADSHEET_ID not set. Exiting.")
        sys.exit(1)

    send_summary = os.getenv("SEND_DAILY_SUMMARY", "true").lower() == "true"

    print("\n[1/4] Connecting to Google Sheets...")
    spreadsheet = open_spreadsheet(spreadsheet_id)
    ensure_sheets(spreadsheet)
    colleges = get_active_colleges(spreadsheet)

    if not colleges:
        print("  No active colleges found.")
        sys.exit(0)

    # Skip colleges with no URLs at all
    valid_colleges = [
        c for c in colleges
        if c.get("placement_url", "").strip() or c.get("ranking_url", "").strip()
    ]
    skipped = [c["college_name"] for c in colleges if c not in valid_colleges]
    if skipped:
        print(f"  Skipped (no URLs): {', '.join(skipped)}")

    print(f"  Found {len(valid_colleges)} college(s) to scrape")

    # Build configs for scraper
    college_configs = [
        {
            "name":          c["college_name"],
            "campus":        str(c.get("campus", "") or ""),
            "placement_url": str(c.get("placement_url", "") or "").strip(),
            "ranking_url":   str(c.get("ranking_url", "") or "").strip(),
        }
        for c in valid_colleges
    ]

    # Build threshold lookup keyed by (name, campus)
    threshold_map = {
        (c["college_name"], str(c.get("campus","") or "")): int(c.get("rank_threshold") or 0)
        for c in valid_colleges
    }

    print(f"\n[2/4] Scraping {len(college_configs)} college(s)...")
    scraped_results = asyncio.run(scrape_all_colleges(college_configs))

    print("\n[3/4] Detecting changes...")
    all_changes  = []
    summary_lines = []

    # Silos to process per college
    SILOS = [
        ("placement",      "placement_data"),
        ("ranking",        "ranking_data"),
        ("rank_publisher", "rank_publisher_data"),
    ]

    for result in scraped_results:
        name   = result["name"]
        campus = result.get("campus", "")
        threshold = threshold_map.get((name, campus), 0)

        if result.get("error"):
            print(f"  ⚠️  {name} ({campus}): scrape error — {result['error']}")
            summary_lines.append(f"⚠️ {name} ({campus}): scrape error")
            continue

        college_changes = []

        for silo, data_key in SILOS:
            new_rows = result.get(data_key, [])
            if not new_rows:
                continue  # silo had no URL or no data — skip silently

            old_snapshot = load_snapshot(spreadsheet, silo, name, campus)
            changes = detect_changes(
                college_name=name,
                campus=campus,
                silo=silo,
                old_snapshot=old_snapshot,
                new_rows=new_rows,
                rank_threshold=threshold if silo in ("ranking","rank_publisher") else 0
            )

            if changes:
                print(f"  🔴 {name} ({campus}) [{silo}]: {len(changes)} change(s)")
                college_changes.extend(changes)
                college_url = result.get(
                    "placement_url" if silo == "placement" else "ranking_url", ""
                )
                send_telegram_alert(name, silo, changes, len(changes))
                send_teams_alert(name, silo, changes, len(changes), college_url)
            else:
                print(f"  ✅ {name} ({campus}) [{silo}]: no changes")

            save_snapshot(spreadsheet, silo, name, campus, new_rows)

        has_changes = bool(college_changes)
        if has_changes:
            log_changes(spreadsheet, college_changes)
            all_changes.extend(college_changes)
            summary_lines.append(
                f"🔴 {name} ({campus}): {len(college_changes)} change(s)"
            )
        else:
            summary_lines.append(f"✅ {name} ({campus or '—'}): no changes")

        update_college_timestamps(spreadsheet, name, campus, has_changes)

    print(f"\n[4/4] Run complete. Total changes: {len(all_changes)}")

    if send_summary:
        send_telegram_summary(summary_lines)
        send_teams_summary(summary_lines)

    print("\n✅ Done.\n")


if __name__ == "__main__":
    main()
