"""
main.py — Shiksha College Monitor orchestrator

Run manually:  python main.py
Run via CI:    triggered by GitHub Actions cron (see .github/workflows/monitor.yml)

Required environment variables:
  SPREADSHEET_ID           — Google Sheets ID (from the URL)
  GOOGLE_CREDENTIALS_JSON  — Service account JSON (as a single-line string)
  TELEGRAM_BOT_TOKEN       — Telegram bot token
  TELEGRAM_CHAT_ID         — Telegram group/channel chat ID
  TEAMS_WEBHOOK_URL        — MS Teams incoming webhook URL

Optional:
  SCRAPE_DELAY_SECONDS     — Delay between colleges (default: 4)
  SEND_DAILY_SUMMARY       — "true" to always send summary even with no changes (default: true)
"""

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
from change_detector import detect_changes, format_changes_for_alert
from alerts import (
    send_telegram_alert, send_telegram_summary,
    send_teams_alert, send_teams_summary
)


def main():
    print("=" * 55)
    print(f"  Shiksha College Monitor — {datetime.now().strftime('%d %b %Y %H:%M')}")
    print("=" * 55)

    # ── Config ────────────────────────────────────────────
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("[FATAL] SPREADSHEET_ID not set. Exiting.")
        sys.exit(1)

    send_summary = os.getenv("SEND_DAILY_SUMMARY", "true").lower() == "true"

    # ── Connect to Google Sheets ──────────────────────────
    print("\n[1/4] Connecting to Google Sheets...")
    spreadsheet = open_spreadsheet(spreadsheet_id)
    ensure_sheets(spreadsheet)
    colleges = get_active_colleges(spreadsheet)

    if not colleges:
        print("  No active colleges found in sheet. Add rows to 'colleges' tab.")
        sys.exit(0)

    print(f"  Found {len(colleges)} active college(s)")

    # ── Scrape all colleges ───────────────────────────────
    print(f"\n[2/4] Scraping {len(colleges)} college(s)...")
    college_configs = [
        {
            "name": c["college_name"],
            "placement_url": c["placement_url"],
            "ranking_url": c["ranking_url"]
        }
        for c in colleges
    ]
    scraped_results = asyncio.run(scrape_all_colleges(college_configs))

    # Build rank_threshold lookup
    threshold_map = {
        c["college_name"]: int(c.get("rank_threshold") or 0)
        for c in colleges
    }

    # ── Compare + Detect Changes ──────────────────────────
    print("\n[3/4] Detecting changes...")
    all_changes = []
    summary_lines = []

    for result in scraped_results:
        name = result["name"]
        threshold = threshold_map.get(name, 0)

        if result.get("error"):
            print(f"  ⚠️  {name}: scrape error — {result['error']}")
            summary_lines.append(f"⚠️ {name}: scrape error")
            continue

        college_all_changes = []

        for silo, new_rows in [
            ("placement", result["placement_data"]),
            ("ranking", result["ranking_data"])
        ]:
            if not new_rows:
                print(f"  ⚠️  {name} [{silo}]: no data scraped, skipping diff")
                continue

            # Load existing snapshot
            old_snapshot = load_snapshot(spreadsheet, silo)

            # Detect changes
            changes = detect_changes(
                college_name=name,
                silo=silo,
                old_snapshot=old_snapshot,
                new_rows=new_rows,
                rank_threshold=threshold
            )

            if changes:
                print(f"  🔴 {name} [{silo}]: {len(changes)} change(s) detected")
                college_all_changes.extend(changes)

                # Format and send alerts immediately per college+silo
                summary = format_changes_for_alert(changes)
                college_url = (
                    result.get("placement_url", "") if silo == "placement"
                    else result.get("ranking_url", "")
                )

                send_telegram_alert(name, silo, summary, len(changes))
                send_teams_alert(name, silo, summary, len(changes), college_url)
            else:
                print(f"  ✅ {name} [{silo}]: no changes")

            # Always update snapshot with latest data
            save_snapshot(spreadsheet, silo, name, new_rows)

        # Log all changes for this college
        has_changes = bool(college_all_changes)
        if has_changes:
            log_changes(spreadsheet, college_all_changes)
            all_changes.extend(college_all_changes)
            summary_lines.append(
                f"🔴 {name}: {len(college_all_changes)} change(s) across placement + ranking"
            )
        else:
            summary_lines.append(f"✅ {name}: no changes")

        # Update timestamps in colleges sheet
        update_college_timestamps(spreadsheet, name, has_changes)

    # ── Daily Summary ─────────────────────────────────────
    print(f"\n[4/4] Run complete.")
    print(f"  Total changes this run: {len(all_changes)}")

    if send_summary:
        send_telegram_summary(summary_lines)
        send_teams_summary(summary_lines)

    print("\n✅ Done.\n")


if __name__ == "__main__":
    main()
