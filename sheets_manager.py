"""
sheets_manager.py — Google Sheets read/write for Shiksha College Monitor

Sheet structure:
  Sheet 1: colleges         — input URLs (managed manually)
  Sheet 2: placement_snapshot — last known placement table per college
  Sheet 3: ranking_snapshot   — last known ranking table per college
  Sheet 4: change_log         — every detected change with timestamp
"""

import json
import os
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SHEET_COLLEGES           = "colleges"
SHEET_PLACEMENT_SNAPSHOT = "placement_snapshot"
SHEET_RANKING_SNAPSHOT   = "ranking_snapshot"
SHEET_CHANGE_LOG         = "change_log"


def get_client() -> gspread.Client:
    """Authenticate using service account JSON (from env or file)."""
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(creds_json)
            creds_path = f.name
    else:
        creds_path = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(creds)


def open_spreadsheet(spreadsheet_id: str) -> gspread.Spreadsheet:
    client = get_client()
    return client.open_by_key(spreadsheet_id)


def ensure_sheets(spreadsheet: gspread.Spreadsheet):
    """Create required sheets if they don't exist."""
    existing = {ws.title for ws in spreadsheet.worksheets()}
    required = [
        SHEET_COLLEGES,
        SHEET_PLACEMENT_SNAPSHOT,
        SHEET_RANKING_SNAPSHOT,
        SHEET_CHANGE_LOG
    ]
    for name in required:
        if name not in existing:
            spreadsheet.add_worksheet(title=name, rows=500, cols=20)
            print(f"  Created sheet: {name}")

    # Add headers to colleges sheet if empty
    colleges_ws = spreadsheet.worksheet(SHEET_COLLEGES)
    if not colleges_ws.get_all_values():
        colleges_ws.append_row([
            "college_name", "placement_url", "ranking_url",
            "active", "rank_threshold", "last_scraped", "last_changed"
        ])
        print("  Added headers to 'colleges' sheet")

    # Add headers to change_log sheet if empty
    log_ws = spreadsheet.worksheet(SHEET_CHANGE_LOG)
    if not log_ws.get_all_values():
        log_ws.append_row([
            "timestamp", "college_name", "silo",
            "change_type", "row_key", "old_value", "new_value"
        ])
        print("  Added headers to 'change_log' sheet")


# ─────────────────────────────────────────────
# READ COLLEGES LIST
# ─────────────────────────────────────────────

def get_active_colleges(spreadsheet: gspread.Spreadsheet) -> list[dict]:
    """Read active colleges from the colleges sheet."""
    ws = spreadsheet.worksheet(SHEET_COLLEGES)
    records = ws.get_all_records()
    return [
        r for r in records
        if str(r.get("active", "TRUE")).upper() in ("TRUE", "1", "YES")
    ]


# ─────────────────────────────────────────────
# SNAPSHOT READ/WRITE
# ─────────────────────────────────────────────

def _snapshot_key(college_name: str, row_key: str) -> str:
    """Compound key: 'CollegeName|||row_identifier'"""
    return f"{college_name}|||{row_key}"


def load_snapshot(spreadsheet: gspread.Spreadsheet, silo: str) -> dict:
    """
    Load snapshot for a silo ('placement' or 'ranking').
    Returns dict: { snapshot_key → json_string_of_row }
    """
    sheet_name = SHEET_PLACEMENT_SNAPSHOT if silo == "placement" else SHEET_RANKING_SNAPSHOT
    ws = spreadsheet.worksheet(sheet_name)
    records = ws.get_all_records()

    snapshot = {}
    for r in records:
        key = r.get("snapshot_key", "")
        data = r.get("data_json", "{}")
        if key:
            snapshot[key] = json.loads(data) if data else {}
    return snapshot


def save_snapshot(spreadsheet: gspread.Spreadsheet, silo: str,
                  college_name: str, rows: list[dict]):
    """
    Overwrite snapshot for one college in the given silo sheet.
    Each data row becomes one sheet row with:
      snapshot_key | row_key | college_name | silo | data_json | updated_at
    """
    sheet_name = SHEET_PLACEMENT_SNAPSHOT if silo == "placement" else SHEET_RANKING_SNAPSHOT
    ws = spreadsheet.worksheet(sheet_name)

    all_records = ws.get_all_records()
    all_values = ws.get_all_values()

    # Build row_key for each data row
    def get_row_key(row: dict, silo: str) -> str:
        if silo == "placement":
            # Key = the "Particulars" column value
            return row.get("Particulars", str(list(row.values())[:1]))
        else:
            # Key = category + publisher_year
            return f"{row.get('category', '')}||{row.get('publisher_year', '')}"

    now = datetime.now().isoformat()
    prefix = college_name

    # Delete existing rows for this college
    if len(all_values) > 1:  # has data beyond header
        rows_to_keep = [all_values[0]]  # header
        for r in all_values[1:]:
            # r[2] is college_name column (0-indexed)
            if len(r) > 2 and r[2] != college_name:
                rows_to_keep.append(r)
        ws.clear()
        ws.append_rows(rows_to_keep)

    # Add headers if sheet is now empty
    current_vals = ws.get_all_values()
    if not current_vals:
        ws.append_row(["snapshot_key", "row_key", "college_name", "silo", "data_json", "updated_at"])

    # Append new snapshot rows
    new_rows = []
    for row in rows:
        row_key = get_row_key(row, silo)
        snap_key = _snapshot_key(college_name, row_key)
        new_rows.append([snap_key, row_key, college_name, silo, json.dumps(row), now])

    if new_rows:
        ws.append_rows(new_rows)


# ─────────────────────────────────────────────
# CHANGE LOG
# ─────────────────────────────────────────────

def log_changes(spreadsheet: gspread.Spreadsheet, changes: list[dict]):
    """Append detected changes to the change_log sheet."""
    if not changes:
        return
    ws = spreadsheet.worksheet(SHEET_CHANGE_LOG)
    rows = []
    for c in changes:
        rows.append([
            c.get("timestamp", datetime.now().isoformat()),
            c.get("college_name", ""),
            c.get("silo", ""),
            c.get("change_type", ""),
            c.get("row_key", ""),
            c.get("old_value", ""),
            c.get("new_value", "")
        ])
    ws.append_rows(rows)
    print(f"  Logged {len(rows)} change(s) to change_log sheet")


# ─────────────────────────────────────────────
# UPDATE last_scraped / last_changed
# ─────────────────────────────────────────────

def update_college_timestamps(spreadsheet: gspread.Spreadsheet,
                               college_name: str,
                               has_changes: bool):
    """Update last_scraped (always) and last_changed (if changes detected)."""
    ws = spreadsheet.worksheet(SHEET_COLLEGES)
    records = ws.get_all_records()
    headers = ws.row_values(1)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, r in enumerate(records, start=2):  # 1-indexed, row 1 = headers
        if r.get("college_name") == college_name:
            # Update last_scraped
            if "last_scraped" in headers:
                col = headers.index("last_scraped") + 1
                ws.update_cell(i, col, now)
            # Update last_changed only if changes found
            if has_changes and "last_changed" in headers:
                col = headers.index("last_changed") + 1
                ws.update_cell(i, col, now)
            break
