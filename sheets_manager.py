"""
sheets_manager.py — Google Sheets read/write for College Monitor
"""

import os
import time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SHEET_COLLEGES              = "colleges"
SHEET_PLACEMENT_SNAPSHOT    = "placement_snapshot"
SHEET_RANKING_SNAPSHOT      = "ranking_snapshot"
SHEET_RANK_PUBLISHER        = "rank_publisher_snapshot"
SHEET_CHANGE_LOG            = "change_log"

COLLEGES_HEADERS            = ["college_name","campus","placement_url","ranking_url","active","rank_threshold","last_scraped","last_changed"]
PLACEMENT_SNAPSHOT_HEADERS  = ["snapshot_key","college_name","campus","particulars","statistics_2023","statistics_2024","statistics_2025","updated_at"]
RANKING_SNAPSHOT_HEADERS    = ["snapshot_key","college_name","campus","category","rank_2023","rank_2024","rank_2025","updated_at"]
RANK_PUBLISHER_HEADERS      = ["snapshot_key","college_name","campus","category","publisher","year","rank","updated_at"]
CHANGE_LOG_HEADERS          = ["Date / Time","College","Campus","Silo","Change Type","What Changed","Old Value","New Value"]

# ── Design tokens ──────────────────────────────────────────
HEADER_BG      = {"red": 0.07, "green": 0.13, "blue": 0.25}   # dark navy
HEADER_FG      = {"red": 1.0,  "green": 1.0,  "blue": 1.0}    # white
COLLEGE_BG     = {"red": 0.82, "green": 0.71, "blue": 0.27}   # gold  — university row
COLLEGE_FG     = {"red": 0.07, "green": 0.13, "blue": 0.25}   # navy text
CAMPUS_BG      = {"red": 0.95, "green": 0.88, "blue": 0.60}   # soft gold — campus row
CAMPUS_FG      = {"red": 0.20, "green": 0.20, "blue": 0.20}   # dark grey text
ALT_ROW_BG     = {"red": 0.95, "green": 0.96, "blue": 0.98}   # light blue-grey
WHITE          = {"red": 1.0,  "green": 1.0,  "blue": 1.0}
CHANGE_ADD_BG  = {"red": 0.85, "green": 0.95, "blue": 0.85}
CHANGE_MOD_BG  = {"red": 0.99, "green": 0.95, "blue": 0.80}
CHANGE_REM_BG  = {"red": 0.99, "green": 0.87, "blue": 0.87}
CHANGE_THR_BG  = {"red": 0.87, "green": 0.87, "blue": 0.99}


def _safe(fn, retries=3, delay=12):
    for attempt in range(retries):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < retries - 1:
                wait = delay * (attempt + 1)
                print(f"  [Rate limit] Waiting {wait}s...")
                time.sleep(wait)
            else:
                raise


def get_client():
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


def open_spreadsheet(spreadsheet_id: str):
    return get_client().open_by_key(spreadsheet_id)


def _col(n: int) -> str:
    return chr(64 + n)


def _fmt_header(ws, n: int):
    _safe(lambda: ws.batch_format([{
        "range": f"A1:{_col(n)}1",
        "format": {
            "backgroundColor": HEADER_BG,
            "textFormat": {"bold": True, "foregroundColor": HEADER_FG, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        }
    }]))


def ensure_sheets(spreadsheet):
    existing = {ws.title for ws in spreadsheet.worksheets()}
    configs = {
        SHEET_COLLEGES:           COLLEGES_HEADERS,
        SHEET_PLACEMENT_SNAPSHOT: PLACEMENT_SNAPSHOT_HEADERS,
        SHEET_RANKING_SNAPSHOT:   RANKING_SNAPSHOT_HEADERS,
        SHEET_RANK_PUBLISHER:     RANK_PUBLISHER_HEADERS,
        SHEET_CHANGE_LOG:         CHANGE_LOG_HEADERS,
    }
    for name, headers in configs.items():
        if name not in existing:
            ws = spreadsheet.add_worksheet(title=name, rows=1000, cols=len(headers) + 2)
            _safe(lambda ws=ws, h=headers: ws.update("A1", [h]))
            _fmt_header(ws, len(headers))
            print(f"  Created: {name}")
        else:
            ws = spreadsheet.worksheet(name)
            current = ws.get_all_values()
            if not current or current[0] != headers:
                clean = [r for r in (current[1:] if current else [])
                         if r and r[0] != headers[0]]
                _safe(lambda ws=ws: ws.clear())
                _safe(lambda ws=ws, h=headers, c=clean: ws.update("A1", [h] + c))
                _fmt_header(ws, len(headers))
                print(f"  Fixed headers: {name}")
        time.sleep(1)


# ─────────────────────────────────────────────
# COLLEGES
# ─────────────────────────────────────────────

def get_active_colleges(spreadsheet) -> list[dict]:
    ws = spreadsheet.worksheet(SHEET_COLLEGES)
    return [
        r for r in ws.get_all_records()
        if str(r.get("active", "TRUE")).upper() in ("TRUE", "1", "YES")
    ]


# ─────────────────────────────────────────────
# ROW KEY HELPERS
# ─────────────────────────────────────────────

def _get_row_key(row: dict, silo: str) -> str:
    if silo == "placement":
        return row.get("Particulars", str(list(row.values())[0]))
    elif silo == "ranking":
        return row.get("Category", str(list(row.values())[0]))
    else:  # rank_publisher
        cat = row.get("Category", "")
        pub = row.get("Publisher", "")
        yr  = row.get("Year", "")
        return f"{cat}|{pub}|{yr}"


def _snap_key(college_name: str, campus: str, row_key: str) -> str:
    tag = f"{college_name}({campus})" if campus else college_name
    return f"{tag}|||{row_key}"


# ─────────────────────────────────────────────
# SNAPSHOT READ
# ─────────────────────────────────────────────

def load_snapshot(spreadsheet, silo: str, college_name: str, campus: str) -> dict:
    sheet_map = {
        "placement":      SHEET_PLACEMENT_SNAPSHOT,
        "ranking":        SHEET_RANKING_SNAPSHOT,
        "rank_publisher": SHEET_RANK_PUBLISHER,
    }
    ws = spreadsheet.worksheet(sheet_map[silo])
    records = ws.get_all_records()
    tag = f"{college_name}({campus})" if campus else college_name
    snapshot = {}

    for r in records:
        key = r.get("snapshot_key", "")
        if not key or "|||" not in key or not key.startswith(tag):
            continue
        if silo == "placement":
            row_dict = {
                "Particulars":       str(r.get("particulars", "")),
                "Statistics (2023)": str(r.get("statistics_2023", "")),
                "Statistics (2024)": str(r.get("statistics_2024", "")),
                "Statistics (2025)": str(r.get("statistics_2025", "")),
            }
        elif silo == "ranking":
            row_dict = {
                "Category": str(r.get("category", "")),
                "2023":     str(r.get("rank_2023", "")),
                "2024":     str(r.get("rank_2024", "")),
                "2025":     str(r.get("rank_2025", "")),
            }
        else:  # rank_publisher
            row_dict = {
                "Category":  str(r.get("category", "")),
                "Publisher": str(r.get("publisher", "")),
                "Year":      str(r.get("year", "")),
                "Rank":      str(r.get("rank", "")),
            }
        snapshot[key] = row_dict
    return snapshot


# ─────────────────────────────────────────────
# SNAPSHOT WRITE — with university + campus grouping
# ─────────────────────────────────────────────

def _build_data_row(row: dict, silo: str, snap_key: str,
                    college_name: str, campus: str, now: str) -> list:
    if silo == "placement":
        return [snap_key, college_name, campus,
                row.get("Particulars", ""),
                row.get("Statistics (2023)", ""),
                row.get("Statistics (2024)", ""),
                row.get("Statistics (2025)", ""),
                now]
    elif silo == "ranking":
        return [snap_key, college_name, campus,
                row.get("Category", ""),
                row.get("2023", ""), row.get("2024", ""), row.get("2025", ""),
                now]
    else:  # rank_publisher
        return [snap_key, college_name, campus,
                row.get("Category", ""),
                row.get("Publisher", ""),
                row.get("Year", ""),
                row.get("Rank", ""),
                now]


def _apply_snapshot_formats(ws, row_meta: list[tuple], num_cols: int):
    """Single batch_format call covering header + all content rows."""
    col = _col(num_cols)
    formats = [{
        "range": f"A1:{col}1",
        "format": {
            "backgroundColor": HEADER_BG,
            "textFormat": {"bold": True, "foregroundColor": HEADER_FG, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        }
    }]
    for row_idx, row_type in row_meta:
        r = f"A{row_idx}:{col}{row_idx}"
        if row_type == "university":
            formats.append({"range": r, "format": {
                "backgroundColor": COLLEGE_BG,
                "textFormat": {"bold": True, "foregroundColor": COLLEGE_FG, "fontSize": 10},
                "horizontalAlignment": "CENTER"
            }})
        elif row_type == "campus":
            formats.append({"range": r, "format": {
                "backgroundColor": CAMPUS_BG,
                "textFormat": {"bold": True, "foregroundColor": CAMPUS_FG,
                               "fontSize": 9, "italic": True},
                "horizontalAlignment": "LEFT"
            }})
        elif row_type == "data_even":
            formats.append({"range": r, "format": {
                "backgroundColor": ALT_ROW_BG,
                "textFormat": {"fontSize": 10}
            }})
        elif row_type == "data_odd":
            formats.append({"range": r, "format": {
                "backgroundColor": WHITE,
                "textFormat": {"fontSize": 10}
            }})
    _safe(lambda: ws.batch_format(formats))


def save_snapshot(spreadsheet, silo: str,
                  college_name: str, campus: str, rows: list[dict]):
    sheet_map = {
        "placement":      (SHEET_PLACEMENT_SNAPSHOT, PLACEMENT_SNAPSHOT_HEADERS),
        "ranking":        (SHEET_RANKING_SNAPSHOT,   RANKING_SNAPSHOT_HEADERS),
        "rank_publisher": (SHEET_RANK_PUBLISHER,     RANK_PUBLISHER_HEADERS),
    }
    sheet_name, headers = sheet_map[silo]
    ws = spreadsheet.worksheet(sheet_name)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    num_cols = len(headers)
    tag = f"{college_name}({campus})" if campus else college_name

    # ── Load existing rows, drop this college+campus ──────
    all_values = _safe(lambda: ws.get_all_values())
    # Group remaining rows by (college_name, campus)
    kept: dict[tuple, list] = {}
    if all_values:
        for r in all_values[1:]:
            if not r or not any(r):
                continue
            if not r[0] or "|||" not in r[0]:
                continue  # skip label rows
            r_key = r[0]
            if r_key.startswith(f"{tag}|||"):
                continue  # drop current college+campus
            # Parse college/campus from snapshot_key
            prefix = r_key.split("|||")[0]
            if "(" in prefix and prefix.endswith(")"):
                c_name  = prefix[:prefix.rfind("(")]
                c_campus = prefix[prefix.rfind("(")+1:-1]
            else:
                c_name   = prefix
                c_campus = ""
            kept.setdefault((c_name, c_campus), []).append(r)

    # ── Build new rows for this college+campus ────────────
    new_rows = []
    for row in rows:
        row_key  = _get_row_key(row, silo)
        snap_key = _snap_key(college_name, campus, row_key)
        new_rows.append(_build_data_row(row, silo, snap_key, college_name, campus, now))

    kept[(college_name, campus)] = new_rows

    # ── Group by university name for display ──────────────
    # university → { campus → [rows] }
    uni_groups: dict[str, dict[str, list]] = {}
    for (cname, ccampus), crows in kept.items():
        uni_groups.setdefault(cname, {})[ccampus] = crows

    # ── Build full write array ────────────────────────────
    write_rows = [headers]
    row_meta   = []
    cur = 2  # row 1 = header

    for u_idx, (uni_name, campus_map) in enumerate(uni_groups.items()):
        # Blank separator before each university (except first)
        if u_idx > 0:
            write_rows.append([""] * num_cols)
            cur += 1

        # University header row
        write_rows.append([uni_name.upper()] + [""] * (num_cols - 1))
        row_meta.append((cur, "university"))
        cur += 1

        for c_idx, (c_name, c_rows) in enumerate(campus_map.items()):
            # Campus sub-header (only if campus is non-empty)
            if c_name:
                write_rows.append([f"  📍 {c_name}"] + [""] * (num_cols - 1))
                row_meta.append((cur, "campus"))
                cur += 1

            for j, dr in enumerate(c_rows):
                write_rows.append(dr)
                row_meta.append((cur, "data_even" if j % 2 == 0 else "data_odd"))
                cur += 1

    # ── Write + format ────────────────────────────────────
    _safe(lambda: ws.clear())
    time.sleep(2)
    _safe(lambda: ws.update("A1", write_rows))
    time.sleep(2)
    _apply_snapshot_formats(ws, row_meta, num_cols)

    print(f"  Snapshot saved: {silo} — {college_name} ({campus or 'no campus'}) [{len(new_rows)} rows]")


# ─────────────────────────────────────────────
# CHANGE LOG
# ─────────────────────────────────────────────

def _change_bg(ct: str) -> dict:
    if "added"     in ct: return CHANGE_ADD_BG
    if "removed"   in ct: return CHANGE_REM_BG
    if "threshold" in ct: return CHANGE_THR_BG
    return CHANGE_MOD_BG


def _change_label(ct: str) -> str:
    return {
        "row_added":               "➕  New Data",
        "row_removed":             "➖  Removed",
        "value_changed":           "✏️  Updated",
        "rank_threshold_improved": "📈  Rank Up",
        "rank_threshold_dropped":  "📉  Rank Down",
    }.get(ct, ct)


def _ensure_change_log_header(ws):
    all_vals = _safe(lambda: ws.get_all_values())
    n = len(CHANGE_LOG_HEADERS)
    header_rows = [i for i, r in enumerate(all_vals) if r == CHANGE_LOG_HEADERS]
    if len(header_rows) != 1 or (header_rows and header_rows[0] != 0):
        data_rows = [r for r in all_vals if r != CHANGE_LOG_HEADERS and any(r)]
        _safe(lambda: ws.clear())
        time.sleep(1)
        _safe(lambda: ws.update("A1", [CHANGE_LOG_HEADERS] + data_rows))
        time.sleep(1)
        all_vals = [CHANGE_LOG_HEADERS] + data_rows
    _safe(lambda: ws.batch_format([{
        "range": f"A1:{_col(n)}1",
        "format": {
            "backgroundColor": HEADER_BG,
            "textFormat": {"bold": True, "foregroundColor": HEADER_FG, "fontSize": 11},
            "horizontalAlignment": "CENTER",
        }
    }]))
    return all_vals


def log_changes(spreadsheet, changes: list[dict]):
    if not changes:
        return
    ws = spreadsheet.worksheet(SHEET_CHANGE_LOG)
    n   = len(CHANGE_LOG_HEADERS)
    col = _col(n)

    all_vals  = _ensure_change_log_header(ws)
    start_row = len(all_vals) + 1

    new_rows = []
    for c in changes:
        new_rows.append([
            c.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            c.get("college_name", ""),
            c.get("campus", ""),
            c.get("silo", "").replace("_", " ").title(),
            _change_label(c.get("change_type", "")),
            c.get("row_key", "").split("||")[0],
            c.get("old_value", "—"),
            c.get("new_value", "—")
        ])

    _safe(lambda: ws.append_rows(new_rows))
    time.sleep(2)

    formats = []
    for i, c in enumerate(changes):
        ri = start_row + i
        bg = _change_bg(c.get("change_type", ""))
        formats += [
            {"range": f"A{ri}:{col}{ri}",
             "format": {"backgroundColor": bg, "textFormat": {"fontSize": 10},
                        "verticalAlignment": "MIDDLE"}},
            {"range": f"B{ri}",
             "format": {"textFormat": {"bold": True, "fontSize": 10}}},
            {"range": f"D{ri}",
             "format": {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 10}}},
            {"range": f"E{ri}",
             "format": {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 10}}},
        ]
    if formats:
        _safe(lambda: ws.batch_format(formats))

    # Set column widths (cosmetic, safe to fail)
    try:
        ws.spreadsheet.batch_update({"requests": [
            {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i+1},
             "properties": {"pixelSize": w}, "fields": "pixelSize"}}
            for i, w in enumerate([155, 140, 100, 110, 130, 180, 220, 220])
        ]})
    except Exception:
        pass

    print(f"  Logged {len(new_rows)} change(s) to change_log")


# ─────────────────────────────────────────────
# TIMESTAMPS
# ─────────────────────────────────────────────

def update_college_timestamps(spreadsheet, college_name: str,
                               campus: str, has_changes: bool):
    ws = spreadsheet.worksheet(SHEET_COLLEGES)
    headers = ws.row_values(1)
    records = ws.get_all_records()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for i, r in enumerate(records, start=2):
        if r.get("college_name") == college_name and str(r.get("campus","")) == str(campus):
            if "last_scraped" in headers:
                _safe(lambda: ws.update_cell(i, headers.index("last_scraped") + 1, now))
            if has_changes and "last_changed" in headers:
                time.sleep(1)
                _safe(lambda: ws.update_cell(i, headers.index("last_changed") + 1, now))
            break
