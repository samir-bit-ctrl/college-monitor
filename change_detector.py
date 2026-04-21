"""
change_detector.py — Compares freshly scraped data against stored snapshots.

Detects:
  - Value changed in existing row
  - New row added
  - Row removed
  - Rank threshold breach (for ranking table)
"""

from datetime import datetime


def _placement_row_key(row: dict) -> str:
    """Use 'Particulars' column as the unique key for placement rows."""
    for key in row:
        if "particular" in key.lower():
            return row[key]
    return str(list(row.values())[0]) if row else ""


def _ranking_row_key(row: dict) -> str:
    """Use category + publisher_year as unique key for ranking rows."""
    return f"{row.get('category', '')}||{row.get('publisher_year', '')}"


def detect_changes(college_name: str,
                   silo: str,
                   old_snapshot: dict,
                   new_rows: list[dict],
                   rank_threshold: int = 0) -> list[dict]:
    """
    Compare new_rows against old_snapshot and return a list of change dicts.

    old_snapshot: dict of { snapshot_key → row_dict }
    new_rows: list of freshly scraped row dicts
    rank_threshold: if > 0, alert if rank number changes by more than this value
    """
    changes = []
    now = datetime.now().isoformat()

    def make_snap_key(row_key):
        return f"{college_name}|||{row_key}"

    def get_row_key(row):
        return _placement_row_key(row) if silo == "placement" else _ranking_row_key(row)

    # Build lookup of new data
    new_lookup = {}
    for row in new_rows:
        rk = get_row_key(row)
        if rk:
            new_lookup[rk] = row

    # Build lookup of old data
    old_lookup = {}
    for snap_key, row_dict in old_snapshot.items():
        if snap_key.startswith(f"{college_name}|||"):
            rk = snap_key.split("|||", 1)[1]
            old_lookup[rk] = row_dict

    # ── Detect: value changes + new rows ──────────────────────────
    for row_key, new_row in new_lookup.items():
        if row_key not in old_lookup:
            # New row added
            changes.append({
                "timestamp": now,
                "college_name": college_name,
                "silo": silo,
                "change_type": "row_added",
                "row_key": row_key,
                "old_value": "",
                "new_value": str(new_row)
            })
        else:
            old_row = old_lookup[row_key]
            # Compare each cell value
            all_keys = set(list(new_row.keys()) + list(old_row.keys()))
            for col in all_keys:
                if col in ("rank_value",):
                    continue  # internal field, skip
                old_val = str(old_row.get(col, "")).strip()
                new_val = str(new_row.get(col, "")).strip()
                if old_val != new_val:
                    changes.append({
                        "timestamp": now,
                        "college_name": college_name,
                        "silo": silo,
                        "change_type": "value_changed",
                        "row_key": row_key,
                        "old_value": f"{col}: {old_val}",
                        "new_value": f"{col}: {new_val}"
                    })

            # ── Rank threshold check ─────────────────────────────
            if silo == "ranking" and rank_threshold > 0:
                try:
                    old_rank = int(old_row.get("rank_value", "0") or "0")
                    new_rank = int(new_row.get("rank_value", "0") or "0")
                    if old_rank > 0 and new_rank > 0:
                        delta = abs(new_rank - old_rank)
                        if delta >= rank_threshold:
                            direction = "improved" if new_rank < old_rank else "dropped"
                            changes.append({
                                "timestamp": now,
                                "college_name": college_name,
                                "silo": silo,
                                "change_type": f"rank_threshold_{direction}",
                                "row_key": row_key,
                                "old_value": f"#{old_rank}",
                                "new_value": f"#{new_rank} (Δ{delta})"
                            })
                except (ValueError, TypeError):
                    pass

    # ── Detect: removed rows ──────────────────────────────────────
    for row_key in old_lookup:
        if row_key not in new_lookup:
            changes.append({
                "timestamp": now,
                "college_name": college_name,
                "silo": silo,
                "change_type": "row_removed",
                "row_key": row_key,
                "old_value": str(old_lookup[row_key]),
                "new_value": ""
            })

    return changes


def format_changes_for_alert(changes: list[dict]) -> str:
    """Format changes into a clean human-readable summary string."""
    if not changes:
        return ""

    lines = []
    for c in changes:
        ct = c["change_type"]
        rk = c["row_key"]

        if ct == "row_added":
            lines.append(f"  ➕ New row added: {rk}")
            if c["new_value"]:
                lines.append(f"     {c['new_value']}")
        elif ct == "row_removed":
            lines.append(f"  ➖ Row removed: {rk}")
        elif ct == "value_changed":
            lines.append(f"  ✏️  Changed [{rk}]")
            lines.append(f"     Old: {c['old_value']}")
            lines.append(f"     New: {c['new_value']}")
        elif ct.startswith("rank_threshold"):
            direction = "📈 Improved" if "improved" in ct else "📉 Dropped"
            lines.append(f"  {direction} [{rk}]: {c['old_value']} → {c['new_value']}")

    return "\n".join(lines)
