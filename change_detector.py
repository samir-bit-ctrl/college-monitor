"""
change_detector.py — Compares scraped data against stored snapshots.
All old_value / new_value fields are human-readable strings, no raw dicts.
"""

from datetime import datetime


def _placement_row_key(row: dict) -> str:
    return row.get("Particulars", str(list(row.values())[0]) if row else "")


def _ranking_row_key(row: dict) -> str:
    return row.get("Category", str(list(row.values())[0]) if row else "")

def _rank_publisher_row_key(row: dict) -> str:
    cat = row.get("Category", "").strip()
    pub = row.get("Publisher", "").strip()
    yr  = row.get("Year", "").strip()
    return f"{cat}||{pub}||{yr}"


def _summarise_row(row: dict, silo: str) -> str:
    """Turn a row dict into a compact human-readable string."""
    if silo == "placement":
        parts = [
            f"{k.replace('Statistics ', '')}: {v}"
            for k, v in row.items()
            if k != "Particulars" and v and k != "rank_value"
        ]
        return " | ".join(parts)
    else:
        parts = [
            f"{k}: #{v}"
            for k, v in row.items()
            if k not in ("Category", "rank_value") and v
        ]
        return " | ".join(parts)


def detect_changes(college_name: str,
                   silo: str,
                   campus: str = "",
                   old_snapshot: dict = None,
                   new_rows: list[dict] = None,
                   rank_threshold: int = 0) -> list[dict]:
    if old_snapshot is None: old_snapshot = {}
    if new_rows is None: new_rows = []
    changes = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_row_key(row):
        if silo == "placement":
            return _placement_row_key(row)
        elif silo == "rank_publisher":
            return _rank_publisher_row_key(row)
        else:
            return _ranking_row_key(row)

    # Build lookups
    new_lookup = {get_row_key(r): r for r in new_rows if get_row_key(r)}
    old_lookup = {}
    for snap_key, row_dict in old_snapshot.items():
        if snap_key.startswith(f"{college_name}|||"):
            rk = snap_key.split("|||", 1)[1]
            old_lookup[rk] = row_dict

    # ── New rows ──────────────────────────────────────────
    for row_key, new_row in new_lookup.items():
        if row_key not in old_lookup:
            changes.append({
                "timestamp":    now,
                "college_name": college_name,
                "campus":       campus,
                "silo":         silo,
                "change_type":  "row_added",
                "row_key":      row_key,
                "old_value":    "—",
                "new_value":    _summarise_row(new_row, silo)
            })

        else:
            old_row = old_lookup[row_key]
            all_keys = set(list(new_row.keys()) + list(old_row.keys()))
            for col in all_keys:
                if col == "rank_value":
                    continue
                old_val = str(old_row.get(col, "")).strip()
                new_val = str(new_row.get(col, "")).strip()
                if old_val != new_val:
                    # Clean column label
                    col_label = col.replace("Statistics ", "").strip("()")
                    changes.append({
                        "timestamp":    now,
                        "college_name": college_name,
                "campus":       campus,
                        "silo":         silo,
                        "change_type":  "value_changed",
                        "row_key":      row_key,
                        "old_value":    f"{col_label}: {old_val}" if old_val else "—",
                        "new_value":    f"{col_label}: {new_val}" if new_val else "—"
                    })

            # ── Rank threshold ────────────────────────────
            if silo == "ranking" and rank_threshold > 0:
                try:
                    old_rank = int(old_row.get("rank_value", "0") or "0")
                    new_rank = int(new_row.get("rank_value", "0") or "0")
                    if old_rank > 0 and new_rank > 0:
                        delta = abs(new_rank - old_rank)
                        if delta >= rank_threshold:
                            direction = "improved" if new_rank < old_rank else "dropped"
                            changes.append({
                                "timestamp":    now,
                                "college_name": college_name,
                "campus":       campus,
                                "silo":         silo,
                                "change_type":  f"rank_threshold_{direction}",
                                "row_key":      row_key,
                                "old_value":    f"#{old_rank}",
                                "new_value":    f"#{new_rank}  (Δ {delta} places)"
                            })
                except (ValueError, TypeError):
                    pass

    # ── Removed rows ──────────────────────────────────────
    for row_key in old_lookup:
        if row_key not in new_lookup:
            changes.append({
                "timestamp":    now,
                "college_name": college_name,
                "campus":       campus,
                "silo":         silo,
                "change_type":  "row_removed",
                "row_key":      row_key,
                "old_value":    _summarise_row(old_lookup[row_key], silo),
                "new_value":    "—"
            })

    return changes
