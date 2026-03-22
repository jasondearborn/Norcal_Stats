"""
Retroactively dismiss duplicate_candidates that are age-constraint impossible.

Run once now, and again after any future `load_db.py --reset` reload.

Usage:
    python cleanup_candidates.py           # apply dismissals
    python cleanup_candidates.py --dry-run # preview without writing
"""

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from load_db import is_impossible_candidate

DB_PATH = Path(__file__).parent / "norcal_stats.db"

FETCH_QUERY = """
SELECT
    dc.id,
    dc.source_a_type                                       AS type_a,
    CASE dc.source_a_type
        WHEN 'player' THEN diva.age_group
        ELSE           divga.age_group
    END                                                    AS age_group_a,
    CASE dc.source_a_type
        WHEN 'player' THEN sa.end_year
        ELSE           sga.end_year
    END                                                    AS end_year_a,
    dc.source_b_type                                       AS type_b,
    CASE dc.source_b_type
        WHEN 'player' THEN divb.age_group
        ELSE           divgb.age_group
    END                                                    AS age_group_b,
    CASE dc.source_b_type
        WHEN 'player' THEN sb.end_year
        ELSE           sgb.end_year
    END                                                    AS end_year_b
FROM duplicate_candidates dc
-- source A
LEFT JOIN player_stats  psa  ON dc.source_a_type = 'player' AND dc.source_a_id = psa.id
LEFT JOIN goalie_stats  gsa  ON dc.source_a_type = 'goalie' AND dc.source_a_id = gsa.id
LEFT JOIN divisions     diva ON dc.source_a_type = 'player' AND psa.division_id = diva.id
LEFT JOIN divisions     divga ON dc.source_a_type = 'goalie' AND gsa.division_id = divga.id
LEFT JOIN seasons       sa   ON dc.source_a_type = 'player' AND psa.season_id = sa.id
LEFT JOIN seasons       sga  ON dc.source_a_type = 'goalie' AND gsa.season_id = sga.id
-- source B
LEFT JOIN player_stats  psb  ON dc.source_b_type = 'player' AND dc.source_b_id = psb.id
LEFT JOIN goalie_stats  gsb  ON dc.source_b_type = 'goalie' AND dc.source_b_id = gsb.id
LEFT JOIN divisions     divb ON dc.source_b_type = 'player' AND psb.division_id = divb.id
LEFT JOIN divisions     divgb ON dc.source_b_type = 'goalie' AND gsb.division_id = divgb.id
LEFT JOIN seasons       sb   ON dc.source_b_type = 'player' AND psb.season_id = sb.id
LEFT JOIN seasons       sgb  ON dc.source_b_type = 'goalie' AND gsb.season_id = sgb.id
WHERE dc.status = 'pending'
"""


def main():
    parser = argparse.ArgumentParser(description="Auto-dismiss impossible duplicate candidates")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")

    rows = db.execute(FETCH_QUERY).fetchall()
    total = len(rows)

    impossible_ids = [
        row["id"]
        for row in rows
        if is_impossible_candidate(
            row["type_a"], row["age_group_a"], row["end_year_a"],
            row["type_b"], row["age_group_b"], row["end_year_b"],
        )
    ]

    n_dismissed = len(impossible_ids)
    n_remaining = total - n_dismissed

    if args.dry_run:
        print(f"[dry-run] Checked {total} pending candidates.")
        print(f"[dry-run] Would auto-dismiss {n_dismissed} impossible candidates.")
        print(f"[dry-run] {n_remaining} would remain pending for manual review.")
    else:
        if impossible_ids:
            resolved_at = datetime.now(timezone.utc).isoformat()
            placeholders = ",".join("?" * len(impossible_ids))
            with db:
                db.execute(
                    f"""
                    UPDATE duplicate_candidates
                    SET status = 'confirmed_different', resolved_at = ?
                    WHERE id IN ({placeholders})
                    """,
                    [resolved_at] + impossible_ids,
                )
        print(f"Checked {total} pending candidates.")
        print(f"Auto-dismissed {n_dismissed} impossible candidates.")
        print(f"{n_remaining} remain pending for manual review.")

    db.close()


if __name__ == "__main__":
    main()
