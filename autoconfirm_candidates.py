"""
Auto-confirm duplicate_candidates with similarity_score = 100.

An exact name match within the same age group across different seasons is
virtually certain to be the same person. This script:

  1. Loads all pending score-100 pairs.
  2. Groups them into connected components via union-find (a player who
     appears in 5 seasons generates 10 pairs; all 5 records must link to
     the same people row).
  3. For each component, creates one people record (or reuses an existing
     one if any record was already reconciled) and links every stat row.
  4. Marks the candidate rows confirmed_same.

Usage:
    python autoconfirm_candidates.py [--dry-run]
"""

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "norcal_stats.db"


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------
class UnionFind:
    def __init__(self):
        self._parent: dict = {}

    def find(self, x):
        self._parent.setdefault(x, x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra

    def components(self) -> dict:
        """Return {root: [members]} for all tracked nodes."""
        result: dict = {}
        for node in self._parent:
            root = self.find(node)
            result.setdefault(root, []).append(node)
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fetch_person_id(db, node_type, node_id):
    table = "player_stats" if node_type == "player" else "goalie_stats"
    row = db.execute(f"SELECT person_id FROM {table} WHERE id = ?", (node_id,)).fetchone()
    return row["person_id"] if row else None


def fetch_raw_name(db, node_type, node_id):
    table = "player_stats" if node_type == "player" else "goalie_stats"
    row = db.execute(f"SELECT raw_name FROM {table} WHERE id = ?", (node_id,)).fetchone()
    return row["raw_name"] if row else None


def set_person_id(db, node_type, node_id, person_id):
    table = "player_stats" if node_type == "player" else "goalie_stats"
    db.execute(f"UPDATE {table} SET person_id = ? WHERE id = ?", (person_id, node_id))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Auto-confirm score-100 duplicate candidates")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")

    # Load all pending score-100 pairs
    pairs = db.execute(
        """
        SELECT id, source_a_type, source_a_id, source_b_type, source_b_id
        FROM duplicate_candidates
        WHERE status = 'pending' AND similarity_score = 100
        """
    ).fetchall()

    if not pairs:
        print("No pending score-100 candidates found.")
        db.close()
        return

    # Build union-find over (type, id) nodes
    uf = UnionFind()
    candidate_ids = [row["id"] for row in pairs]
    for row in pairs:
        a = (row["source_a_type"], row["source_a_id"])
        b = (row["source_b_type"], row["source_b_id"])
        uf.union(a, b)

    components = uf.components()
    n_components = len(components)
    n_pairs = len(pairs)

    print(f"Found {n_pairs} pending score-100 candidates → {n_components} unique people.")

    if args.dry_run:
        print(f"[dry-run] Would create up to {n_components} people records and link stat rows.")
        db.close()
        return

    resolved_at = datetime.now(timezone.utc).isoformat()
    n_created = 0
    n_reused = 0

    with db:
        for members in components.values():
            # Find existing person_id among all members, if any
            existing_pid = None
            canonical_name = None
            for node_type, node_id in members:
                pid = fetch_person_id(db, node_type, node_id)
                if pid is not None:
                    existing_pid = pid
                    break
                if canonical_name is None:
                    canonical_name = fetch_raw_name(db, node_type, node_id)

            if existing_pid is not None:
                person_id = existing_pid
                n_reused += 1
            else:
                cur = db.execute(
                    "INSERT INTO people (canonical_name) VALUES (?)", (canonical_name,)
                )
                person_id = cur.lastrowid
                n_created += 1

            # Link all stat rows in this component
            for node_type, node_id in members:
                set_person_id(db, node_type, node_id, person_id)

        # Mark all candidate rows confirmed_same
        placeholders = ",".join("?" * len(candidate_ids))
        db.execute(
            f"""
            UPDATE duplicate_candidates
            SET status = 'confirmed_same', resolved_at = ?
            WHERE id IN ({placeholders})
            """,
            [resolved_at] + candidate_ids,
        )

    pending_left = db.execute(
        "SELECT COUNT(*) FROM duplicate_candidates WHERE status = 'pending'"
    ).fetchone()[0]

    print(f"Created {n_created} new people records, reused {n_reused}.")
    print(f"Linked stat rows and marked {n_pairs} candidates confirmed_same.")
    print(f"{pending_left} candidates remain pending for manual review.")
    db.close()


if __name__ == "__main__":
    main()
