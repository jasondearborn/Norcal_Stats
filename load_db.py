"""
ETL script for Norcal Stats hockey project.
Loads norcal_player_stats.csv and norcal_goalie_stats.csv into norcal_stats.db.
"""

import argparse
import os
import re
import sqlite3
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SIMILARITY_THRESHOLD = 91
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "norcal_stats.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"
PLAYER_CSV = BASE_DIR / "norcal_player_stats.csv"
GOALIE_CSV = BASE_DIR / "norcal_goalie_stats.csv"


# ---------------------------------------------------------------------------
# Division parsing
# ---------------------------------------------------------------------------
def parse_division(name: str) -> dict:
    """
    Parse a raw division name into (age_group, tier, program).

    Examples
    --------
    'Girls 10-U'  -> age_group='10U',  tier=None,  program='female'
    'Girls 14AAA' -> age_group='14U',  tier='AAA',  program='female'
    'Girls 19AA'  -> age_group='19U',  tier='AA',   program='female'
    'High School D1' -> age_group='HS', tier='D1', program='high_school'
    '16U AA East' -> age_group='16U',  tier='AA',   program='coed'
    '10U A'       -> age_group='10U',  tier='A',    program='coed'
    '14U B Flight I' -> age_group='14U', tier='B',  program='coed'
    """
    age_group = None
    tier = None
    program = "coed"

    # Girls prefix
    if name.startswith("Girls "):
        program = "female"
        rest = name[len("Girls "):].strip()
        # e.g. "10-U", "12-U"  ->  "10U"
        m = re.match(r"^(\d+)-U$", rest)
        if m:
            age_group = m.group(1) + "U"
            tier = None
            return dict(age_group=age_group, tier=tier, program=program)
        # e.g. "14AAA", "19AA"  ->  age_group="14U", tier="AAA"
        m = re.match(r"^(\d+)(A+)$", rest)
        if m:
            age_group = m.group(1) + "U"
            tier = m.group(2)
            return dict(age_group=age_group, tier=tier, program=program)
        # fallback: store rest as age_group
        age_group = rest
        return dict(age_group=age_group, tier=tier, program=program)

    # High School
    if name.startswith("High School"):
        program = "high_school"
        age_group = "HS"
        m = re.search(r"(D\d+)", name)
        if m:
            tier = m.group(1)
        return dict(age_group=age_group, tier=tier, program=program)

    # Coed: e.g. "16U AA East", "10U A", "14U B Flight I", "12U BB West"
    # Pattern: <age_group> <tier> [optional suffix words]
    m = re.match(r"^(\d+U)\s+(A{1,3}|B{1,2})((\s+(East|West|Flight\s+[IVX]+))*)?$", name, re.IGNORECASE)
    if m:
        age_group = m.group(1)
        tier = m.group(2).upper()
        return dict(age_group=age_group, tier=tier, program=program)

    # Fallback: store the whole name as age_group
    age_group = name
    return dict(age_group=age_group, tier=tier, program=program)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def apply_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    sql = schema_path.read_text()
    conn.executescript(sql)
    conn.commit()


def drop_all_tables(conn: sqlite3.Connection) -> None:
    tables = [
        "duplicate_candidates",
        "goalie_stats",
        "player_stats",
        "people",
        "team_registrations",
        "teams",
        "divisions",
        "seasons",
    ]
    conn.execute("PRAGMA foreign_keys = OFF")
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def safe_int(val, default=0) -> int:
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def safe_float(val, default=0.0) -> float:
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_text(val) -> str | None:
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s else None


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------
def seed_seasons(conn: sqlite3.Connection, season_names: list[str]) -> dict[str, int]:
    """Insert seasons and return {name: id} map."""
    print("  seeding seasons...")
    season_map: dict[str, int] = {}
    for name in sorted(set(season_names)):
        m = re.match(r"^(\d{4})-(\d{2})$", name)
        if m:
            start_year = int(m.group(1))
            # handle century boundary: "2099-00" would be 2100
            end_suffix = int(m.group(2))
            end_year = (start_year // 100) * 100 + end_suffix
            if end_year <= start_year:
                end_year += 100
        else:
            raise ValueError(f"Unexpected season format: {name!r}")
        conn.execute(
            "INSERT OR IGNORE INTO seasons (name, start_year, end_year) VALUES (?,?,?)",
            (name, start_year, end_year),
        )
    conn.commit()
    for row in conn.execute("SELECT id, name FROM seasons"):
        season_map[row["name"]] = row["id"]
    return season_map


def seed_divisions(conn: sqlite3.Connection, division_names: list[str]) -> dict[str, int]:
    """Insert divisions and return {name: id} map."""
    print("  seeding divisions...")
    div_map: dict[str, int] = {}
    for name in sorted(set(division_names)):
        parsed = parse_division(name)
        conn.execute(
            """
            INSERT OR IGNORE INTO divisions (name, age_group, tier, program)
            VALUES (?,?,?,?)
            """,
            (name, parsed["age_group"], parsed["tier"], parsed["program"]),
        )
    conn.commit()
    for row in conn.execute("SELECT id, name FROM divisions"):
        div_map[row["name"]] = row["id"]
    return div_map


def seed_teams(conn: sqlite3.Connection, team_names: list[str]) -> dict[str, int]:
    """Insert teams and return {name: id} map."""
    print("  seeding teams...")
    team_map: dict[str, int] = {}
    for name in sorted(set(team_names)):
        conn.execute("INSERT OR IGNORE INTO teams (name) VALUES (?)", (name,))
    conn.commit()
    for row in conn.execute("SELECT id, name FROM teams"):
        team_map[row["name"]] = row["id"]
    return team_map


def seed_team_registrations(
    conn: sqlite3.Connection,
    records: list[tuple[int, int, int]],
) -> None:
    """Insert (team_id, season_id, division_id) tuples into team_registrations."""
    print("  seeding team_registrations...")
    conn.executemany(
        """
        INSERT OR IGNORE INTO team_registrations (team_id, season_id, division_id)
        VALUES (?,?,?)
        """,
        records,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Stats loading
# ---------------------------------------------------------------------------
def load_player_stats(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    season_map: dict[str, int],
    div_map: dict[str, int],
    team_map: dict[str, int],
) -> None:
    print("  loading player_stats...")
    rows = []
    for _, row in df.iterrows():
        season_id = season_map[row["Season Name"]]
        division_id = div_map[row["Division"]]
        team_id = team_map[row["Team"]]
        raw_name = str(row["Name"]).strip()
        jersey_number = safe_text(row["#"])
        rows.append((
            season_id,
            division_id,
            raw_name,
            jersey_number,
            team_id,
            safe_int(row["GP"]),
            safe_int(row["Goals"]),
            safe_int(row["Ass."]),
            safe_int(row["Hat"]),
            safe_int(row["Min"]),
            safe_float(row["Pts/Game"]),
            safe_int(row["Pts"]),
        ))
    conn.executemany(
        """
        INSERT OR IGNORE INTO player_stats
            (season_id, division_id, raw_name, jersey_number, team_id,
             games_played, goals, assists, hat_tricks, penalty_minutes,
             points_per_game, points)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()


def load_goalie_stats(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    season_map: dict[str, int],
    div_map: dict[str, int],
    team_map: dict[str, int],
) -> None:
    print("  loading goalie_stats...")
    rows = []
    for _, row in df.iterrows():
        season_id = season_map[row["Season Name"]]
        division_id = div_map[row["Division"]]
        team_id = team_map[row["Team"]]
        raw_name = str(row["Name"]).strip()
        rows.append((
            season_id,
            division_id,
            raw_name,
            team_id,
            safe_int(row["GP"]),
            safe_int(row["Shots"]),
            safe_int(row["GA"]),
            safe_float(row["GAA"]),
            safe_float(row["Save %"]),
            safe_int(row["SO"]),
        ))
    conn.executemany(
        """
        INSERT OR IGNORE INTO goalie_stats
            (season_id, division_id, raw_name, team_id,
             games_played, shots_against, goals_against,
             goals_against_avg, save_percentage, shutouts)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Age-constraint impossibility check
# ---------------------------------------------------------------------------
def is_impossible_candidate(
    type_a: str, age_group_a: str | None, end_year_a: int,
    type_b: str, age_group_b: str | None, end_year_b: int,
) -> bool:
    """Return True if two records cannot possibly belong to the same person.

    Rules:
      1. Same age group, gap >= threshold: a player ages out after at most
         2-3 seasons in the same age group. Threshold = 2 for cross-type
         pairs (player↔goalie is less likely) and 3 for same-type.
      2. Age group regression: a player can't appear in a lower age group
         in a later season (you can't go back down once you've aged up).
      3. Cross-age-group time mismatch: only applied when both groups are
         below 14U. Skipped for 14U+ because those players can play up to
         16U/18U in the same or following season.

    HS divisions use a higher threshold of 5 (4-year school career).
    Mixed HS/numeric pairs are conservative — never auto-dismissed.
    Unknown age group formats are conservative — never auto-dismissed.
    """
    # Normalize so A is the earlier (or equal) season
    if end_year_a > end_year_b:
        type_a, age_group_a, end_year_a, type_b, age_group_b, end_year_b = (
            type_b, age_group_b, end_year_b, type_a, age_group_a, end_year_a
        )
    year_diff = end_year_b - end_year_a  # always >= 0
    cross_type = type_a != type_b

    # HS same-group: 4-year school career → gap >= 5 is impossible
    if age_group_a == "HS" and age_group_b == "HS":
        return year_diff >= 5
    # Mixed HS/numeric: conservative — don't auto-dismiss
    if age_group_a == "HS" or age_group_b == "HS":
        return False

    # Extract numeric age (e.g. "12U" → 12); unknown format → conservative
    try:
        a = int(age_group_a.rstrip("U"))
        b = int(age_group_b.rstrip("U"))
    except (ValueError, AttributeError):
        return False

    # Rule 1: same age group — gap >= threshold is impossible
    # Cross-type pairs use a tighter threshold (player↔goalie switching is uncommon)
    if a == b:
        threshold = 2 if cross_type else 3
        return year_diff >= threshold

    # Rule 2: age group regression in a later season is impossible
    # (only across different seasons; same-season cross-group is conservative)
    if year_diff > 0 and b < a:
        return True

    # Rule 3: cross-age-group time mismatch
    # Skipped if either group is >= 14U (14U+ players can play up to 16U/18U)
    if a < 14 and b < 14:
        expected = b - a  # calendar years to naturally age from group a to group b
        return year_diff < expected - 2 or year_diff > expected + 2

    return False


# ---------------------------------------------------------------------------
# Fuzzy duplicate detection
# ---------------------------------------------------------------------------
def build_group_dict(conn: sqlite3.Connection, table: str) -> dict[tuple, list[tuple]]:
    """
    Return {(age_group, program): [(id, raw_name, season_id, end_year), ...]}
    for the given stats table (player_stats or goalie_stats).
    end_year comes from the seasons table and is used for age-constraint checks.
    """
    query = f"""
        SELECT s.id, s.raw_name, s.season_id,
               d.age_group, d.program,
               seas.end_year
        FROM {table} s
        JOIN divisions d ON s.division_id = d.id
        JOIN seasons seas ON s.season_id = seas.id
    """
    groups: dict[tuple, list[tuple]] = {}
    for row in conn.execute(query):
        key = (row["age_group"], row["program"])
        groups.setdefault(key, []).append(
            (row["id"], row["raw_name"], row["season_id"], row["end_year"])
        )
    return groups


def insert_duplicate(
    conn: sqlite3.Connection,
    type_a: str,
    id_a: int,
    type_b: str,
    id_b: int,
    score: float,
) -> None:
    # Canonical ordering: ensure (type_a, id_a) <= (type_b, id_b)
    # to keep the UNIQUE constraint meaningful regardless of comparison order.
    # Order: player < goalie; within same type: lower id first.
    def sort_key(t, i):
        return (0 if t == "player" else 1, i)

    if sort_key(type_a, id_a) > sort_key(type_b, id_b):
        type_a, id_a, type_b, id_b = type_b, id_b, type_a, id_a

    conn.execute(
        """
        INSERT OR IGNORE INTO duplicate_candidates
            (source_a_type, source_a_id, source_b_type, source_b_id,
             similarity_score, status)
        VALUES (?,?,?,?,?,'pending')
        """,
        (type_a, id_a, type_b, id_b, score),
    )


def run_fuzzy_within(
    conn: sqlite3.Connection,
    groups: dict[tuple, list[tuple]],
    src_type: str,
) -> int:
    """Compare records within the same (age_group, program) group, different seasons."""
    count = 0
    for key, records in groups.items():
        age_group, _program = key
        n = len(records)
        for i in range(n):
            id_a, name_a, season_a, end_year_a = records[i]
            for j in range(i + 1, n):
                id_b, name_b, season_b, end_year_b = records[j]
                # Skip same-season comparisons
                if season_a == season_b:
                    continue
                # Skip age-constraint-impossible pairs
                if is_impossible_candidate(
                    src_type, age_group, end_year_a,
                    src_type, age_group, end_year_b,
                ):
                    continue
                score = fuzz.token_sort_ratio(name_a, name_b)
                if score >= SIMILARITY_THRESHOLD:
                    insert_duplicate(conn, src_type, id_a, src_type, id_b, score)
                    count += 1
    conn.commit()
    return count


def run_fuzzy_cross(
    conn: sqlite3.Connection,
    player_groups: dict[tuple, list[tuple]],
    goalie_groups: dict[tuple, list[tuple]],
) -> int:
    """Cross-compare player_stats names against goalie_stats names within same (age_group, program)."""
    count = 0
    all_keys = set(player_groups.keys()) | set(goalie_groups.keys())
    for key in all_keys:
        age_group, _program = key
        p_records = player_groups.get(key, [])
        g_records = goalie_groups.get(key, [])
        if not p_records or not g_records:
            continue
        for pid, pname, _pseason, pend_year in p_records:
            for gid, gname, _gseason, gend_year in g_records:
                # Allow same-season cross comparison (player and goalie can be same person)
                # Skip age-constraint-impossible pairs
                if is_impossible_candidate(
                    "player", age_group, pend_year,
                    "goalie", age_group, gend_year,
                ):
                    continue
                score = fuzz.token_sort_ratio(pname, gname)
                if score >= SIMILARITY_THRESHOLD:
                    insert_duplicate(conn, "player", pid, "goalie", gid, score)
                    count += 1
    conn.commit()
    return count


def run_fuzzy_detection(conn: sqlite3.Connection) -> None:
    print("  running fuzzy duplicate detection...")

    player_groups = build_group_dict(conn, "player_stats")
    goalie_groups = build_group_dict(conn, "goalie_stats")

    n_pp = run_fuzzy_within(conn, player_groups, "player")
    print(f"    player-player matches: {n_pp}")

    n_gg = run_fuzzy_within(conn, goalie_groups, "goalie")
    print(f"    goalie-goalie matches: {n_gg}")

    n_pg = run_fuzzy_cross(conn, player_groups, goalie_groups)
    print(f"    player-goalie matches: {n_pg}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(conn: sqlite3.Connection) -> None:
    tables = [
        "seasons",
        "divisions",
        "teams",
        "team_registrations",
        "player_stats",
        "goalie_stats",
        "people",
        "duplicate_candidates",
    ]
    print("\n--- Row counts ---")
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:<30} {cnt:>6}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Load Norcal Stats into SQLite")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate the database before loading",
    )
    args = parser.parse_args()

    if args.reset and DB_PATH.exists():
        print(f"--reset: removing {DB_PATH}")
        DB_PATH.unlink()

    print(f"Connecting to {DB_PATH}")
    conn = get_connection(DB_PATH)

    if args.reset:
        print("Dropping all tables...")
        drop_all_tables(conn)

    print("Applying schema...")
    apply_schema(conn, SCHEMA_PATH)

    # ---- Read CSVs ----
    print("Reading CSVs...")
    player_df = pd.read_csv(PLAYER_CSV, dtype={"#": str})
    goalie_df = pd.read_csv(GOALIE_CSV)

    all_season_names = list(player_df["Season Name"].unique()) + list(goalie_df["Season Name"].unique())
    all_division_names = list(player_df["Division"].unique()) + list(goalie_df["Division"].unique())
    all_team_names = list(player_df["Team"].unique()) + list(goalie_df["Team"].unique())

    # ---- Seed lookup tables ----
    season_map = seed_seasons(conn, all_season_names)
    div_map = seed_divisions(conn, all_division_names)
    team_map = seed_teams(conn, all_team_names)

    # Build team_registrations tuples
    reg_tuples: list[tuple[int, int, int]] = []
    for df in [player_df, goalie_df]:
        for _, row in df.iterrows():
            reg_tuples.append((
                team_map[row["Team"]],
                season_map[row["Season Name"]],
                div_map[row["Division"]],
            ))
    seed_team_registrations(conn, reg_tuples)

    # ---- Load stats ----
    load_player_stats(conn, player_df, season_map, div_map, team_map)
    load_goalie_stats(conn, goalie_df, season_map, div_map, team_map)

    # ---- Fuzzy detection ----
    run_fuzzy_detection(conn)

    # ---- Summary ----
    print_summary(conn)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
