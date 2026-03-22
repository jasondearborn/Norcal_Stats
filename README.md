# Norcal Stats

Northern California (CAHA) youth hockey statistics scraper and database.

Scrapes player and goalie stats from [stats.caha.timetoscore.com](https://stats.caha.timetoscore.com) and loads them into a normalized SQLite database with fuzzy duplicate detection for player identity reconciliation.

---

## Prerequisites

- Python 3.12+
- Git

---

## Setup

```bash
git clone <repo-url>
cd Norcal_Stats
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Usage

### Scrape fresh data

Run the Jupyter notebook to re-scrape from CAHA and regenerate the CSVs:

```bash
source .venv/bin/activate
jupyter notebook Norcal_Stats.ipynb
```

> **Note:** The notebook is being phased out in favor of a standalone scraper script (see BACKLOG #004).

### Load the database

```bash
source .venv/bin/activate
python load_db.py           # incremental (INSERT OR IGNORE)
python load_db.py --reset   # drop and fully reload
```

The script reads `norcal_player_stats.csv` and `norcal_goalie_stats.csv`, populates all tables, and runs fuzzy name matching to populate `duplicate_candidates`.

---

## Database Schema

`norcal_stats.db` — SQLite, defined in `schema.sql`.

| Table | Description |
|---|---|
| `seasons` | One row per season (e.g., `2022-23`) |
| `divisions` | One row per division with parsed `age_group`, `tier`, `program` |
| `teams` | Unique team names |
| `team_registrations` | Bridge: which team played in which division in which season |
| `people` | Canonical player identities — populated during manual reconciliation |
| `player_stats` | One row per player per season/division; `raw_name` preserved as-scraped |
| `goalie_stats` | One row per goalie per season/division; `raw_name` preserved as-scraped |
| `duplicate_candidates` | Pairs of records suspected to be the same person, pending review |

### Division programs

| `program` | Description |
|---|---|
| `coed` | Standard age-division tiers (10U–18U, tiers A/AA/AAA/B/BB) |
| `female` | Girls-only divisions |
| `high_school` | High School D1/D2/D3 |

---

## Duplicate Reconciliation

Players can appear with slightly different name spellings across seasons (e.g., with or without a middle name). The ETL script uses `rapidfuzz.token_sort_ratio` (threshold: 88) to generate candidate pairs across different seasons within the same age group and program.

**Workflow (future web UI — see BACKLOG #007):**

1. Query `duplicate_candidates WHERE status = 'pending'`
2. For each pair, show the user both records side by side
3. **Confirmed same person:** create a row in `people`, set `person_id` on both stat rows, update `status = 'confirmed_same'`
4. **Different people:** update `status = 'confirmed_different'`

**Dual-roster note:** A player appearing in multiple divisions within the *same* season is intentional — girls may play on both a girls and coed team; players ~14U+ may dual-roster on a High School team. Same-season cross-division appearances are never flagged as duplicates.

---

## Data Notes

- **Seasons covered:** 2010-11 through 2022-23 (11 seasons)
- **Missing seasons:** 2011-12 and 2020-21 were not scraped (likely COVID/data gap)
- **Records:** ~10,900 player stat rows, ~1,500 goalie stat rows, 35 divisions, 160 teams

---

## Project Roadmap

See [BACKLOG.md](BACKLOG.md) for the full list of planned work.
