# CLAUDE.md — Project Context for AI Assistants

## Project

CAHA Norcal youth hockey statistics. Scrapes player and goalie stats from `stats.caha.timetoscore.com` covering Northern California age-division hockey (10U through High School, including Girls divisions). Data spans 2010-11 through 2022-23.

## Stack

- Python 3.12
- pandas, BeautifulSoup4, requests (scraping)
- rapidfuzz (fuzzy name matching)
- SQLite via stdlib `sqlite3` (no ORM)

## Environment

Always use the `.venv` virtual environment:

```bash
source .venv/bin/activate      # activate
.venv/bin/python script.py     # or run directly
pip install -r requirements.txt
```

Never install packages globally. Never skip the venv.

## Key Files

| File | Purpose |
|---|---|
| `schema.sql` | SQLite DDL — single source of truth for table definitions |
| `load_db.py` | ETL: CSV → SQLite + fuzzy duplicate detection |
| `norcal_stats.db` | Generated database — not committed to git (derivable from CSVs + script) |
| `norcal_player_stats.csv` | Scraped player stats — committed as source data |
| `norcal_goalie_stats.csv` | Scraped goalie stats — committed as source data |
| `Norcal_Stats.ipynb` | Original scraper notebook — being phased out (see BACKLOG #004) |
| `BACKLOG.md` | Sequentially numbered work items — add new items at the bottom |

## Database Design Principles

- `raw_name` in `player_stats` and `goalie_stats` is never modified — it stores the name exactly as scraped.
- `person_id` on both stats tables is `NULL` until manually reconciled. Do not auto-assign `person_id`.
- `people` rows are only created through the reconciliation workflow (future web UI, BACKLOG #007).
- `duplicate_candidates` is the queue for reconciliation. `status` values: `pending`, `confirmed_same`, `confirmed_different`.
- Foreign keys are enabled: `PRAGMA foreign_keys = ON` must be set on every connection.

## Dual-Roster Rules

Same player appearing in **multiple divisions within the same season** is expected and intentional:
- Girls can dual-roster on coed teams.
- Players ~14U and older can dual-roster on High School teams.

**Never** flag same-season cross-division appearances as duplicates. The fuzzy matcher already enforces this (it only compares records from different seasons).

## Division Programs

| `program` | When to use |
|---|---|
| `coed` | Standard age divisions (10U–18U, tiers A/AA/AAA/B/BB), including East/West variants |
| `female` | Any division prefixed with "Girls" |
| `high_school` | Any division prefixed with "High School" |

## Roadmap (do not re-plan what's already decided)

1. **#004** — Migrate scraper from Jupyter notebook to `scrape.py`
2. **#005** — Annual scrape job (append new season, no full reload)
3. **#006** — Scrape missing seasons (2011-12, 2020-21) if available
4. **#007** — Web interface: analytics, duplicate reconciliation, insights
5. **#008** — Add DB indexes
6. **#009** — USA Hockey ID enrichment

## BACKLOG.md Convention

Items are sequentially numbered (`#001`, `#002`, ...) and never renumbered or reused. Always append new items at the bottom with the next available number. See BACKLOG.md for format.
