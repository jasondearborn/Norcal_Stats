# CLAUDE.md — Project Context for AI Assistants

## Project

CAHA Norcal youth hockey statistics. Scrapes player and goalie stats from `stats.caha.timetoscore.com` covering Northern California age-division hockey (10U through High School, including Girls divisions). Data spans 2010-11 through 2022-23.

## Stack

- Python 3.12
- pandas, BeautifulSoup4, requests (scraping)
- rapidfuzz (fuzzy name matching)
- SQLite via stdlib `sqlite3` (no ORM)
- Flask + Jinja2 + HTMX + Pico.css (web UI)

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
| `load_db.py` | ETL: CSV → SQLite + fuzzy duplicate detection (threshold 91) |
| `cleanup_candidates.py` | Retroactively dismiss age-impossible pending candidates; re-run after `--reset` |
| `autoconfirm_candidates.py` | Bulk-confirm score-100 candidates via union-find; re-run after `--reset` |
| `app.py` | Flask web UI — reconciliation queue, confirm/dismiss actions |
| `templates/` | Jinja2 templates for the web UI |
| `tests/` | pytest test suite — must be green before every commit |
| `norcal_stats.db` | Generated database — not committed to git (derivable from CSVs + scripts) |
| `norcal_player_stats.csv` | Scraped player stats — committed as source data |
| `norcal_goalie_stats.csv` | Scraped goalie stats — committed as source data |
| `Norcal_Stats.ipynb` | Original scraper notebook — being phased out (see BACKLOG #004) |
| `BACKLOG.md` | Sequentially numbered work items — add new items at the bottom |

## Database Design Principles

- `raw_name` in `player_stats` and `goalie_stats` is never modified — it stores the name exactly as scraped.
- `person_id` on both stats tables is `NULL` until reconciled. Do not auto-assign `person_id` outside of the reconciliation scripts.
- `people` rows are created by `autoconfirm_candidates.py` (score=100 pairs) or manually via the web UI reconciliation queue (score 91–99 pairs).
- `duplicate_candidates` is the reconciliation queue. `status` values: `pending`, `confirmed_same`, `confirmed_different`.
- Score=100 candidates → auto-confirmed by `autoconfirm_candidates.py`. Score 91–99 → manual review. Score < 91 → auto-dismissed.
- Foreign keys are enabled: `PRAGMA foreign_keys = ON` must be set on every connection.

## After a `load_db.py --reset`

Re-run both post-processing scripts in order:
```bash
.venv/bin/python cleanup_candidates.py       # dismiss age-impossible pairs
.venv/bin/python autoconfirm_candidates.py   # confirm score-100 pairs
```

## Age / Play-Up Rules (affects duplicate detection)

- 10U players can play up to 12U; max 3 consecutive seasons in 12U (2-year span)
- 12U players **cannot** play up to 14U
- 14U players **can** play up to 16U or 18U (rare but valid; don't come back down same season)
- HS spans 4 grades (~4 seasons)
- Same age group, gap ≥ 3 seasons → impossible (gap ≥ 5 for HS)
- Cross-type (player↔goalie) same age group, gap ≥ 2 → impossible (tighter threshold)

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
4. **#007** — Web interface: phase 1 (reconciliation) done; phases 2–3 (analytics, dashboards) open
5. **#008** — Add DB indexes
6. **#009** — USA Hockey ID enrichment

## BACKLOG.md Convention

Items are sequentially numbered (`#001`, `#002`, ...) and never renumbered or reused. Always append new items at the bottom with the next available number. See BACKLOG.md for format.

## Development Workflow

After completing any feature or fix:

1. **Run the test suite** — must be green before committing.
   ```bash
   .venv/bin/pytest tests/ -v
   ```
2. **Commit** with a descriptive message referencing the BACKLOG item if applicable.
3. **Push** to `origin/main`.

For enhancements, follow TDD order: write failing tests first, implement, re-run until green, then commit and push.
