# Backlog

Items are sequentially numbered and never renumbered. Add new items at the bottom with the next available number.

**Types:** `Bug` | `Enhancement` | `Feature`
**Statuses:** `Open` | `In Progress` | `Done`

---

### #001 — Initial web scraper
**Type:** Feature
**Status:** Done
**Description:** Jupyter notebook scraper targeting `stats.caha.timetoscore.com`. Discovers divisions by season, scrapes player and goalie stat tables, deduplicates, and writes `norcal_player_stats.csv` and `norcal_goalie_stats.csv`.

---

### #002 — SQLite schema and ETL script
**Type:** Feature
**Status:** Done
**Description:** Designed normalized SQLite schema (`schema.sql`) and ETL loader (`load_db.py`). Populates `seasons`, `divisions`, `teams`, `team_registrations`, `player_stats`, and `goalie_stats` from CSVs. Includes `--reset` flag for full reload.

---

### #003 — Fuzzy duplicate candidate detection
**Type:** Enhancement
**Status:** Done
**Description:** Added `rapidfuzz.token_sort_ratio` pass at the end of `load_db.py` to populate `duplicate_candidates`. Compares names within the same `age_group` and `program` across different seasons only (same-season multi-division appearances are intentional dual-roster). Threshold: 91. Age-constraint impossibility filter (`is_impossible_candidate()`) prevents generation of pairs that are impossible given youth hockey age rules.

---

### #004 — Migrate scraper from Jupyter to standalone script
**Type:** Feature
**Status:** Open
**Description:** Replace `Norcal_Stats.ipynb` with a standalone `scrape.py` script. Should be importable, CLI-driven, and testable without a Jupyter environment. Maintain the same scraping logic.

---

### #005 — Annual scrape job
**Type:** Feature
**Status:** Open
**Description:** Build a mechanism to scrape and append a single new season once it concludes, without a full reload. Should detect what seasons are already in the DB and only fetch new data. Target cadence: once per season (roughly annual).

---

### #006 — Scrape missing seasons
**Type:** Enhancement
**Status:** Open
**Description:** Seasons 2011-12 and 2020-21 were not captured in the initial scrape (likely COVID gap for 2020-21; unclear for 2011-12). Investigate availability on the source site and add if accessible.

---

### #007 — Web interface
**Type:** Feature
**Status:** In Progress
**Description:** Flask + HTMX + Pico.css web UI served on `0.0.0.0:5000` via systemd (`norcal-stats.service`). Phase 1 (reconciliation queue) complete: review pending `duplicate_candidates` side-by-side, confirm same person (creates/links `people` record) or dismiss. Score-100 pairs auto-confirmed; score < 91 auto-dismissed; score 91–99 surface for human review. Remaining phases: (2) analytic query explorer; (3) insight dashboards.

---

### #008 — Add database indexes
**Type:** Enhancement
**Status:** Open
**Description:** Add indexes on commonly queried columns: `player_stats.raw_name`, `player_stats.season_id`, `player_stats.division_id`, `player_stats.team_id`; same for `goalie_stats`; `duplicate_candidates.status`.

---

### #009 — USA Hockey ID enrichment
**Type:** Enhancement
**Status:** Open
**Description:** Add `usa_hockey_id` to the `people` table (already present in schema). Define a process to populate it — either manual entry during reconciliation or lookup against USA Hockey's registration data.
