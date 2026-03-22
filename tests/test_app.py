"""
Read-only test harness for the Norcal Stats web interface.

"Read-only" means: the production database (norcal_stats.db) is NEVER
touched. All tests run against an in-memory SQLite database seeded with
minimal fixture data. The real DB is not opened, modified, or even
checked for existence.

Workflow contract:
  - Write tests for an enhancement BEFORE implementing it; they should FAIL.
  - Implement the enhancement.
  - Re-run the suite; all tests should PASS.
  - Commit only when the suite is green.
"""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

# ── App import ────────────────────────────────────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
import app as flask_app  # noqa: E402

# ── Schema (read once at module load) ─────────────────────────────────────────
SCHEMA_SQL = (Path(__file__).parent.parent / "schema.sql").read_text()


# ── Fixtures ──────────────────────────────────────────────────────────────────

class _NoCloseDB:
    """Wraps a sqlite3.Connection and makes close() a no-op.

    The app calls db.close() at the end of each request. In tests we share one
    in-memory connection across the whole test, so we must prevent the app from
    closing it before our post-request assertions run.
    """

    def __init__(self, db):
        self._db = db

    def __getattr__(self, name):
        return getattr(self._db, name)

    def close(self):
        pass  # intentional no-op

    def __enter__(self):
        return self._db.__enter__()

    def __exit__(self, *args):
        return self._db.__exit__(*args)


def _make_db():
    """Return an in-memory SQLite connection pre-loaded with fixture data."""
    db = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA_SQL)

    db.execute("INSERT INTO seasons (name, start_year, end_year) VALUES ('2015-16', 2015, 2016)")
    db.execute("INSERT INTO seasons (name, start_year, end_year) VALUES ('2016-17', 2016, 2017)")
    db.execute("INSERT INTO divisions (name, age_group, tier, program) VALUES ('12U AA', '12U', 'AA', 'coed')")
    db.execute("INSERT INTO teams (name) VALUES ('Sharks')")
    db.execute("INSERT INTO teams (name) VALUES ('Ducks')")

    # Two player stat rows — close but not identical names
    db.execute(
        """INSERT INTO player_stats
           (season_id, division_id, raw_name, jersey_number, team_id,
            games_played, goals, assists, points, hat_tricks, penalty_minutes, points_per_game)
           VALUES (1, 1, 'John Smith', '7', 1, 10, 5, 3, 8, 0, 2, 0.8)"""
    )
    db.execute(
        """INSERT INTO player_stats
           (season_id, division_id, raw_name, jersey_number, team_id,
            games_played, goals, assists, points, hat_tricks, penalty_minutes, points_per_game)
           VALUES (2, 1, 'Jon Smith', '7', 2, 12, 6, 4, 10, 1, 0, 0.83)"""
    )

    # Goalie stat row for cross-type candidate test
    db.execute(
        """INSERT INTO goalie_stats
           (season_id, division_id, raw_name, team_id,
            games_played, shots_against, goals_against, goals_against_avg, save_percentage, shutouts)
           VALUES (1, 1, 'Alex Jones', 1, 8, 200, 10, 1.25, 0.95, 2)"""
    )

    # Two pending duplicate candidates: player-player pair + player-goalie pair
    db.execute(
        """INSERT INTO duplicate_candidates
           (source_a_type, source_a_id, source_b_type, source_b_id, similarity_score, status)
           VALUES ('player', 1, 'player', 2, 96.0, 'pending')"""
    )
    db.execute(
        """INSERT INTO duplicate_candidates
           (source_a_type, source_a_id, source_b_type, source_b_id, similarity_score, status)
           VALUES ('player', 1, 'goalie', 1, 80.0, 'pending')"""
    )

    db.commit()
    return db


@pytest.fixture
def test_db():
    raw = _make_db()
    db = _NoCloseDB(raw)
    yield db
    raw.close()  # actually close after the test is done


@pytest.fixture
def client(test_db):
    flask_app.app.config["TESTING"] = True
    with patch.object(flask_app, "get_db", return_value=test_db):
        with flask_app.app.test_client() as c:
            yield c


# ── Route tests ───────────────────────────────────────────────────────────────

class TestIndexRedirect:
    def test_redirects_to_reconciliation(self, client):
        resp = client.get("/")
        assert resp.status_code == 302
        assert "/reconciliation" in resp.headers["Location"]


class TestQueue:
    def test_queue_loads(self, client):
        resp = client.get("/reconciliation")
        assert resp.status_code == 200

    def test_queue_shows_pending_candidates(self, client):
        resp = client.get("/reconciliation")
        body = resp.data.decode()
        assert "John Smith" in body
        assert "Jon Smith" in body

    def test_queue_shows_progress_counter(self, client):
        resp = client.get("/reconciliation")
        body = resp.data.decode()
        # "2 pending" somewhere on the page
        assert "2" in body
        assert "pending" in body


class TestReview:
    def test_review_page_loads(self, client):
        resp = client.get("/reconciliation/1")
        assert resp.status_code == 200

    def test_review_shows_both_names(self, client):
        resp = client.get("/reconciliation/1")
        body = resp.data.decode()
        assert "John Smith" in body
        assert "Jon Smith" in body

    def test_review_shows_stat_fields(self, client):
        resp = client.get("/reconciliation/1")
        body = resp.data.decode()
        assert "2015-16" in body
        assert "Sharks" in body

    def test_review_404_for_missing_candidate(self, client):
        resp = client.get("/reconciliation/9999")
        assert resp.status_code == 404

    def test_review_has_confirm_form(self, client):
        resp = client.get("/reconciliation/1")
        body = resp.data.decode()
        assert "canonical_name" in body
        assert "John Smith" in body  # pre-filled value

    def test_review_cross_type_candidate(self, client):
        """Player-vs-goalie candidate renders without error."""
        resp = client.get("/reconciliation/2")
        assert resp.status_code == 200


class TestConfirm:
    def test_confirm_creates_person_record(self, client, test_db):
        client.post(
            "/reconciliation/1/confirm",
            data={"canonical_name": "John Smith"},
        )
        person = test_db.execute("SELECT * FROM people WHERE canonical_name = 'John Smith'").fetchone()
        assert person is not None

    def test_confirm_links_both_stat_rows(self, client, test_db):
        client.post(
            "/reconciliation/1/confirm",
            data={"canonical_name": "John Smith"},
        )
        person = test_db.execute("SELECT id FROM people WHERE canonical_name = 'John Smith'").fetchone()
        pid = person["id"]

        row_a = test_db.execute("SELECT person_id FROM player_stats WHERE id = 1").fetchone()
        row_b = test_db.execute("SELECT person_id FROM player_stats WHERE id = 2").fetchone()
        assert row_a["person_id"] == pid
        assert row_b["person_id"] == pid

    def test_confirm_marks_candidate_confirmed_same(self, client, test_db):
        client.post(
            "/reconciliation/1/confirm",
            data={"canonical_name": "John Smith"},
        )
        row = test_db.execute("SELECT status FROM duplicate_candidates WHERE id = 1").fetchone()
        assert row["status"] == "confirmed_same"

    def test_confirm_sets_resolved_at(self, client, test_db):
        client.post(
            "/reconciliation/1/confirm",
            data={"canonical_name": "John Smith"},
        )
        row = test_db.execute("SELECT resolved_at FROM duplicate_candidates WHERE id = 1").fetchone()
        assert row["resolved_at"] is not None

    def test_confirm_redirects_to_next_pending(self, client):
        resp = client.post(
            "/reconciliation/1/confirm",
            data={"canonical_name": "John Smith"},
        )
        assert resp.status_code == 302
        # Should redirect to candidate 2 (next pending)
        assert "/reconciliation/2" in resp.headers["Location"]

    def test_confirm_reuses_existing_person_id(self, client, test_db):
        """If one stat row already has a person_id, link the other to it."""
        # Pre-link row_a to a person
        test_db.execute("INSERT INTO people (canonical_name) VALUES ('Pre-existing')")
        existing_pid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
        test_db.execute("UPDATE player_stats SET person_id = ? WHERE id = 1", (existing_pid,))
        test_db.commit()

        client.post(
            "/reconciliation/1/confirm",
            data={"canonical_name": "Pre-existing"},
        )
        row_b = test_db.execute("SELECT person_id FROM player_stats WHERE id = 2").fetchone()
        assert row_b["person_id"] == existing_pid

        # No duplicate person record should have been created
        count = test_db.execute("SELECT COUNT(*) FROM people").fetchone()[0]
        assert count == 1

    def test_confirm_missing_name_returns_400(self, client):
        resp = client.post("/reconciliation/1/confirm", data={"canonical_name": ""})
        assert resp.status_code == 400


class TestDismiss:
    def test_dismiss_marks_candidate_confirmed_different(self, client, test_db):
        client.post("/reconciliation/1/dismiss")
        row = test_db.execute("SELECT status FROM duplicate_candidates WHERE id = 1").fetchone()
        assert row["status"] == "confirmed_different"

    def test_dismiss_sets_resolved_at(self, client, test_db):
        client.post("/reconciliation/1/dismiss")
        row = test_db.execute("SELECT resolved_at FROM duplicate_candidates WHERE id = 1").fetchone()
        assert row["resolved_at"] is not None

    def test_dismiss_redirects_to_next_pending(self, client):
        resp = client.post("/reconciliation/1/dismiss")
        assert resp.status_code == 302
        assert "/reconciliation/2" in resp.headers["Location"]

    def test_dismiss_htmx_returns_200_empty(self, client):
        resp = client.post(
            "/reconciliation/1/dismiss",
            headers={"HX-Request": "true"},
        )
        assert resp.status_code == 200
        assert resp.data == b""

    def test_dismiss_does_not_modify_stat_rows(self, client, test_db):
        client.post("/reconciliation/1/dismiss")
        row_a = test_db.execute("SELECT person_id FROM player_stats WHERE id = 1").fetchone()
        row_b = test_db.execute("SELECT person_id FROM player_stats WHERE id = 2").fetchone()
        assert row_a["person_id"] is None
        assert row_b["person_id"] is None

    def test_queue_empty_redirects_to_queue(self, client, test_db):
        """After dismissing the last two candidates, redirect goes to queue."""
        client.post("/reconciliation/1/dismiss")
        resp = client.post("/reconciliation/2/dismiss")
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/reconciliation")
