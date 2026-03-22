import sqlite3
from datetime import datetime, timezone

from flask import Flask, redirect, render_template, request, url_for

app = Flask(__name__)
DB_PATH = "norcal_stats.db"


def get_db():
    db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def _fetch_stat_row(db, row_type, row_id):
    """Return a stat row with joined season, division, and team names."""
    if row_type == "player":
        return db.execute(
            """
            SELECT ps.*, s.name AS season_name, d.name AS division_name, t.name AS team_name
            FROM player_stats ps
            JOIN seasons s ON ps.season_id = s.id
            JOIN divisions d ON ps.division_id = d.id
            JOIN teams t ON ps.team_id = t.id
            WHERE ps.id = ?
            """,
            (row_id,),
        ).fetchone()
    else:
        return db.execute(
            """
            SELECT gs.*, s.name AS season_name, d.name AS division_name, t.name AS team_name
            FROM goalie_stats gs
            JOIN seasons s ON gs.season_id = s.id
            JOIN divisions d ON gs.division_id = d.id
            JOIN teams t ON gs.team_id = t.id
            WHERE gs.id = ?
            """,
            (row_id,),
        ).fetchone()


def _next_pending_id(db, exclude_id=None):
    """Return the id of the next pending candidate, or None."""
    if exclude_id is not None:
        row = db.execute(
            "SELECT id FROM duplicate_candidates WHERE status = 'pending' AND id != ? ORDER BY similarity_score DESC LIMIT 1",
            (exclude_id,),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT id FROM duplicate_candidates WHERE status = 'pending' ORDER BY similarity_score DESC LIMIT 1"
        ).fetchone()
    return row["id"] if row else None


@app.route("/")
def index():
    return redirect(url_for("queue"))


@app.route("/reconciliation")
def queue():
    db = get_db()
    rows = db.execute(
        """
        SELECT
            dc.id,
            dc.similarity_score,
            dc.status,
            CASE dc.source_a_type WHEN 'player' THEN psa.raw_name ELSE gsa.raw_name END AS name_a,
            CASE dc.source_b_type WHEN 'player' THEN psb.raw_name ELSE gsb.raw_name END AS name_b,
            dc.source_a_type,
            dc.source_b_type,
            CASE dc.source_a_type WHEN 'player' THEN sa.name ELSE sa2.name END AS season_a,
            CASE dc.source_b_type WHEN 'player' THEN sb.name ELSE sb2.name END AS season_b,
            CASE dc.source_a_type WHEN 'player' THEN ta.name ELSE ta2.name END AS team_a,
            CASE dc.source_b_type WHEN 'player' THEN tb.name ELSE tb2.name END AS team_b
        FROM duplicate_candidates dc
        LEFT JOIN player_stats psa ON dc.source_a_type = 'player' AND dc.source_a_id = psa.id
        LEFT JOIN goalie_stats gsa ON dc.source_a_type = 'goalie' AND dc.source_a_id = gsa.id
        LEFT JOIN player_stats psb ON dc.source_b_type = 'player' AND dc.source_b_id = psb.id
        LEFT JOIN goalie_stats gsb ON dc.source_b_type = 'goalie' AND dc.source_b_id = gsb.id
        LEFT JOIN seasons sa ON dc.source_a_type = 'player' AND psa.season_id = sa.id
        LEFT JOIN seasons sa2 ON dc.source_a_type = 'goalie' AND gsa.season_id = sa2.id
        LEFT JOIN seasons sb ON dc.source_b_type = 'player' AND psb.season_id = sb.id
        LEFT JOIN seasons sb2 ON dc.source_b_type = 'goalie' AND gsb.season_id = sb2.id
        LEFT JOIN teams ta ON dc.source_a_type = 'player' AND psa.team_id = ta.id
        LEFT JOIN teams ta2 ON dc.source_a_type = 'goalie' AND gsa.team_id = ta2.id
        LEFT JOIN teams tb ON dc.source_b_type = 'player' AND psb.team_id = tb.id
        LEFT JOIN teams tb2 ON dc.source_b_type = 'goalie' AND gsb.team_id = tb2.id
        WHERE dc.status = 'pending'
        ORDER BY dc.similarity_score DESC
        """,
    ).fetchall()

    counts = db.execute(
        "SELECT status, COUNT(*) AS n FROM duplicate_candidates GROUP BY status"
    ).fetchall()
    db.close()

    total = sum(r["n"] for r in counts)
    pending = next((r["n"] for r in counts if r["status"] == "pending"), 0)
    return render_template(
        "reconciliation/queue.html", rows=rows, pending=pending, total=total
    )


@app.route("/reconciliation/<int:candidate_id>")
def review(candidate_id):
    db = get_db()
    candidate = db.execute(
        "SELECT * FROM duplicate_candidates WHERE id = ?", (candidate_id,)
    ).fetchone()
    if candidate is None:
        db.close()
        return "Candidate not found", 404

    row_a = _fetch_stat_row(db, candidate["source_a_type"], candidate["source_a_id"])
    row_b = _fetch_stat_row(db, candidate["source_b_type"], candidate["source_b_id"])
    db.close()

    pre_filled = row_a["raw_name"] if row_a else ""
    return render_template(
        "reconciliation/review.html",
        candidate=candidate,
        row_a=row_a,
        row_b=row_b,
        pre_filled=pre_filled,
    )


@app.route("/reconciliation/<int:candidate_id>/confirm", methods=["POST"])
def confirm(candidate_id):
    canonical_name = request.form.get("canonical_name", "").strip()
    if not canonical_name:
        return "canonical_name is required", 400

    db = get_db()
    candidate = db.execute(
        "SELECT * FROM duplicate_candidates WHERE id = ?", (candidate_id,)
    ).fetchone()
    if candidate is None:
        db.close()
        return "Candidate not found", 404

    row_a = _fetch_stat_row(db, candidate["source_a_type"], candidate["source_a_id"])
    row_b = _fetch_stat_row(db, candidate["source_b_type"], candidate["source_b_id"])

    pid_a = row_a["person_id"] if row_a else None
    pid_b = row_b["person_id"] if row_b else None

    with db:
        if pid_a is None and pid_b is None:
            cur = db.execute(
                "INSERT INTO people (canonical_name) VALUES (?)", (canonical_name,)
            )
            person_id = cur.lastrowid
        elif pid_a is not None and pid_b is None:
            person_id = pid_a
            db.execute(
                "UPDATE people SET canonical_name = ? WHERE id = ?",
                (canonical_name, person_id),
            )
        elif pid_a is None and pid_b is not None:
            person_id = pid_b
            db.execute(
                "UPDATE people SET canonical_name = ? WHERE id = ?",
                (canonical_name, person_id),
            )
        else:
            # Both already linked — use source_a's person, update canonical name
            person_id = pid_a
            db.execute(
                "UPDATE people SET canonical_name = ? WHERE id = ?",
                (canonical_name, person_id),
            )

        # Link both stat rows to the person
        table_a = "player_stats" if candidate["source_a_type"] == "player" else "goalie_stats"
        table_b = "player_stats" if candidate["source_b_type"] == "player" else "goalie_stats"
        db.execute(
            f"UPDATE {table_a} SET person_id = ? WHERE id = ?",
            (person_id, candidate["source_a_id"]),
        )
        db.execute(
            f"UPDATE {table_b} SET person_id = ? WHERE id = ?",
            (person_id, candidate["source_b_id"]),
        )

        db.execute(
            "UPDATE duplicate_candidates SET status = 'confirmed_same', resolved_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), candidate_id),
        )

    next_id = _next_pending_id(db, exclude_id=candidate_id)
    db.close()

    if next_id:
        return redirect(url_for("review", candidate_id=next_id))
    return redirect(url_for("queue"))


@app.route("/reconciliation/<int:candidate_id>/dismiss", methods=["POST"])
def dismiss(candidate_id):
    db = get_db()
    with db:
        db.execute(
            "UPDATE duplicate_candidates SET status = 'confirmed_different', resolved_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), candidate_id),
        )
    next_id = _next_pending_id(db, exclude_id=candidate_id)
    db.close()

    # HTMX inline dismiss from queue — return empty 200 so HTMX can swap the row out
    if request.headers.get("HX-Request"):
        return "", 200

    if next_id:
        return redirect(url_for("review", candidate_id=next_id))
    return redirect(url_for("queue"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
