PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS seasons (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    start_year INTEGER NOT NULL,
    end_year   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS divisions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    age_group  TEXT,
    tier       TEXT,
    program    TEXT NOT NULL DEFAULT 'coed'
               CHECK (program IN ('coed', 'female', 'high_school'))
);

CREATE TABLE IF NOT EXISTS teams (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS team_registrations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id     INTEGER NOT NULL REFERENCES teams(id),
    season_id   INTEGER NOT NULL REFERENCES seasons(id),
    division_id INTEGER NOT NULL REFERENCES divisions(id),
    UNIQUE (team_id, season_id, division_id)
);

CREATE TABLE IF NOT EXISTS people (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL,
    usa_hockey_id  TEXT,
    notes          TEXT
);

CREATE TABLE IF NOT EXISTS player_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    season_id       INTEGER NOT NULL REFERENCES seasons(id),
    division_id     INTEGER NOT NULL REFERENCES divisions(id),
    raw_name        TEXT NOT NULL,
    jersey_number   TEXT,
    team_id         INTEGER NOT NULL REFERENCES teams(id),
    person_id       INTEGER REFERENCES people(id),
    games_played    INTEGER NOT NULL DEFAULT 0,
    goals           INTEGER NOT NULL DEFAULT 0,
    assists         INTEGER NOT NULL DEFAULT 0,
    hat_tricks      INTEGER NOT NULL DEFAULT 0,
    penalty_minutes INTEGER NOT NULL DEFAULT 0,
    points_per_game REAL NOT NULL DEFAULT 0.0,
    points          INTEGER NOT NULL DEFAULT 0,
    UNIQUE (season_id, division_id, raw_name, team_id)
);

CREATE TABLE IF NOT EXISTS goalie_stats (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    season_id         INTEGER NOT NULL REFERENCES seasons(id),
    division_id       INTEGER NOT NULL REFERENCES divisions(id),
    raw_name          TEXT NOT NULL,
    team_id           INTEGER NOT NULL REFERENCES teams(id),
    person_id         INTEGER REFERENCES people(id),
    games_played      INTEGER NOT NULL DEFAULT 0,
    shots_against     INTEGER NOT NULL DEFAULT 0,
    goals_against     INTEGER NOT NULL DEFAULT 0,
    goals_against_avg REAL NOT NULL DEFAULT 0.0,
    save_percentage   REAL NOT NULL DEFAULT 0.0,
    shutouts          INTEGER NOT NULL DEFAULT 0,
    UNIQUE (season_id, division_id, raw_name, team_id)
);

CREATE TABLE IF NOT EXISTS duplicate_candidates (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_a_type    TEXT NOT NULL CHECK (source_a_type IN ('player', 'goalie')),
    source_a_id      INTEGER NOT NULL,
    source_b_type    TEXT NOT NULL CHECK (source_b_type IN ('player', 'goalie')),
    source_b_id      INTEGER NOT NULL,
    similarity_score REAL NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'confirmed_same', 'confirmed_different')),
    resolved_at      TEXT,
    UNIQUE (source_a_type, source_a_id, source_b_type, source_b_id)
);
