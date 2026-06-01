"""
I.B.R.A — Data Pipeline
Phase 1: Data Layer

Pulls all NBA data from Basketball Reference and stores it in SQLite.
Run modes:
    python data/nba_pipeline.py              # Full pull
    python data/nba_pipeline.py --update     # Game logs only (run daily)
    python data/nba_pipeline.py --season 2024-25  # Past season
"""

import os
import sys
import time
import argparse
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import Team

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(__file__), "ibra.db")
DB_URL = f"sqlite:///{os.path.abspath(DB_PATH)}"
CURRENT_SEASON = "2025-26"
REQUEST_DELAY = 1.5

TEAM_NAMES = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
    "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets",
    "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat",
    "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards",
]

def season_end_year(season: str) -> int:
    return int(season.split("-")[0]) + 1

def get_team_enum(team_name: str):
    for t in Team:
        if t.value.lower() == team_name.lower():
            return t
    return None

# ── DB Setup ──────────────────────────────────────────────────────────────────

def get_engine():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    return create_engine(DB_URL)

def init_db(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS teams (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name   TEXT UNIQUE,
                updated_at  TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS team_game_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                team_name   TEXT,
                season      TEXT,
                date        TEXT,
                home_team   TEXT,
                away_team   TEXT,
                home_score  INTEGER,
                away_score  INTEGER,
                updated_at  TEXT,
                UNIQUE(team_name, season, date, home_team, away_team)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS player_season_totals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                season      TEXT,
                name        TEXT,
                team        TEXT,
                positions   TEXT,
                age         REAL,
                games       INTEGER,
                points      REAL,
                rebounds    REAL,
                assists     REAL,
                steals      REAL,
                blocks      REAL,
                turnovers   REAL,
                fg_pct      REAL,
                three_pct   REAL,
                ft_pct      REAL,
                data        TEXT,
                updated_at  TEXT,
                UNIQUE(season, name, team)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS player_advanced_stats (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                season      TEXT,
                name        TEXT,
                team        TEXT,
                positions   TEXT,
                age         REAL,
                per         REAL,
                ts_pct      REAL,
                bpm         REAL,
                vorp        REAL,
                data        TEXT,
                updated_at  TEXT,
                UNIQUE(season, name, team)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at      TEXT,
                season      TEXT,
                step        TEXT,
                status      TEXT,
                records     INTEGER,
                error       TEXT
            )
        """))
        conn.commit()
    print("✓ Database initialized")

# ── Helpers ───────────────────────────────────────────────────────────────────

def now():
    return datetime.utcnow().isoformat()

def log_step(engine, season, step, status, records=0, error=None):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_log (run_at, season, step, status, records, error)
            VALUES (:run_at, :season, :step, :status, :records, :error)
        """), {"run_at": now(), "season": season, "step": step,
               "status": status, "records": records,
               "error": str(error) if error else None})
        conn.commit()

def safe_get(fn, *args, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(REQUEST_DELAY)
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries - 1:
                wait = REQUEST_DELAY * (2 ** attempt)
                print(f"  Retry {attempt+1}/{retries} after {wait:.1f}s — {e}")
                time.sleep(wait)
            else:
                raise

# ── Step 1: Teams ─────────────────────────────────────────────────────────────

def pull_teams(engine):
    print("\n[1/5] Saving team list...")
    with engine.connect() as conn:
        for name in TEAM_NAMES:
            conn.execute(text("""
                INSERT OR IGNORE INTO teams (full_name, updated_at)
                VALUES (:name, :updated_at)
            """), {"name": name, "updated_at": now()})
        conn.commit()
    print(f"  ✓ {len(TEAM_NAMES)} teams saved")
    log_step(engine, CURRENT_SEASON, "teams", "ok", len(TEAM_NAMES))

# ── Step 2: Team Game Logs ────────────────────────────────────────────────────

def pull_team_game_logs(engine, season):
    print(f"\n[2/5] Pulling game logs for all 30 teams ({season})...")
    year = season_end_year(season)
    total = 0

    # Pull full schedule once, then filter per team
    try:
        schedule = safe_get(client.season_schedule, season_end_year=year)
        df = pd.DataFrame(schedule)
    except Exception as e:
        print(f"  ✗ Failed to pull schedule: {e}")
        log_step(engine, season, "team_game_logs", "error", error=e)
        return

    for i, team_name in enumerate(TEAM_NAMES, 1):
        try:
            team_enum = get_team_enum(team_name)
            team_df = df[(df["home_team"] == team_enum) | (df["away_team"] == team_enum)].copy()

            with engine.connect() as conn:
                for _, row in team_df.iterrows():
                    conn.execute(text("""
                        INSERT OR IGNORE INTO team_game_logs
                        (team_name, season, date, home_team, away_team,
                         home_score, away_score, updated_at)
                        VALUES (:team_name, :season, :date, :home_team, :away_team,
                                :home_score, :away_score, :updated_at)
                    """), {
                        "team_name": team_name,
                        "season": season,
                        "date": str(row.get("start_time", "")),
                        "home_team": str(row.get("home_team", "")),
                        "away_team": str(row.get("away_team", "")),
                        "home_score": row.get("home_team_score", None),
                        "away_score": row.get("away_team_score", None),
                        "updated_at": now(),
                    })
                conn.commit()

            total += len(team_df)
            print(f"  [{i:2}/30] {team_name}: {len(team_df)} games")
        except Exception as e:
            print(f"  ✗ {team_name} failed: {e}")

    print(f"  ✓ Total: {total} game log rows saved")
    log_step(engine, season, "team_game_logs", "ok", total)

# ── Step 3: Player Season Totals ──────────────────────────────────────────────

def pull_player_season_totals(engine, season):
    print(f"\n[3/5] Pulling player season totals ({season})...")
    year = season_end_year(season)
    try:
        data = safe_get(client.players_season_totals, season_end_year=year)
        df = pd.DataFrame(data)

        with engine.connect() as conn:
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT OR REPLACE INTO player_season_totals
                    (season, name, team, positions, age, games, points, rebounds,
                     assists, steals, blocks, turnovers, fg_pct, three_pct, ft_pct,
                     data, updated_at)
                    VALUES (:season, :name, :team, :positions, :age, :games, :points,
                            :rebounds, :assists, :steals, :blocks, :turnovers, :fg_pct,
                            :three_pct, :ft_pct, :data, :updated_at)
                """), {
                    "season": season,
                    "name": str(row.get("name", "")),
                    "team": str(row.get("team", "")),
                    "positions": str(row.get("positions", "")),
                    "age": row.get("age", None),
                    "games": row.get("games_played", None),
                    "points": row.get("points", None),
                    "rebounds": row.get("total_rebounds", None),
                    "assists": row.get("assists", None),
                    "steals": row.get("steals", None),
                    "blocks": row.get("blocks", None),
                    "turnovers": row.get("turnovers", None),
                    "fg_pct": row.get("made_field_goals", 0) / row.get("attempted_field_goals", 1)
                               if row.get("attempted_field_goals", 0) > 0 else None,
                    "three_pct": row.get("made_three_point_field_goals", 0) / row.get("attempted_three_point_field_goals", 1)
                                 if row.get("attempted_three_point_field_goals", 0) > 0 else None,
                    "ft_pct": row.get("made_free_throws", 0) / row.get("attempted_free_throws", 1)
                              if row.get("attempted_free_throws", 0) > 0 else None,
                    "data": row.to_json(),
                    "updated_at": now(),
                })
            conn.commit()

        print(f"  ✓ {len(df)} player records saved")
        log_step(engine, season, "player_season_totals", "ok", len(df))
    except Exception as e:
        print(f"  ✗ Player season totals failed: {e}")
        log_step(engine, season, "player_season_totals", "error", error=e)

# ── Step 4: Player Advanced Stats ─────────────────────────────────────────────

def pull_player_advanced_stats(engine, season):
    print(f"\n[4/5] Pulling player advanced stats ({season})...")
    year = season_end_year(season)
    try:
        data = safe_get(client.players_advanced_season_totals, season_end_year=year)
        df = pd.DataFrame(data)

        with engine.connect() as conn:
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT OR REPLACE INTO player_advanced_stats
                    (season, name, team, positions, age, per, ts_pct, bpm, vorp,
                     data, updated_at)
                    VALUES (:season, :name, :team, :positions, :age, :per, :ts_pct,
                            :bpm, :vorp, :data, :updated_at)
                """), {
                    "season": season,
                    "name": str(row.get("name", "")),
                    "team": str(row.get("team", "")),
                    "positions": str(row.get("positions", "")),
                    "age": row.get("age", None),
                    "per": row.get("player_efficiency_rating", None),
                    "ts_pct": row.get("true_shooting_percentage", None),
                    "bpm": row.get("box_plus_minus", None),
                    "vorp": row.get("value_over_replacement_player", None),
                    "data": row.to_json(),
                    "updated_at": now(),
                })
            conn.commit()

        print(f"  ✓ {len(df)} advanced stat records saved")
        log_step(engine, season, "player_advanced_stats", "ok", len(df))
    except Exception as e:
        print(f"  ✗ Player advanced stats failed: {e}")
        log_step(engine, season, "player_advanced_stats", "error", error=e)

# ── Step 5: Standings ─────────────────────────────────────────────────────────

def pull_standings(engine, season):
    print(f"\n[5/5] Pulling standings ({season})...")
    year = season_end_year(season)
    try:
        data = safe_get(client.standings, season_end_year=year)
        # Convert enums to strings
        rows = []
        for row in data:
            rows.append({
                "team": row["team"].value,
                "wins": row["wins"],
                "losses": row["losses"],
                "division": row["division"].value,
                "conference": row["conference"].value,
                "season": season,
                "updated_at": now(),
            })
        df = pd.DataFrame(rows)
        df.to_sql("standings", engine, if_exists="replace", index=False)
        print(f"  ✓ {len(df)} team standings saved")
        log_step(engine, season, "standings", "ok", len(df))
    except Exception as e:
        print(f"  ✗ Standings failed: {e}")
        log_step(engine, season, "standings", "error", error=e)
# ── Query Helpers ─────────────────────────────────────────────────────────────

def query_recent_games(engine, team_name: str, n: int = 10, season: str = CURRENT_SEASON) -> pd.DataFrame:
    return pd.read_sql("""
        SELECT date, home_team, away_team, home_score, away_score
        FROM team_game_logs
        WHERE team_name = ? AND season = ?
        ORDER BY date DESC
        LIMIT ?
    """, engine, params=(team_name, season, n))

# ── Main ──────────────────────────────────────────────────────────────────────

def run_pipeline(season: str = CURRENT_SEASON, update_only: bool = False):
    print(f"\n{'='*50}")
    print(f"  I.B.R.A Data Pipeline — {season}")
    print(f"  Mode: {'Update (game logs only)' if update_only else 'Full pull'}")
    print(f"  DB: {os.path.abspath(DB_PATH)}")
    print(f"{'='*50}")

    start = time.time()
    engine = get_engine()
    init_db(engine)

    if update_only:
        pull_team_game_logs(engine, season)
    else:
        pull_teams(engine)
        pull_team_game_logs(engine, season)
        pull_player_season_totals(engine, season)
        pull_player_advanced_stats(engine, season)
        pull_standings(engine, season)

    elapsed = time.time() - start
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)

    print(f"\n{'='*50}")
    print(f"  ✓ Pipeline complete in {mins}m {secs}s")
    print(f"  DB saved to: {os.path.abspath(DB_PATH)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="I.B.R.A NBA Data Pipeline")
    parser.add_argument("--season", default=CURRENT_SEASON, help="Season e.g. 2024-25")
    parser.add_argument("--update", action="store_true", help="Update game logs only")
    args = parser.parse_args()
    run_pipeline(season=args.season, update_only=args.update)