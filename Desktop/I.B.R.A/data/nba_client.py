"""
I.B.R.A — Basketball Reference Client
Phase 1: Data Layer

Fetches team stats, player profiles, game logs, and rosters
from Basketball Reference via basketball_reference_web_scraper.
"""

import time
import pandas as pd
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import Team, OutputType

REQUEST_DELAY = 1.0  # seconds between requests


# ── Team lookup ──────────────────────────────────────────────────────────────

TEAM_ABBREVIATIONS = {
    "atlanta hawks": "ATL", "boston celtics": "BOS", "brooklyn nets": "BKN",
    "charlotte hornets": "CHA", "chicago bulls": "CHI", "cleveland cavaliers": "CLE",
    "dallas mavericks": "DAL", "denver nuggets": "DEN", "detroit pistons": "DET",
    "golden state warriors": "GSW", "houston rockets": "HOU", "indiana pacers": "IND",
    "los angeles clippers": "LAC", "los angeles lakers": "LAL", "memphis grizzlies": "MEM",
    "miami heat": "MIA", "milwaukee bucks": "MIL", "minnesota timberwolves": "MIN",
    "new orleans pelicans": "NOP", "new york knicks": "NYK", "oklahoma city thunder": "OKC",
    "orlando magic": "ORL", "philadelphia 76ers": "PHI", "phoenix suns": "PHX",
    "portland trail blazers": "POR", "sacramento kings": "SAC", "san antonio spurs": "SAS",
    "toronto raptors": "TOR", "utah jazz": "UTA", "washington wizards": "WAS",
}

def get_team_abbreviation(team_name: str) -> str:
    """Convert a full team name to its abbreviation."""
    return TEAM_ABBREVIATIONS.get(team_name.lower())


# ── Season helpers ────────────────────────────────────────────────────────────

def season_end_year(season: str = "2025-26") -> int:
    """Convert '2025-26' to 2026 (the year Basketball Reference uses)."""
    return int(season.split("-")[0]) + 1


# ── Core data functions ───────────────────────────────────────────────────────

def get_all_teams() -> pd.DataFrame:
    """Return a DataFrame of all NBA teams."""
    rows = [{"full_name": name, "abbreviation": abbr}
            for name, abbr in TEAM_ABBREVIATIONS.items()]
    return pd.DataFrame(rows)


def get_team_game_log(team_name: str, season: str = "2025-26") -> pd.DataFrame:
    """
    Fetch the full schedule/results for a team this season.
    """
    time.sleep(REQUEST_DELAY)
    team_enum = None
    for t in Team:
        if t.value.lower() == team_name.lower():
            team_enum = t
            break
    if not team_enum:
        raise ValueError(f"Team not found: {team_name}")

    year = season_end_year(season)
    schedule = client.season_schedule(season_end_year=year)
    df = pd.DataFrame(schedule)
    # Filter to just this team's games
    df = df[(df["home_team"] == team_enum) | (df["away_team"] == team_enum)]
    return df.reset_index(drop=True)

    year = season_end_year(season)
    games = client.team_game_log(team=team_enum, season_end_year=year)
    return pd.DataFrame(games)


def get_player_season_totals(season: str = "2025-26") -> pd.DataFrame:
    """Fetch season totals for all players."""
    time.sleep(REQUEST_DELAY)
    year = season_end_year(season)
    players = client.players_season_totals(season_end_year=year)
    return pd.DataFrame(players)


def get_player_advanced_stats(season: str = "2025-26") -> pd.DataFrame:
    """Fetch advanced stats (PER, TS%, BPM, VORP, etc.) for all players."""
    time.sleep(REQUEST_DELAY)
    year = season_end_year(season)
    players = client.players_advanced_season_totals(season_end_year=year)
    return pd.DataFrame(players)


def get_team_box_scores(date_str: str) -> pd.DataFrame:
    """
    Fetch all team box scores for a given date.
    date_str format: 'YYYY-MM-DD'
    """
    time.sleep(REQUEST_DELAY)
    from datetime import datetime
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    scores = client.team_box_scores(
        day=dt.day, month=dt.month, year=dt.year
    )
    return pd.DataFrame(scores)


def get_player_box_scores(date_str: str) -> pd.DataFrame:
    """
    Fetch all player box scores for a given date.
    date_str format: 'YYYY-MM-DD'
    """
    time.sleep(REQUEST_DELAY)
    from datetime import datetime
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    scores = client.player_box_scores(
        day=dt.day, month=dt.month, year=dt.year
    )
    return pd.DataFrame(scores)


def get_schedule(team_name: str, season: str = "2025-26") -> pd.DataFrame:
    """Fetch the full schedule for a team."""
    time.sleep(REQUEST_DELAY)
    t.value.lower() == team_name.lower()
    if not abbr:
        raise ValueError(f"Team not found: {team_name}")

    team_enum = None
    for t in Team:
        if t.value == abbr:
            team_enum = t
            break

    year = season_end_year(season)
    schedule = client.team_game_log(team=team_enum, season_end_year=year)
    return pd.DataFrame(schedule)


if __name__ == "__main__":
    print("Testing Basketball Reference client...")

    all_teams = get_all_teams()
    print(f"✓ {len(all_teams)} teams loaded")

    print("Fetching player season totals (this may take a moment)...")
    players = get_player_season_totals()
    print(f"✓ {len(players)} player records loaded")

    print("Fetching Lakers game log...")
    log = get_team_game_log("Los Angeles Lakers")
    print(f"✓ Lakers game log: {len(log)} games")

    print("\nAll checks passed. Basketball Reference client is ready.")