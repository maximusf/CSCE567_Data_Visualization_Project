# merge.py
# by Maximus Fernandez

# Merges the three cleaned data sources (Twitch, SteamDB, Google Trends)
# into a single master dataset and computes the derived per-game summary
# statistics that visualizations 2 and 4 need.

# Pipeline:
#   1. Load the three cleaned long-format CSVs (one row per game-month).
#   2. Outer-merge on (game, month) so that no row is dropped just
#      because one source lacks data for that period. This matters
#      because the three sources have different time coverage:
#        - Twitch: from each game's first appearance to present (90+ months for Among Us).
#        - SteamDB: from each game's launch to present.
#        - Google Trends: only the focused window selected per game.
#      Outer merge preserves all months from all sources.
#   3. Write data/clean/master.csv as the canonical merged dataset.
#   4. Compute two summary tables:
#        - lag_summary.csv: days between each game's Twitch peak month
#          and Steam peak month (visualization #2).
#        - growth_summary.csv: percent growth in Steam players in the
#          30 days following each game's Twitch peak month
#          (visualization #4).

# Design decision on "peak":
#   We define a game's peak month as the month with the highest average
#   (not peak) viewer or player count. Average captures sustained
#   popularity, which is the actual phenomenon the project's thesis
#   describes (streamers manufacturing mainstream awareness over a
#   period, not single-stream burst moments). However, SteamDB only
#   began tracking monthly average concurrent players in October 2022,
#   so for games whose viral period predates that (Among Us, Fall Guys),
#   avg_players is NaN and we fall back to peak_players. This fallback
#   is documented per-row in the lag summary so the writeup can address
#   it honestly.

import pandas as pd
from pathlib import Path

CLEAN_DIR = Path("data/clean")

# Game launch / mainstream-relevance dates. Used only for sanity checks
# and the writeup; not part of the merge logic itself.
GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]

# The cutoff date before which SteamDB did not track avg_players. Games
# whose Twitch peak falls before this date will fall back to peak_players
# for the Steam side of the lag calculation.
STEAMDB_AVG_CUTOFF = pd.Timestamp("2022-10-01")


def load_sources():
    # Loads the three cleaned long-format CSVs and returns them as
    # DataFrames. Parses the month column as datetime in each so the
    # eventual merge happens on real dates rather than strings.
    
    twitch = pd.read_csv(CLEAN_DIR / "twitch_all_clean.csv",
                         parse_dates=["month"])
    steam = pd.read_csv(CLEAN_DIR / "steamdb_all_clean.csv",
                        parse_dates=["month"])
    trends = pd.read_csv(CLEAN_DIR / "trends_all_clean.csv",
                         parse_dates=["month"])
    return twitch, steam, trends


def build_master(twitch: pd.DataFrame, steam: pd.DataFrame, trends: pd.DataFrame) -> pd.DataFrame:
    # Outer-merges the three cleaned sources on (game, month) and returns
    # the unified master DataFrame.
    
    # Outer merge so we keep every (game, month) pair from any source.
    # An inner merge would silently discard rows where one source lacks
    # data, which would distort the time series visualization.
    master = twitch.merge(steam, on=["game", "month"], how="outer")
    master = master.merge(trends, on=["game", "month"], how="outer")

    # Sort by (game, month) so the master CSV reads naturally as a series
    # of per-game time series stacked end-to-end.
    master = master.sort_values(["game", "month"]).reset_index(drop=True)

    return master


def pick_steam_metric(master: pd.DataFrame, game: str, twitch_peak_month: pd.Timestamp) -> str:
    # Decides which Steam metric (avg_players or peak_players) to use for
    # a given game's analysis.

    # The rule: if the game has a non-null avg_players value at its
    # Twitch-peak month AND at the following month (which we need for
    # the growth calculation), use avg_players. Otherwise, fall back to
    # peak_players for the entire game's analysis.

    # Critically, the decision is made per-game, not per-column. Mixing
    # metrics within a game's lag and growth calculations would produce
    # incoherent numbers (e.g. comparing avg_viewers peak to peak_players
    # peak for one game and to avg_players peak for another).

    # Among Us and Fall Guys both peaked virally in 2020, before SteamDB
    # started tracking avg_players in October 2022, so they fall back to
    # peak_players. Vampire Survivors and Lethal Company both peaked
    # after the cutoff and use avg_players.
    
    sub = master[master["game"] == game]
    next_month = twitch_peak_month + pd.DateOffset(months=1)

    peak_row = sub[sub["month"] == twitch_peak_month]
    after_row = sub[sub["month"] == next_month]

    # Both rows must exist and have a non-null avg_players value for us
    # to use it. Otherwise we cannot compute month-over-month growth on
    # avg, so peak_players is the only consistent option.
    has_avg = (
        len(peak_row) > 0
        and len(after_row) > 0
        and pd.notna(peak_row["avg_players"].iloc[0])
        and pd.notna(after_row["avg_players"].iloc[0])
    )
    return "avg_players" if has_avg else "peak_players"


def find_peak_month(df: pd.DataFrame, value_col: str) -> pd.Timestamp:
    # Returns the month with the maximum value in value_col within the
    # supplied (single-game) DataFrame. Skips NaN values automatically
    # via idxmax. The caller is responsible for passing a column that
    # actually has data for this game; pick_steam_metric handles that
    # decision for the Steam side.

    return df.loc[df[value_col].idxmax(), "month"]


def compute_lag_summary(master: pd.DataFrame) -> pd.DataFrame:
    # For each game, finds the Twitch peak month and the Steam peak month,
    # then computes the lag in days between them. Positive lag means
    # Steam peaked AFTER Twitch (the project's thesis), negative means
    # Steam peaked first.
    
    rows = []
    for game in GAMES:
        sub = master[master["game"] == game]

        # Twitch side: avg_viewers always exists for these games. We use
        # avg rather than peak because the project's thesis is about
        # sustained popularity rather than single-stream burst moments.
        twitch_peak_month = find_peak_month(sub, "avg_viewers")

        # Steam side: pick metric per-game based on data availability at
        # the Twitch-peak month. See pick_steam_metric for the full rule.
        steam_metric = pick_steam_metric(master, game, twitch_peak_month)

        # When we use peak_players for older games, restrict to non-null
        # rows. For games using avg_players, we rely on the per-game data
        # window already being valid (pick_steam_metric vetted it).
        valid = sub[sub[steam_metric].notna()]
        steam_peak_month = find_peak_month(valid, steam_metric)

        lag_days = (steam_peak_month - twitch_peak_month).days

        rows.append({
            "game": game,
            "twitch_peak_month": twitch_peak_month,
            "twitch_metric": "avg_viewers",
            "steam_peak_month": steam_peak_month,
            "steam_metric": steam_metric,
            "lag_days": lag_days,
        })

    return pd.DataFrame(rows)


def compute_growth_summary(master: pd.DataFrame, lag_summary: pd.DataFrame) -> pd.DataFrame:
    # For each game, computes the percent growth in Steam players in the
    # month immediately following the Twitch viewership peak.

    # The project proposal describes this as "30 days following" the
    # Twitch peak, but our data is at monthly granularity, so we
    # operationalize "30 days after month M" as the month M+1 entry.
    # The percent growth compares M+1 to M.

    # Edge case: Fall Guys had its Twitch viewership peak in July 2020,
    # a full month BEFORE its Steam launch in August 2020. This means
    # there is no Steam player data at the Twitch peak month itself,
    # making percent growth mathematically undefined (cannot grow from
    # a missing baseline). Rather than silently returning NaN for what
    # is arguably the most striking data point in the project (extreme
    # streamer-driven hype preceding actual product availability), we
    # detect this specific case and report the players_at_peak as 0
    # with the players_month_after value carried forward; the resulting
    # growth is reported as "launch" rather than a numeric percentage,
    # so the visualization can render it distinctly from the others.

    # Uses the same Steam metric (avg_players or peak_players) that the
    # lag summary picked for each game, so growth and lag tell consistent
    # stories per-game.
    
    rows = []
    for _, lag_row in lag_summary.iterrows():
        game = lag_row["game"]
        twitch_peak_month = lag_row["twitch_peak_month"]
        steam_metric = lag_row["steam_metric"]

        sub = master[master["game"] == game].sort_values("month")

        # The month-after-peak. DateOffset handles year rollovers
        # correctly (e.g. Dec 2023 + 1 month becomes Jan 2024).
        next_month = twitch_peak_month + pd.DateOffset(months=1)

        before_row = sub[sub["month"] == twitch_peak_month]
        after_row = sub[sub["month"] == next_month]

        before = before_row[steam_metric].iloc[0] if len(before_row) > 0 else pd.NA
        after = after_row[steam_metric].iloc[0] if len(after_row) > 0 else pd.NA

        # Detect the pre-launch hype case: Twitch had real viewership but
        # the game was not yet on Steam. In that case the post-peak month
        # IS the launch month, and any growth metric that divides by 0 or
        # NaN would be meaningless. We mark these explicitly so the
        # visualization layer can render them in a distinct style.
        was_pre_launch = pd.isna(before) and pd.notna(after)

        if was_pre_launch:
            note = "pre_launch_hype"
            pct_growth = pd.NA
        elif pd.isna(before) or pd.isna(after) or before == 0:
            note = "insufficient_data"
            pct_growth = pd.NA
        else:
            note = "ok"
            pct_growth = (after - before) / before * 100

        rows.append({
            "game": game,
            "twitch_peak_month": twitch_peak_month,
            "steam_metric": steam_metric,
            "players_at_peak": before,
            "players_month_after": after,
            "pct_growth": pct_growth,
            "note": note,
        })

    df = pd.DataFrame(rows)
    df["pct_growth"] = pd.to_numeric(df["pct_growth"], errors="coerce").round(2)
    return df


def main():
    twitch, steam, trends = load_sources()

    master = build_master(twitch, steam, trends)
    master_out = CLEAN_DIR / "master.csv"
    master.to_csv(master_out, index=False)
    print(f"Wrote {len(master)} rows to {master_out}")

    lag = compute_lag_summary(master)
    lag_out = CLEAN_DIR / "lag_summary.csv"
    lag.to_csv(lag_out, index=False)
    print(f"\nWrote lag summary to {lag_out}:")
    print(lag.to_string(index=False))

    growth = compute_growth_summary(master, lag)
    growth_out = CLEAN_DIR / "growth_summary.csv"
    growth.to_csv(growth_out, index=False)
    print(f"\nWrote growth summary to {growth_out}:")
    print(growth.to_string(index=False))


if __name__ == "__main__":
    main()
