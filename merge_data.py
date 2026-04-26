# merge_data.py
# by Maximus Fernandez
#
# Merges the three cleaned sources (Twitch, SteamDB, Google Trends) on
# (game, month) and computes the lag and growth summaries used by
# visualizations 2 and 4.
#
# Peak metric choice: peak month = the month with the highest average
# viewer or player count. Average captures sustained popularity, which
# is what the project's thesis is about. SteamDB only began tracking
# avg_players in October 2022, so for games whose viral period
# predates that (Among Us, Fall Guys), the code falls back to
# peak_players. The fallback is recorded per row in the lag summary.

import pandas as pd
from pathlib import Path

CLEAN_DIR = Path("data/clean")

GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]

# Cutoff before which SteamDB did not track avg_players.
STEAMDB_AVG_CUTOFF = pd.Timestamp("2022-10-01")


def load_sources():
    twitch = pd.read_csv(CLEAN_DIR / "twitch_all_clean.csv",
                         parse_dates=["month"])
    steam = pd.read_csv(CLEAN_DIR / "steamdb_all_clean.csv",
                        parse_dates=["month"])
    trends = pd.read_csv(CLEAN_DIR / "trends_all_clean.csv",
                         parse_dates=["month"])
    return twitch, steam, trends


def build_master(twitch: pd.DataFrame, steam: pd.DataFrame, trends: pd.DataFrame) -> pd.DataFrame:
    # Outer merge so every (game, month) from any source is preserved.
    # An inner merge would drop months where one source lacks data,
    # which would distort the time series.
    master = twitch.merge(steam, on=["game", "month"], how="outer")
    master = master.merge(trends, on=["game", "month"], how="outer")

    master = master.sort_values(["game", "month"]).reset_index(drop=True)

    return master


def pick_steam_metric(master: pd.DataFrame, game: str, twitch_peak_month: pd.Timestamp) -> str:
    # Picks avg_players if both the Twitch peak month and the following
    # month have non-null values. Otherwise falls back to peak_players.
    # The choice is per-game so lag and growth use the same metric and
    # stay internally consistent.
    sub = master[master["game"] == game]
    next_month = twitch_peak_month + pd.DateOffset(months=1)

    peak_row = sub[sub["month"] == twitch_peak_month]
    after_row = sub[sub["month"] == next_month]

    # All four conditions must hold to use avg_players: both rows
    # must exist after the outer merge AND both must have non-null
    # avg values. Any failure means avg cannot support the growth
    # calculation, so peak_players is the only consistent option.
    has_avg = (
        len(peak_row) > 0
        and len(after_row) > 0
        and pd.notna(peak_row["avg_players"].iloc[0])
        and pd.notna(after_row["avg_players"].iloc[0])
    )
    return "avg_players" if has_avg else "peak_players"


def find_peak_month(df: pd.DataFrame, value_col: str) -> pd.Timestamp:
    # idxmax skips NaN automatically, but if value_col is entirely NaN
    # for this slice it raises. Caller is responsible for filtering to
    # non-null rows when needed (see compute_lag_summary).
    return df.loc[df[value_col].idxmax(), "month"]


def compute_lag_summary(master: pd.DataFrame) -> pd.DataFrame:
    # Lag in days between Twitch peak month and Steam peak month per
    # game. Positive lag means Steam peaked after Twitch.
    rows = []
    for game in GAMES:
        sub = master[master["game"] == game]

        twitch_peak_month = find_peak_month(sub, "avg_viewers")

        steam_metric = pick_steam_metric(master, game, twitch_peak_month)

        # Restrict to non-null rows so idxmax does not pick a NaN row
        # for games that fall back to peak_players.
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
    # Percent growth in Steam players from the Twitch peak month to the
    # following month, per game. "30 days after" the proposal calls for
    # is operationalized as month M+1 since the data is monthly.
    #
    # Edge case: Fall Guys had its Twitch peak in July 2020, the month
    # before its August 2020 Steam launch. There is no Steam baseline
    # at the Twitch peak, so growth is undefined and the row is tagged
    # "pre_launch_hype" for the visualization to render distinctly.
    #
    # Uses the same Steam metric the lag summary picked for that game.
    rows = []
    for _, lag_row in lag_summary.iterrows():
        game = lag_row["game"]
        twitch_peak_month = lag_row["twitch_peak_month"]
        steam_metric = lag_row["steam_metric"]

        sub = master[master["game"] == game].sort_values("month")

        # DateOffset(months=1) handles year rollover correctly
        # (Dec 2023 + 1 month becomes Jan 2024).
        next_month = twitch_peak_month + pd.DateOffset(months=1)

        before_row = sub[sub["month"] == twitch_peak_month]
        after_row = sub[sub["month"] == next_month]

        # Guard against missing rows: outer merge can leave a (game, month)
        # combination absent if no source had data for it. iloc[0] would
        # raise on an empty slice, so default to NA in that case.
        before = before_row[steam_metric].iloc[0] if len(before_row) > 0 else pd.NA
        after = after_row[steam_metric].iloc[0] if len(after_row) > 0 else pd.NA

        # Pre-launch hype: Twitch viewership existed but the game was
        # not yet on Steam.
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
