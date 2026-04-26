# clean_steam.py
# by Maximus Fernandez

# Cleans the four manually-collected SteamDB monthly CSVs. SteamDB's
# "Charts" page does provide a CSV export, but the raw output has the
# same Excel-induced formatting issues as the TwitchTracker data plus
# a few that are unique to SteamDB:

#   1. The % Gain columns are inconsistent within a single file: some
#      rows kept the percent sign as a string ("-19.80%"), others were
#      converted by Excel into decimals (0.247 meaning 24.7%).
#   2. Large numbers are stored as quoted strings with thousands separators
#      ("14,730" instead of 14730).
#   3. Missing values are represented as the string "-".
#   4. Months are formatted with the full month name ("April 2026") rather
#      than the abbreviated form Twitch uses ("Apr 2026"), so a different
#      strptime format string is needed.
#   5. SteamDB prepends a synthetic "Last 30 days" row to its monthly
#      table that summarizes the trailing 30-day window. This is not a
#      real month and would crash datetime parsing, so it must be
#      filtered out before any further processing.

# This script normalizes all four files into clean CSVs whose schema
# aligns with the cleaned Twitch data, ready for joining on (game, month).

import pandas as pd
from pathlib import Path

IN_DIR = Path("data/raw")
OUT_DIR = Path("data/clean")
OUT_DIR.mkdir(parents=True, exist_ok=True)

GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]

# Column names we want in the output. Matches the order the columns appear
# in the SteamDB export. We rename "Peak" and "Average" to be more explicit
# about what they measure (monthly concurrent players), and use snake_case
# throughout for consistency with the cleaned Twitch data.
OUT_COLUMNS = [
    "month",
    "peak_players",
    "peak_gain",
    "peak_pct_gain",
    "avg_players",
    "avg_pct_gain",
]


def parse_pct(val):
    
    # Normalizes the percent-gain columns into a single consistent
    # representation: a percentage as a float (e.g. 13.2 means 13.2%,
    # not 0.132).

    # Excel inconsistently converted these on paste. Some cells kept the
    # percent sign as a string ("-19.80%"), others were silently converted
    # to decimals (0.247 meaning 24.7%, or 43.882 meaning 4388.2% in the
    # case of Lethal Company's launch month). The rule is simple: if the
    # cell still contains a percent sign, the number to its left is already
    # on the percentage scale; otherwise Excel converted it from a percent
    # into its decimal equivalent and we recover the percentage by
    # multiplying by 100.

    # An earlier version of this function tried to use the magnitude of
    # the number to guess which form a value was in, but that breaks for
    # legitimate large percent gains (e.g. a +4388% launch spike), since
    # those land in Excel as 43.882 in decimal form, indistinguishable by
    # magnitude from a real 43.882%. The percent sign is the only reliable
    # signal, so we use it exclusively.
    
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    # Strip thousands separators that may have survived as strings.
    s = s.replace(",", "")

    # Case A: still has the percent sign. The number is already on the
    # percentage scale; just strip the sign.
    if s.endswith("%"):
        s = s[:-1]
        try:
            return float(s)
        except ValueError:
            return pd.NA

    # Case B: no percent sign means Excel auto-converted from a percent
    # to its decimal form. Multiply by 100 to recover the percentage.
    try:
        return float(s) * 100
    except ValueError:
        return pd.NA


def parse_count(val):
    # Parses a count column (Peak, Average, Gain) into an integer.

    # Handles thousands separators that survive as strings ("14,730"), the
    # optional leading "+" sign on Gain values, and the dash placeholder
    # for missing values.
    
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    # Strip thousands separators and the optional leading "+" on Gain.
    s = s.replace(",", "").lstrip("+")

    try:
        return int(float(s))
    except ValueError:
        return pd.NA


def clean_one(game: str) -> pd.DataFrame:
    
    # Loads, cleans, and returns the DataFrame for a single game.
    
    in_path = IN_DIR / f"steamdb_{game}.csv"

    df = pd.read_csv(in_path, header=0)
    df.columns = OUT_COLUMNS

    # Filter out the synthetic "Last 30 days" summary row that SteamDB
    # prepends to the table. This row is not a real month and does not
    # parse with strptime, so leaving it in would either crash the date
    # parser or produce a NaT row that contaminates downstream joins.
    df = df[df["month"].str.strip().str.lower() != "last 30 days"]

    # SteamDB sometimes injects sale or bundle annotations directly into
    # the month cell (observed in vampire_survivors: "July 2024Fanatical:
    # Into Games Bundle 2024"). To tolerate this, extract just the
    # leading "Month Year" portion of each cell with a regex before
    # passing it to the datetime parser. The regex matches a full month
    # name followed by a 4-digit year and ignores anything that follows.
    df["month"] = df["month"].astype(str).str.extract(
        r"^(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})"
    ).agg(" ".join, axis=1)

    # SteamDB writes out the full month name ("April 2026"), so we use
    # %B (full name) here rather than %b (abbreviated) which we used for
    # the Twitch data.
    df["month"] = pd.to_datetime(df["month"], format="%B %Y", errors="coerce")

    # Apply the per-column parsers. These mirror the structure used in
    # clean_twitch.py so that the two pipelines stay readable side by side.
    for col in ("peak_players", "avg_players", "peak_gain"):
        df[col] = df[col].apply(parse_count)

    for col in ("peak_pct_gain", "avg_pct_gain"):
        df[col] = df[col].apply(parse_pct)
        # Round to two decimals to clean up float precision artifacts that
        # appear after multiplying Excel-converted decimals by 100
        # (e.g. 0.247 * 100 producing 24.700000000000003).
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Cast count columns to nullable Int64 so that missing values stay as
    # NaN rather than silently becoming 0. round() before astype to handle
    # any incidental float values produced by parse_count.
    int_cols = ["peak_players", "avg_players", "peak_gain"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    # Sort oldest to newest. SteamDB lists newest first; flipping makes the
    # CSV align with the cleaned Twitch data and with normal time-series
    # convention, simplifying the eventual join on (game, month).
    df = df.sort_values("month").reset_index(drop=True)

    # Tag every row with its game name so that when the four games are
    # concatenated into a master dataset, every row carries its source.
    df.insert(1, "game", game)

    return df # Return the cleaned DataFrame for this game.


def main():
    all_games = []
    for game in GAMES:
        print(f"Cleaning {game}...")
        df = clean_one(game)
        out = OUT_DIR / f"steamdb_{game}_clean.csv"
        df.to_csv(out, index=False)
        print(f"  Wrote {len(df)} rows to {out}")
        all_games.append(df)

    master = pd.concat(all_games, ignore_index=True)
    master_out = OUT_DIR / "steamdb_all_clean.csv"
    master.to_csv(master_out, index=False)
    print(f"\nCombined: {len(master)} rows to {master_out}")

    # Quick sanity check: any column that comes out entirely NaN indicates
    # a parsing rule that did not match the actual data.
    print("\nNon-null counts in combined dataset:")
    print(master.notna().sum().to_string())


if __name__ == "__main__":
    main()
