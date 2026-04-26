# clean_twitch.py
# by Maximus Fernandez
#
# Cleans the four manually-collected TwitchTracker CSVs. Manual copy-paste
# from the website into Excel introduced several formatting inconsistencies
# that pandas cannot handle in a single read_csv call:
#
#   1. Duplicate column headers ("Gain" and "% Gain" each appear twice,
#      once for the viewer columns and once for the stream columns).
#   2. The % Gain columns are inconsistent within a single file: some rows
#      kept the percent sign as a string ("-17.70%"), others were converted
#      by Excel into decimals (0.132 meaning 13.2%).
#   3. Hours Watched uses human-readable suffixes (1.51M, 45.9K, 308) rather
#      than raw integers.
#   4. Large numbers are stored as quoted strings with thousands separators
#      ("2,625" instead of 2625).
#   5. Missing values are represented as the string "-".
#   6. Month is formatted as "Mar 2026" rather than an ISO date.
#
# This script normalizes all four files into clean numeric CSVs ready for
# merging with the SteamDB and Google Trends data later in the pipeline.

import pandas as pd
import re
from pathlib import Path

IN_DIR = Path("data/raw")
OUT_DIR = Path("data/clean")
OUT_DIR.mkdir(parents=True, exist_ok=True)

GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]

# Column names we want in the output. We rename the four duplicates ("Gain",
# "% Gain") to disambiguate viewer-side vs stream-side. The original column
# order in the CSV is fixed (it matches the website's table layout), so we
# can rename positionally.
OUT_COLUMNS = [
    "month",
    "avg_viewers", "viewers_gain", "viewers_pct_gain",
    "peak_viewers",
    "avg_streams", "streams_gain", "streams_pct_gain",
    "peak_streams",
    "hours_watched",
]


def parse_suffixed_number(val):
    # Converts a value like '1.51M', '45.9K', '308', '2,625' to a float.
    # Returns NaN for missing values ('-', empty, or already NaN).

    # The Hours Watched column on TwitchTracker uses K (thousands) and M
    # (millions) suffixes for readability. We need raw integers to do any
    # real analysis.

    # pandas will pass through actual NaN floats; check with pd.isna.
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    # Remove thousands separators that may have survived as strings.
    s = s.replace(",", "")

    # Detect and strip the K/M suffix, then multiply.
    multiplier = 1
    if s.endswith("M"):
        multiplier = 1_000_000
        s = s[:-1]
    elif s.endswith("K"):
        multiplier = 1_000
        s = s[:-1]

    try:
        return float(s) * multiplier
    except ValueError:
        # Anything we cannot parse (unexpected format) becomes NaN rather
        # than silently corrupting the dataset.
        return pd.NA


def parse_pct(val):
    # Normalizes the % Gain column into a single consistent representation:
    # a percentage as a float (e.g. 13.2 means 13.2%, not 0.132).

    # Excel inconsistently converted these on paste. Some cells kept the
    # percent sign as a string ("-17.70%"), others were silently converted
    # to decimals (0.132). We need to detect which form a value is in and
    # normalize both to the same scale.
    
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    # Case A: still has the percent sign. Strip it and we are done.
    if s.endswith("%"):
        s = s[:-1].replace(",", "")
        try:
            return float(s)
        except ValueError:
            return pd.NA

    # Case B: Excel already converted to decimal form. The signature is a
    # value with absolute magnitude less than 10 and a decimal point. Real
    # percentages from the site that were not auto-converted (like "326.00%")
    # always retain their percent sign and hit Case A above. So if we are
    # here with no percent sign, Excel converted it and we multiply by 100.
    s = s.replace(",", "")
    try:
        n = float(s)
    except ValueError:
        return pd.NA

    # Heuristic: any decimal-form value with abs() <= 10 is almost certainly
    # an Excel-converted percentage. Real percentage values in this dataset
    # that were not auto-converted (the ones Case A handles) include large
    # numbers like 326.00% and 800%, but Excel only auto-converts values
    # that look like clean decimals from its perspective. Values without a
    # percent sign are decimal-form percentages.
    if abs(n) <= 10:
        return n * 100
    # If the value is large and lacks a percent sign, it is already in
    # percentage scale (rare, but possible if Excel kept the integer form
    # of something like "200%" as 200).
    return n


def parse_count(val):
    # Parses a count column (Avg Viewers, Peak Viewers, Avg Streams,
    # Peak Streams, and the two Gain columns) into an integer.

    # Handles thousands separators that survive as strings ("2,625") and
    # the dash placeholder for missing values.
    
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    # Strip thousands separators and the optional leading "+" sign that
    # the Gain columns sometimes carry.
    s = s.replace(",", "").lstrip("+")

    try:
        return int(float(s))
    except ValueError:
        return pd.NA


def clean_one(game: str) -> pd.DataFrame:
    # Loads, cleans, and returns the DataFrame for a single game.
    
    in_path = IN_DIR / f"twitch_{game}.csv"

    # We pass header=0 and immediately overwrite the column names because
    # the source file has duplicate "Gain" and "% Gain" headers that pandas
    # would auto-suffix with .1, making downstream code less readable.
    df = pd.read_csv(in_path, header=0)
    df.columns = OUT_COLUMNS

    # Parse each column using the appropriate helper. The order of
    # operations matters here only in that month must stay first.
    df["month"] = pd.to_datetime(df["month"], format="%b %Y", errors="coerce")

    for col in ("avg_viewers", "peak_viewers", "avg_streams",
                "peak_streams", "viewers_gain", "streams_gain"):
        df[col] = df[col].apply(parse_count)

    for col in ("viewers_pct_gain", "streams_pct_gain"):
        df[col] = df[col].apply(parse_pct)

    df["hours_watched"] = df["hours_watched"].apply(parse_suffixed_number)

    # Cast numeric columns to nullable integer / float types so missing
    # values stay as NaN rather than being coerced into 0 or strings.
    # We use Int64 (capital I, the nullable integer type) for all the
    # count columns. Hours Watched goes through the same treatment even
    # though parse_suffixed_number returns floats, because the K/M
    # expansion always yields whole-number-equivalent values (e.g.
    # "1.51M" -> 1510000.0) that round cleanly to integers and match
    # the integer treatment given to the other counts.
    int_cols = ["avg_viewers", "peak_viewers", "avg_streams",
                "peak_streams", "viewers_gain", "streams_gain",
                "hours_watched"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    for col in ("viewers_pct_gain", "streams_pct_gain"):
        # Round to two decimal places. The raw values can come out with
        # float precision artifacts (e.g. 13.200000000000001 from 0.132 * 100
        # in the Excel-decimal branch of parse_pct), which are harmless
        # mathematically but ugly in the CSV.
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Sort oldest to newest. The website lists newest first; flipping makes
    # the CSV align with the SteamDB exports and with normal time-series
    # convention, which simplifies the eventual join.
    df = df.sort_values("month").reset_index(drop=True)

    # Tag the row with the game name so that when we concatenate all four
    # games into a master dataset later, every row carries its source.
    df.insert(1, "game", game)

    return df


def main():
    all_games = []
    for game in GAMES:
        print(f"Cleaning {game}...")
        df = clean_one(game)
        out = OUT_DIR / f"twitch_{game}_clean.csv"
        df.to_csv(out, index=False)
        print(f"  Wrote {len(df)} rows to {out}")
        all_games.append(df)

    # Also write a combined long-format file for easier comparison across
    # games in a single DataFrame later.
    master = pd.concat(all_games, ignore_index=True)
    master_out = OUT_DIR / "twitch_all_clean.csv"
    master.to_csv(master_out, index=False)
    print(f"\nCombined: {len(master)} rows to {master_out}")

    # Print a quick sanity summary so we can spot any column that came out
    # entirely NaN (which would indicate a parsing rule that did not match
    # the actual data).
    print("\nNon-null counts in combined dataset:")
    print(master.notna().sum().to_string())


if __name__ == "__main__":
    main()