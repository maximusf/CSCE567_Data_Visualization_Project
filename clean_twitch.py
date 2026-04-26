# clean_twitch.py
# by Maximus Fernandez
#
# Cleans the four manually-collected TwitchTracker CSVs. Issues to fix:
#   1. Duplicate "Gain" and "% Gain" headers (one pair for viewers, one
#      pair for streams).
#   2. % Gain inconsistent within a file: some rows kept the percent
#      sign ("-17.70%"), others were converted by Excel to decimals
#      (0.132 meaning 13.2%).
#   3. Hours Watched uses K/M suffixes (1.51M, 45.9K).
#   4. Large numbers stored as quoted strings with thousands separators.
#   5. Missing values shown as "-".
#   6. Month formatted as "Mar 2026" not ISO.

import pandas as pd
import re
from pathlib import Path

IN_DIR = Path("data/raw")
OUT_DIR = Path("data/clean")
OUT_DIR.mkdir(parents=True, exist_ok=True)

GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]

# Rename the duplicate Gain/% Gain columns to disambiguate viewer-side
# from stream-side. Order matches the website's table layout.
OUT_COLUMNS = [
    "month",
    "avg_viewers", "viewers_gain", "viewers_pct_gain",
    "peak_viewers",
    "avg_streams", "streams_gain", "streams_pct_gain",
    "peak_streams",
    "hours_watched",
]


def parse_suffixed_number(val):
    # Converts '1.51M', '45.9K', '308', '2,625' to a number.
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    s = s.replace(",", "")

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
        return pd.NA


def parse_pct(val):
    # Normalizes % Gain to a float on the percentage scale
    # (13.2 means 13.2%, not 0.132).
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    if s.endswith("%"):
        s = s[:-1].replace(",", "")
        try:
            return float(s)
        except ValueError:
            return pd.NA

    s = s.replace(",", "")
    try:
        n = float(s)
    except ValueError:
        return pd.NA

    # No percent sign: usually an Excel-converted decimal. Values with
    # abs <= 10 are treated as decimal form. Larger bare values are
    # assumed to already be on the percent scale.
    if abs(n) <= 10:
        return n * 100
    return n


def parse_count(val):
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    s = s.replace(",", "").lstrip("+")

    try:
        return int(float(s))
    except ValueError:
        return pd.NA


def clean_one(game: str) -> pd.DataFrame:
    in_path = IN_DIR / f"twitch_{game}.csv"

    # header=0 reads the first row as headers, then df.columns =
    # OUT_COLUMNS overwrites them positionally. Needed because the
    # source has duplicate "Gain" / "% Gain" headers that pandas
    # would otherwise auto-suffix with .1.
    df = pd.read_csv(in_path, header=0)
    df.columns = OUT_COLUMNS

    df["month"] = pd.to_datetime(df["month"], format="%b %Y", errors="coerce")

    for col in ("avg_viewers", "peak_viewers", "avg_streams",
                "peak_streams", "viewers_gain", "streams_gain"):
        df[col] = df[col].apply(parse_count)

    for col in ("viewers_pct_gain", "streams_pct_gain"):
        df[col] = df[col].apply(parse_pct)

    df["hours_watched"] = df["hours_watched"].apply(parse_suffixed_number)

    # Nullable Int64 so missing values stay NaN rather than become 0.
    int_cols = ["avg_viewers", "peak_viewers", "avg_streams",
                "peak_streams", "viewers_gain", "streams_gain",
                "hours_watched"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    for col in ("viewers_pct_gain", "streams_pct_gain"):
        # Round to clean up float artifacts from decimal-to-percent
        # conversion (0.132 * 100 producing 13.200000000000001).
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Website lists newest first. Sort ascending to match the other
    # cleaners and standard time series order.
    df = df.sort_values("month").reset_index(drop=True)

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

    master = pd.concat(all_games, ignore_index=True)
    master_out = OUT_DIR / "twitch_all_clean.csv"
    master.to_csv(master_out, index=False)
    print(f"\nCombined: {len(master)} rows to {master_out}")

    print("\nNon-null counts in combined dataset:")
    print(master.notna().sum().to_string())


if __name__ == "__main__":
    main()
