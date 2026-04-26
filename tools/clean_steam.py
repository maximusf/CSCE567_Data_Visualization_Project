# clean_steam.py
# by Maximus Fernandez
#
# Cleans the four manually-collected SteamDB monthly CSVs. Issues to
# fix in the raw exports:
#   1. % Gain inconsistent within a file: some rows kept the percent
#      sign as a string ("-19.80%"), others were converted by Excel
#      into decimals (0.247 meaning 24.7%).
#   2. Large numbers stored as quoted strings with thousands separators.
#   3. Missing values shown as "-".
#   4. Months use full month name ("April 2026") not the abbreviated
#      form Twitch uses.
#   5. SteamDB prepends a "Last 30 days" summary row that is not a real
#      month and would crash the date parser if left in.

import pandas as pd
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
IN_DIR = ROOT_DIR / "data/raw"
OUT_DIR = ROOT_DIR / "data/clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)

GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]

OUT_COLUMNS = [
    "month",
    "peak_players",
    "peak_gain",
    "peak_pct_gain",
    "avg_players",
    "avg_pct_gain",
]


def parse_pct(val):
    # Normalizes percent gain to a float on the percentage scale
    # (13.2 means 13.2%, not 0.132).
    #
    # The percent sign is the only reliable signal of which form a value
    # is in. Magnitude does not work because legitimate large gains
    # (a +4388% launch spike) land in Excel as 43.882 in decimal form,
    # indistinguishable from a real 43.882%.

    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s in ("", "-"):
        return pd.NA

    s = s.replace(",", "")

    if s.endswith("%"):
        s = s[:-1]
        try:
            return float(s)
        except ValueError:
            return pd.NA

    # No percent sign: Excel auto-converted from a percent to decimal.
    # Multiply by 100 to recover the percentage scale.
    try:
        return float(s) * 100
    except ValueError:
        return pd.NA


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
    in_path = IN_DIR / f"steamdb_{game}.csv"

    df = pd.read_csv(in_path, header=0)
    df.columns = OUT_COLUMNS

    # Drop the synthetic "Last 30 days" summary row.
    df = df[df["month"].str.strip().str.lower() != "last 30 days"]

    # SteamDB sometimes appends sale or bundle text to the month cell
    # (e.g. "July 2024Fanatical: Into Games Bundle 2024"). Extract just
    # the leading "Month Year".
    # str.extract with two capture groups returns a 2-column DataFrame
    # (month name, year). agg(" ".join, axis=1) recombines them into
    # a single "Month Year" string per row for the date parser.
    df["month"] = df["month"].astype(str).str.extract(
        r"^(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})"
    ).agg(" ".join, axis=1)

    df["month"] = pd.to_datetime(df["month"], format="%B %Y", errors="coerce")

    for col in ("peak_players", "avg_players", "peak_gain"):
        df[col] = df[col].apply(parse_count)

    for col in ("peak_pct_gain", "avg_pct_gain"):
        df[col] = df[col].apply(parse_pct)
        # Round to clean up float artifacts from the decimal-to-percent
        # conversion (0.247 * 100 producing 24.700000000000003).
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Nullable Int64 so missing values stay NaN rather than become 0.
    int_cols = ["peak_players", "avg_players", "peak_gain"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    # SteamDB lists newest first. Sort ascending to match the other
    # cleaners and standard time series order.
    df = df.sort_values("month").reset_index(drop=True)

    df.insert(1, "game", game)

    return df


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

    print("\nNon-null counts in combined dataset:")
    print(master.notna().sum().to_string())


if __name__ == "__main__":
    main()
