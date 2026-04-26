# clean_google.py
# by Maximus Fernandez
#
# Cleans the four Google Trends CSV exports. Trends exports are already
# clean (ISO dates, integer 0 to 100 scores). Only renaming and game
# tagging is needed.
#
# Note for analysis: Trends scores are normalized per search term, so
# 100 is the per-term peak. Values are not comparable in absolute terms
# across games, only useful for spotting timing of awareness spikes
# within a single game.

import pandas as pd
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
IN_DIR = ROOT_DIR / "data/raw"
OUT_DIR = ROOT_DIR / "data/clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)

GAMES = ["among_us", "fall_guys", "vampire_survivors", "lethal_company"]


def clean_one(game: str) -> pd.DataFrame:
    in_path = IN_DIR / f"google_{game}.csv"

    # Rename positionally because Google Trends names the score column
    # after the search term, which differs per file.
    df = pd.read_csv(in_path, header=0)
    df.columns = ["month", "trends_score"]

    df["month"] = pd.to_datetime(df["month"], errors="coerce")

    # Nullable Int64 so parse failures land as NaN instead of 0.
    df["trends_score"] = pd.to_numeric(df["trends_score"], errors="coerce").astype("Int64")

    df = df.sort_values("month").reset_index(drop=True)
    df.insert(1, "game", game)

    return df


def main():
    all_games = []
    for game in GAMES:
        print(f"Cleaning {game}...")
        df = clean_one(game)
        out = OUT_DIR / f"trends_{game}_clean.csv"
        df.to_csv(out, index=False)
        print(f"  Wrote {len(df)} rows to {out}")
        all_games.append(df)

    master = pd.concat(all_games, ignore_index=True)
    master_out = OUT_DIR / "trends_all_clean.csv"
    master.to_csv(master_out, index=False)
    print(f"\nCombined: {len(master)} rows to {master_out}")

    print("\nNon-null counts in combined dataset:")
    print(master.notna().sum().to_string())


if __name__ == "__main__":
    main()
