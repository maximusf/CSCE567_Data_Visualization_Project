# The Streamer Effect

Final project for CSCE 567 (Data Visualization). Argues that Twitch viewership spikes causally preceded Steam player count surges across four case-study games: Among Us, Fall Guys, Vampire Survivors, and Lethal Company.

**Live site:** https://streamer-effect-dv.vercel.app/

## Data sources

- **TwitchTracker** - monthly viewership stats. Manually collected (no CSV export).
- **SteamDB** - monthly concurrent player stats. CSV export per game.
- **Google Trends** - monthly search interest. CSV export per game, date range chosen per game to preserve normalization.
- **Streamer events** - hand-curated, sourced from news articles. Used for clickable annotations on viz 1.

Raw files live in `data/raw/`. Cleaned files land in `data/clean/`.

## Project structure

```text
.
|-- data/
|   |-- raw/              # source CSVs (manual + downloads)
|   `-- clean/            # cleaned + merged outputs
|-- tools/                # data collection + cleaning helpers
|   |-- clean_twitch.py   # cleans TwitchTracker CSVs
|   |-- clean_steam.py    # cleans SteamDB CSVs
|   |-- clean_google.py   # cleans Google Trends CSVs
|   |-- merge_data.py     # joins sources, computes lag + growth summaries
|   `-- scrape_twitch.py  # archived scraping attempt / reference
|-- web/                  # D3.js single-page site
`-- run_pipeline.py       # runs the pipeline from the project root
```

## Running the pipeline

Requires Python 3.10+ and pandas.

```bash
pip install pandas
python run_pipeline.py
```

Regenerates everything in `data/clean/` from `data/raw/`.

## Outputs

- `data/clean/master.csv` - all sources joined on (game, month).
- `data/clean/lag_summary.csv` - days between Twitch peak and Steam peak per game.
- `data/clean/growth_summary.csv` - Steam player growth in the month after Twitch peak.

## Visualizations

Built with D3.js. Four charts:

1. Per-game timeline: three normalized series (Twitch viewers, Steam players, Trends score) on a shared axis with streamer event annotations.
2. Gantt-style lag chart showing each game's Twitch peak and Steam peak on a 2018-2026 calendar.
3. Vertical bar chart of post-peak Steam player growth on a log scale.
4. Per-month scatter plot of Twitch viewers vs Steam players with pooled and split-by-game trend lines.

Local preview from the project root:

```bash
python -m http.server 8000
```

Then open `http://localhost:8000/web/`.
