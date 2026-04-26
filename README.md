# The Streamer Effect

Final project for CSCE 567 (Data Visualization). Argues that Twitch viewership spikes causally preceded Steam player count surges across four case-study games: Among Us, Fall Guys, Vampire Survivors, and Lethal Company.

**Live site:** _(Vercel link goes here once deployed)_

## Data sources

- **TwitchTracker** - monthly viewership stats. Manually collected (no CSV export).
- **SteamDB** - monthly concurrent player stats. CSV export per game.
- **Google Trends** - monthly search interest. CSV export per game, date range chosen per game to preserve normalization.

Raw files live in `data/raw/`. Cleaned files land in `data/clean/`.

## Project structure

```
.
├── data/
│   ├── raw/            # source CSVs (manual + downloads)
│   └── clean/          # cleaned + merged outputs
├── viz/                # D3.js single-page app
├── clean_twitch.py     # cleans TwitchTracker CSVs
├── clean_steam.py      # cleans SteamDB CSVs
├── clean_google.py     # cleans Google Trends CSVs
├── merge_data.py       # joins sources, computes lag + growth summaries
└── run_pipeline.py     # runs all four scripts in order
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

1. Dual-axis line chart per game (Twitch viewers + Steam players + Trends score).
2. Horizontal bar chart of lag days.
3. Scatter plot of monthly Twitch viewers vs Steam players, colored by game.
4. Vertical bar chart of post-peak player growth (log scale).

Local preview:

```bash
cd viz
python -m http.server 8000
```

Then open `http://localhost:8000`.