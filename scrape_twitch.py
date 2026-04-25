# scrape_twitch.py
# by Maximus Fernandez


# Scrapes monthly statistics tables from TwitchTracker for the four case
# study games used in the "Streamer Effect" project. TwitchTracker does not
# offer a CSV export, and its tables are rendered client-side by DataTables.js
# behind a Cloudflare bot check, so a plain HTTP request will not work.
# Playwright drives a real Chromium browser to clear Cloudflare and let the
# JavaScript template render the table, then we read raw values directly
# from the DOM.

from playwright_stealth import stealth_sync

from playwright.sync_api import sync_playwright
import pandas as pd
import time
from pathlib import Path

# Game IDs come from the TwitchTracker URL path:
# https://twitchtracker.com/games/{id}
GAMES = {
    "among_us": "510218",
    "fall_guys": "512980",
    "vampire_survivors": "1833694612",
    "lethal_company": "2085980140",
}

OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Column order matches the order of <td> cells in each row of the
# TwitchTracker table. Confirmed against the page's source template
# (the script tag with id="sbm-template").
COLUMNS = [
    "month", "avg_viewers", "gain", "pct_gain",
    "peak_viewers", "avg_streams", "streams_gain", "streams_pct_gain",
    "peak_streams", "hours_watched",
]


def scrape_game(page, game_id: str) -> pd.DataFrame:
    """
    Loads one game's TwitchTracker page and returns a DataFrame of its
    monthly statistics table. Assumes Cloudflare has already been cleared
    on this browser context (see main()).
    """
    url = f"https://twitchtracker.com/games/{game_id}"
    print(f"  Loading {url}")

    # domcontentloaded fires before all subresources finish, which is fine
    # because we explicitly wait for the table selector below.
    page.goto(url, wait_until="domcontentloaded")

    # Some Cloudflare challenges appear only after landing on the specific
    # game page rather than on the site homepage. Give the user a second
    # manual checkpoint here before we start waiting for the table.
    input("If a Cloudflare check appears on the game page, solve it in the "
          "browser, then press Enter here to continue... ")

    # The table is built client-side from a JS template, so it does not exist
    # in the initial HTML. Wait for at least one row to be inserted before
    # trying to read the table. 60 seconds gives Cloudflare time to clear
    # if a fresh challenge appears.
    page.wait_for_selector("#DataTables_Table_0 tbody tr", timeout=60_000)

    # The table sits inside a scroll container with overflow:auto and a fixed
    # max-height of 450px. DataTables.js usually renders all rows into the
    # DOM up front rather than virtualizing, but we scroll to the bottom of
    # the inner container as a safety measure in case any rows are deferred.
    # We scroll the container, not the window, because the page itself is
    # not what is overflowing.
    page.evaluate("""
        const sb = document.querySelector('.dataTables_scrollBody');
        if (sb) sb.scrollTop = sb.scrollHeight;
    """)
    time.sleep(1)  # short pause for any lazy row insertion to settle

    # Read each row by pulling the data-order attribute from each <td>.
    # We prefer data-order over innerText because:
    #   1. Numbers are stored raw (e.g. data-order="1510959") rather than
    #      formatted for display (e.g. "1.51M"), so no string parsing needed.
    #   2. The Month cell stores an ISO date (e.g. "2026-04-01") rather
    #      than a localized "Mar 2026" string, which is unambiguous.
    #
    # However, data-order has one quirk: when a cell shows "-" (meaning
    # "no value", typically the oldest row in the table where there is no
    # prior month to compute gain against), the data-order is set to "0".
    # That would silently corrupt the dataset by turning missing values into
    # real zeros. So we check innerText first and emit null when the cell
    # visibly displays "-" or is empty.
    rows = page.evaluate("""
        () => {
            const trs = document.querySelectorAll('#DataTables_Table_0 tbody tr');
            return Array.from(trs).map(tr => {
                const tds = tr.querySelectorAll('td');
                return Array.from(tds).map(td => {
                    const txt = td.innerText.trim();
                    if (txt === '-' || txt === '') return null;
                    return td.getAttribute('data-order');
                });
            });
        }
    """)

    df = pd.DataFrame(rows, columns=COLUMNS)
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerces the scraped string values into proper datetime and numeric
    types, and sorts oldest to newest.
    """
    # The month column comes in as ISO strings like "2026-04-01" thanks to
    # data-order. Anything that fails to parse (should not happen, but
    # defensive) becomes NaT.
    df["month"] = pd.to_datetime(df["month"], errors="coerce")

    # Convert every other column from string to numeric. Nulls (cells that
    # displayed "-") stay as NaN, which is the correct representation for
    # missing data and what pandas operations expect.
    numeric_cols = [c for c in df.columns if c != "month"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # TwitchTracker shows newest month at the top. Sorting ascending makes
    # the CSV line up with how time series are normally analyzed and how
    # SteamDB exports its data, which simplifies the eventual merge.
    df = df.sort_values("month").reset_index(drop=True)
    return df


def main():
    with sync_playwright() as p:
        # We use launch_persistent_context instead of launch + new_context so
        # that cookies and Cloudflare clearance tokens persist to a folder
        # on disk (.pw_profile). On the first run, the user solves the
        # Cloudflare challenge once. On every subsequent run, the saved
        # cookies let the same browser skip the challenge entirely.
        #
        # headless=False is required because Cloudflare's bot detection
        # reliably blocks headless Chromium. A visible window also lets the
        # user solve any interactive challenge by hand if one appears.
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=".pw_profile",
            headless=False,
            viewport={"width": 1400, "height": 900},
        )
        page = ctx.new_page()

        # Hit the homepage first so that Cloudflare's challenge, if any,
        # runs once at the start rather than four separate times during the
        # scrape loop. After this clears, the cookie applies site-wide.
        print("Loading homepage to clear Cloudflare if needed...")
        page.goto("https://twitchtracker.com/", wait_until="domcontentloaded")
        input("If a Cloudflare check appears, solve it in the browser, "
              "then press Enter here to continue... ")

        for name, gid in GAMES.items():
            print(f"\nScraping {name}")
            try:
                df = scrape_game(page, gid)
                df = clean(df)
                out = OUT_DIR / f"twitch_{name}.csv"
                df.to_csv(out, index=False)
                print(f"  Saved {len(df)} rows to {out}")
            except Exception as e:
                # Catch per-game so a single failure does not abort the
                # other three. Common causes: Cloudflare reissuing a
                # challenge mid-loop, or selector timeout if the page
                # structure changes.
                print(f"  Failed: {e}")

            # Small delay between games to avoid hitting rate limits and to
            # be a reasonably polite scraper. TwitchTracker is a small site
            # and there is no need to hammer it for four pages.
            time.sleep(3)

        ctx.close()


if __name__ == "__main__":
    main()
