# NOT WORKING
# Cloudflare bot protection blocked this approach. Tables were
# manually copied into Excel and cleaned there. Cleaned CSVs live in
# data/raw and the cleaning code is kept here for reference.

# scrape_twitch.py
# by Maximus Fernandez
#
# Scrapes monthly statistics from TwitchTracker. The site does not
# offer CSV export and the table is rendered client-side by
# DataTables.js behind a Cloudflare check, so plain HTTP does not
# work. Playwright drives a real Chromium browser to clear Cloudflare
# and let the table render before reading values from the DOM.

from playwright_stealth import stealth_sync # to add later if revisited

from playwright.sync_api import sync_playwright
import pandas as pd
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

# Game IDs from the TwitchTracker URL path.
GAMES = {
    "among_us": "510218",
    "fall_guys": "512980",
    "vampire_survivors": "1833694612",
    "lethal_company": "2085980140",
}

OUT_DIR = ROOT_DIR / "data/raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Order matches the <td> cells per row.
COLUMNS = [
    "month", "avg_viewers", "gain", "pct_gain",
    "peak_viewers", "avg_streams", "streams_gain", "streams_pct_gain",
    "peak_streams", "hours_watched",
]


def scrape_game(page, game_id: str) -> pd.DataFrame:
    url = f"https://twitchtracker.com/games/{game_id}"
    print(f"  Loading {url}")

    page.goto(url, wait_until="domcontentloaded")

    # Some Cloudflare checks fire on the game page rather than the
    # homepage. Pause for a manual solve if needed.
    input("If a Cloudflare check appears on the game page, solve it in the "
          "browser, then press Enter here to continue... ")

    # Table is built client-side. Wait for at least one row to exist.
    page.wait_for_selector("#DataTables_Table_0 tbody tr", timeout=60_000)

    # Scroll the inner container in case rows are deferred.
    page.evaluate("""
        const sb = document.querySelector('.dataTables_scrollBody');
        if (sb) sb.scrollTop = sb.scrollHeight;
    """)
    time.sleep(1)

    # Pull the data-order attribute per cell. data-order stores raw
    # values (1510959 not "1.51M", ISO date not "Mar 2026"), so no
    # string parsing is needed. Quirk: when a cell shows "-",
    # data-order is "0", which would silently turn missing values
    # into real zeros. So innerText is checked first and a null is
    # emitted when the cell visibly displays "-".
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
    df["month"] = pd.to_datetime(df["month"], errors="coerce")

    numeric_cols = [c for c in df.columns if c != "month"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("month").reset_index(drop=True)
    return df


def main():
    with sync_playwright() as p:
        # Persistent context so Cloudflare clearance cookies survive
        # across runs. Headless is off because Cloudflare reliably
        # blocks headless Chromium and a visible window also lets the
        # user solve any interactive challenge by hand.
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(ROOT_DIR / ".pw_profile"),
            headless=False,
            viewport={"width": 1400, "height": 900},
        )
        page = ctx.new_page()

        # Hit the homepage first so any Cloudflare challenge fires
        # once at the start instead of four times during the loop.
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
                # Catch per-game so one failure does not abort the
                # other three. Common causes: Cloudflare reissuing a
                # challenge mid-loop, or selector timeout from a page
                # structure change.
                print(f"  Failed: {e}")

            time.sleep(3)

        ctx.close()


if __name__ == "__main__":
    main()
