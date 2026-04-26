# run_pipeline.py
# by Maximus Fernandez
#
# Runs the full raw-to-master pipeline:
#   1. tools/clean_twitch.py    cleans TwitchTracker CSVs
#   2. tools/clean_steam.py     cleans SteamDB CSVs
#   3. tools/clean_google.py    cleans Google Trends CSVs
#   4. tools/merge_data.py      joins cleaned sources, writes master.csv,
#                               lag_summary.csv, growth_summary.csv
#
# Cleaners are independent. merge_data must run after them since it
# reads their outputs. Steps run sequentially as subprocesses to keep
# console output ordered and avoid global state leaking between scripts.

import subprocess
import sys
from pathlib import Path

SCRIPTS = [
    "tools/clean_twitch.py",
    "tools/clean_steam.py",
    "tools/clean_google.py",
    "tools/merge_data.py",
]


def run_script(script: str) -> bool:
    # Uses sys.executable so the child uses the same interpreter as
    # the orchestrator (avoids "python" vs "python3" vs venv mismatch).
    print(f"\n{'=' * 60}", flush=True)
    print(f"Running {script}", flush=True)
    print(f"{'=' * 60}", flush=True)

    # check=False so each failure is reported explicitly and the merge
    # step can be skipped cleanly when a cleaner fails.
    result = subprocess.run([sys.executable, script], check=False)
    return result.returncode == 0


def main():
    # Verify every script exists up front so the user gets a clear
    # error rather than a confusing failure partway through.
    missing = [s for s in SCRIPTS if not Path(s).exists()]
    if missing:
        print("ERROR: Missing script(s) in current directory:")
        for s in missing:
            print(f"  {s}")
        print("\nMake sure you are running this from the project root and "
              "that all four scripts are present.")
        sys.exit(1)

    # Skip merge if any cleaner failed. Cleaners themselves are
    # independent so we keep going to surface as many failures as
    # possible in one run.
    failed = []
    for script in SCRIPTS:
        # Special case: merge_data depends on cleaner outputs, so
        # skip it if any cleaner failed. Mark it as failed too so the
        # final summary reflects that the pipeline did not complete.
        if script == "tools/merge_data.py" and failed:
            print(f"\n{'=' * 60}", flush=True)
            print("Skipping tools/merge_data.py because one or more cleaners failed.", flush=True)
            print(f"{'=' * 60}", flush=True)
            failed.append(script)
            continue
        if not run_script(script):
            failed.append(script)

    print(f"\n{'=' * 60}", flush=True)
    print("Pipeline summary", flush=True)
    print(f"{'=' * 60}", flush=True)
    if failed:
        print(f"FAILED: {len(failed)} script(s) did not complete successfully:")
        for s in failed:
            print(f"  {s}")
        sys.exit(1)
    else:
        print(f"All {len(SCRIPTS)} scripts completed successfully.")
        print("Cleaned data and master files are in data/clean/")


if __name__ == "__main__":
    main()
