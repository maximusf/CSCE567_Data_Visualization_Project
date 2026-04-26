# clean_all.py
# by Maximus Fernandez
#
# Orchestrator that runs all three data-cleaning scripts in sequence:
#
#   1. clean_twitch.py  - cleans manually-collected TwitchTracker data
#   2. clean_steam.py   - cleans the SteamDB monthly CSV exports
#   3. clean_google.py  - cleans the Google Trends CSV exports
#
# Each underlying script is self-contained and can be run on its own;
# this file exists so the entire raw-to-clean pipeline can be regenerated
# with a single command. We invoke each script as a subprocess rather than
# importing it, which keeps each cleaner's main() side effects (file I/O,
# console output) properly isolated and avoids any risk of one script's
# global state leaking into another.

import subprocess
import sys
from pathlib import Path

# Order matters only for readability of the console output. The three
# scripts are independent and could run in any order or even in parallel.
SCRIPTS = [
    "clean_twitch.py",
    "clean_steam.py",
    "clean_google.py",
]


def run_script(script: str) -> bool:
    # Runs a single cleaning script as a subprocess and streams its output
    # to the console. Returns True if the script exited with code 0,
    # False otherwise.

    # We use sys.executable rather than a hardcoded "python" so that
    # whichever Python interpreter is running this orchestrator (whether
    # "python", "python3", or a virtual environment binary) is reused for
    # the child process. This avoids the common pitfall of the orchestrator
    # succeeding under one interpreter while a child fails under a
    # different one with missing dependencies.
    
    print(f"\n{'=' * 60}")
    print(f"Running {script}")
    print(f"{'=' * 60}")

    # check=False so we can report each failure explicitly rather than
    # having subprocess raise CalledProcessError and abort the orchestrator
    # before the remaining scripts get a chance to run.
    result = subprocess.run(
        [sys.executable, script],
        check=False,
    )
    return result.returncode == 0


def main():
    # Verify each script exists in the current directory before running
    # anything, so the user gets a clear error up front rather than a
    # confusing "File not found" partway through.
    missing = [s for s in SCRIPTS if not Path(s).exists()]
    if missing:
        print("ERROR: Missing script(s) in current directory:")
        for s in missing:
            print(f"  {s}")
        print("\nMake sure you are running this from the project root and "
              "that all three cleaning scripts are present.")
        sys.exit(1)

    # Run each script in sequence and remember any failures.
    failed = []
    for script in SCRIPTS:
        if not run_script(script):
            failed.append(script)

    # Final summary so the result of the full pipeline is visible at the
    # bottom of the terminal even after lots of intermediate output.
    print(f"\n{'=' * 60}")
    print("Pipeline summary")
    print(f"{'=' * 60}")
    if failed:
        print(f"FAILED: {len(failed)} script(s) did not complete successfully:")
        for s in failed:
            print(f"  {s}")
        sys.exit(1)
    else:
        print(f"All {len(SCRIPTS)} cleaning scripts completed successfully.")
        print("Cleaned data is in data/clean/")


if __name__ == "__main__":
    main()
