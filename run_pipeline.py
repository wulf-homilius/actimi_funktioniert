#!/usr/bin/env python3
"""
Pipeline script to run the Actimi to Sensdoc synchronization process.

This script executes the following steps in order:
1. Add the fixed code to matching patients in Sensdoc (put_given_code.py)
2. Synchronize observations from Actimi to Sensdoc (actimi_to_sensdoc.py)
3. Fetch and display observations for verification (Patient_main_obs.py)

Run this script from the project root directory.
"""

import subprocess
import sys
from pathlib import Path

def run_script(script_name: str, *args: str) -> int:
    """Run a Python script and return its exit code."""
    cmd = [sys.executable, script_name] + list(args)
    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent)
        return result.returncode
    except Exception as e:
        print(f"Error running {script_name}: {e}")
        return 1

def main() -> int:
    print("Starting Actimi to Sensdoc pipeline...")

    Step 1: Add code to patients
    print("\nStep 1: Adding fixed code to patients...")
    if run_script("import_requests.py") != 0:
        print("Step 1 failed. Aborting.")
        return 1

    """# Step 2: Synchronize observations
    print("\nStep 2: Synchronizing observations...")
    if run_script("Patient_main_obs.py") != 0:
        print("Step 2 failed. Aborting.")
        return 1"""

    # Step 3: Fetch and display observations
    print("\nStep 3: Fetching observations for verification...")
    if run_script("actimi_to_sensdoc.py") != 0:
        print("Step 3 failed. Aborting.")
        return 1

    print("\nPipeline completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())