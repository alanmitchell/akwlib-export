#!/usr/bin/env python3
"""Main script for downloading and processing current AkWarm library data.
"""

import argparse
from pathlib import Path
import subprocess

# libraries in this project
import akwlib_to_sqlite
import tmy
import city

# set up commmand line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-t', '--tmy', help='process TMY files in addition to AkWarm data', action="store_true")

args = parser.parse_args()

# output directory for processed files
out_path = Path('data/v01')

# --- Process TMY3 Files, if requested
if args.tmy:
    tmy.process_tmy('data/tmy3-raw', out_path / 'tmy3')

# --- Download AkWarm library and convert to SQLite database
akwlib_to_sqlite.download_and_convert(out_path)

# --- Create City and Utility Dataframes 
city.process_city_data(out_path)

# Commit any changed files to the GitHub repo.  No commits or pushes occur if none
# of the files have changed.
subprocess.run('git add .', shell=True, check=False)   # stage any changed files
lib_name = open('data/v01/cur-lib-name.txt').read()
subprocess.run(f'git commit -m "Files from {lib_name} AkWarm library."', shell=True, check=False)
subprocess.run('git push', shell=True, check=False)
