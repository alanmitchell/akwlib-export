"""Module to do processing of TMY3 files into Pandas dataframes and CSV files.
"""
from datetime import datetime
import csv
from pathlib import Path

import pandas as pd

import util as au  # a utility library in this repo.

def process_tmy(raw_tmy_dir, output_dir):
    """Takes raw TMY files and some supplemental files in the 'raw_tmy_dir' and processes
    them into Pandas DataFrames and CSV files that are written to the 'output_dir'.
    """

    print('Processing TMY files...\n')

    raw_path = Path(raw_tmy_dir)
    out_path = Path(output_dir)

    meta_list = []

    # Read the Design Heating Temperature data into a DataFrame to 
    # eventually add to the metadata dataframe.
    df_design = pd.read_excel(raw_path / 'design_temps.xlsx', index_col='tmy_id')

    for f_path in raw_path.glob('*.csv'):
        
        # Use a csvreader just to process the header row
        with open(f_path) as csvfile:                    
            tmyreader = csv.reader(csvfile)
            hdr = next(tmyreader)
            meta = dict(
                tmy_id = int(hdr[0]),
                city = hdr[1].strip(),
                state = hdr[2].strip(),
                utc_offset = float(hdr[3]),
                latitude = float(hdr[4]),
                longitude = float(hdr[5]),
                elevation = float(hdr[6]) * 3.28084   # in feet
            )

            # read the rest of the lines into a DataFrame
            df = pd.read_csv(csvfile)

            # start making final DataFrame
            df['db_temp'] = df['Dry-bulb (C)'] * 1.8 + 32.0   # deg F
            df['rh'] = df['RHum (%)']                         # 0 - 100
            df['wind_spd'] = df['Wspd (m/s)'] * 2.23694     # miles per hour
            df_final = df[['db_temp', 'rh', 'wind_spd']].copy()

            # make a list of date/times with the stamp occurring in the
            # middle of the hour associated with the data.  Also, use 
            # the year 2018 for all the timestamps
            ts = []
            for dt, tm in zip(df['Date (MM/DD/YYYY)'], df['Time (HH:MM)']):
                m, d, _ = dt.split('/')
                h, _ = tm.split(':')
                ts.append( datetime(2018, int(m), int(d), int(h) - 1, 30))

            df_final.index = ts
            df_final.index.name = 'timestamp'
            df_final['month'] = df_final.index.month

            meta['db_temp_avg'] = df_final.db_temp.mean()
            meta['rh_avg'] = df_final.rh.mean()
            meta['wind_spd_avg'] = df_final.wind_spd.mean()
            
            # If available, add the Design Heating Temperature to the metadata;
            # If not available, calculate it from the 1% temperature value
            try:
                meta['heating_design_temp'] = df_design.loc[meta['tmy_id']].htg_design_temp
            except:
                meta['heating_design_temp'] = df_final.db_temp.quantile(0.01)

            base_no_ext = f_path.stem

            meta_list.append(meta)

            # --- Store the site's DataFrame
            au.save_df(df_final, out_path / base_no_ext)

    df_meta = pd.DataFrame(meta_list)
    df_meta.set_index('tmy_id', inplace=True)
    au.save_df(df_meta, out_path / 'tmy3_meta')
