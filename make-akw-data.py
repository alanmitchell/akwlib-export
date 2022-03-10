#!/usr/bin/env python3
# %%
# Execute this cell prior to any of the cells below
from ast import arg
import os
from glob import glob
from datetime import datetime
import importlib
import csv
import io
import math
import argparse
from pathlib import Path

import pandas as pd
import numpy as np
from fuzzywuzzy import process, fuzz
import util as au  # a utility library in this repo.
#importlib.reload(au)    # needed if you modify the admin_util library

# set up commmand line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-t', '--tmy', help='process TMY files', action="store_true")

args = parser.parse_args()
# %%
# Some functions needed for processing

def closest_tmy(city_ser, dft):
    """Finds the closest TMY3 site, and returns ID and City, State name of
    that TMY3 site.  'city_ser' is a Pandas Series describing the city, and 'dft'
    is a DataFrame of meta data describing the possible TMY sites. 
    """
    dists = au.haversine(city_ser.Latitude, city_ser.Longitude, dft.latitude, dft.longitude)
    closest_id = dists.idxmin()
    tmy_site = dft.loc[closest_id]
    return closest_id, '{}, {}'.format(tmy_site.city, tmy_site.state)


# base file path for processed files
base_path = Path('data/v01')
# %%
# Process TMY3 Files, if requested

if args.tmy:

    meta_list = []

    # Read the Design Heating Temperature data into a DataFrame to 
    # eventually add to the metadata dataframe.
    df_design = pd.read_excel('data/tmy3-raw/design_temps.xlsx', index_col='tmy_id')

    for fn in glob('data/tmy3-raw/*.csv'):
        
        # Use a csvreader just to process the header row
        with open(fn) as csvfile:                    
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
            # print(meta['heating_design_temp'], df_final.db_temp.quantile(0.01))

            base_file = os.path.basename(fn)
            base_no_ext = base_file.split('.')[0]
            meta['url'] = f'v01/tmy3/{base_no_ext}'

            meta_list.append(meta)

            # --- Store the site's DataFrame
            au.save_df(df_final, base_path / f'tmy3/{base_no_ext}')

    df_meta = pd.DataFrame(meta_list)
    df_meta.set_index('tmy_id', inplace=True)
    au.save_df(df_meta, base_path / f'tmy3/tmy3_meta')

#import sys; sys.exit()
# -------------------------------------------------------------------
# Create City and Utility Dataframes 

# %%
# read in the DataFrame that describes the available TMY3 climate files.
df_tmy_meta = pd.read_pickle('data/v01/tmy3/tmy3_meta.pkl', compression='bz2')

# Read in the other City and Utility Excel files.
df_city = pd.read_excel('data/city-util/raw/City.xlsx')
df_city_util_link = pd.read_excel('data/city-util/raw/City Utility Links.xlsx')

# Retrieve the Miscellaneous Information and store into a Pandas Series.
misc_info = pd.read_excel('data/city-util/raw/Misc Info.xlsx').iloc[0]

df_util = pd.read_excel('data/city-util/raw/Utility.xlsx')
df_util.drop(['SiteSourceMultiplierOverride', 'BuybackRate', 'Notes'], axis=1, inplace=True)
df_util.index = df_util.ID
df_util['NameShort'] = df_util['Name'].str[:6]

# make a list of blocks with rates for each utility and save that as
# a column in the DataFrame.
blocks_col = []
for ix, util in df_util.iterrows():
    adjust = au.chg_nonnum(util.FuelSurcharge, 0.0) + au.chg_nonnum(util.PurchasedEnergyAdj, 0.0)
    if util.ChargesRCC:
        adjust += au.chg_nonnum(misc_info.RegSurchargeElectric, 0.0)
    blocks = []
    for blk in range(1, 6):
        block_kwh = au.chg_nonnum(util['Block{}'.format(blk)], math.nan)
        block_rate = au.chg_nonnum(util['Rate{}'.format(blk)], math.nan)
        if not math.isnan(block_rate):
            block_rate += adjust
        blocks.append((block_kwh, block_rate))
    blocks_col.append(blocks)
df_util['Blocks'] = blocks_col

df_city = df_city.query('Active == 1')[[
    'ID',
    'Name',
    'Latitude',
    'Longitude',
    'ERHRegionID',
    'WAPRegionID',
    'ImpCost',
    'FuelRefer',
    'FuelCityID',
    'Oil1Price',
    'Oil2Price',
    'PropanePrice',
    'BirchPrice',
    'SprucePrice',
    'CoalPrice',
    'SteamPrice',
    'HotWaterPrice',
    'MunicipalSalesTax',
    'BoroughSalesTax'
]]
df_city.set_index('ID', inplace=True)

# Find the closest TMY3 site to each city.
# Find the Electric Utilities associated with each city.
# Determine a Natural Gas price for the city if there is 
# a natural gas utility present.
# Put all this information in the City DataFrame.
tmy_ids = []
tmy_names = []
utils = []
gas_prices = []
SELF_GEN_ID = 131   # ID number of "Self-Generation" utility
for ix, city_ser in df_city.iterrows():
    
    # get closest TMY3 site
    id, nm = closest_tmy(city_ser, df_tmy_meta)    
    tmy_ids.append(id)
    tmy_names.append(nm)
    
    # find electric utilities associated with city
    util_list = df_city_util_link.query('CityID == @ix')['UtilityID']
    df_city_utils = df_util.loc[util_list]
    elec_utils = df_city_utils.query('Type==1 and Active==1').copy()
    elec_utils.sort_values(by=['NameShort', 'IsCommercial', 'ID'], inplace=True)
    if len(elec_utils) > 0:
        utils.append(list(zip(elec_utils.Name, elec_utils.ID)))
    else:
        # If there is no Electric Utility associated with this city,
        # assign the self-generation electric utility.
        utils.append([('Self Generation', SELF_GEN_ID)])

    # In AkWarm, there is only PCE data for the residential rate structures.
    # We need to add it to the Commercial rate structures becuase community
    # building may use those rates, and they potentially can get PCE.  So,
    # For each city, look at the utilities and find the PCE value.  Then
    # use that for the rate structures that are missing a PCE value.
    # This code wouldn't work if there were multiple utilities serving a city
    # with different PCE rates.  But that only occurs in the Anchorage area,
    # and there is no PCE there.
    pce_val = elec_utils.PCE.max()
    if pce_val > 0.0:
        for ix, util in elec_utils.iterrows():
            if math.isnan(util.PCE):
                df_util.loc[ix, 'PCE'] = pce_val

    # if there is a gas utility, determine the marginal gas price
    # at a usage of 130 ccf/month, and assign that to the City.
    # This avoids the complication of working with the block rate
    # structure.
    gas_price = math.nan
    gas_utils = df_city_utils.query('Type==2 and Active==1').copy()
    # Use a residential gas utility, the smallest ID
    if len(gas_utils):
        gas_util = gas_utils.sort_values(by=['IsCommercial', 'ID']).iloc[0]
        # get the rate for a usage of 130 ccf
        for block in range(1, 6):
            block_val = gas_util['Block{}'.format(block)]
            #set_trace()
            if math.isnan(block_val) or block_val >= 130:
                gas_price = gas_util['Rate{}'.format(block)] + \
                            au.chg_nonnum(gas_util.FuelSurcharge, 0.0) + \
                            au.chg_nonnum(gas_util.PurchasedEnergyAdj, 0.0)
                break

    gas_prices.append(gas_price)


# Put all the information determined above for the cities into the
# City DataFrame as new columns.
df_city['TMYid'] = tmy_ids
df_city['TMYname'] = tmy_names
df_city['ElecUtilities'] = utils
df_city['GasPrice'] =  gas_prices

# delete out the individual block and rate columns in the utility table,
# and surcharges, as they are no longer needed.
df_util.drop(['Block{}'.format(n) for n in range(1, 6)], axis=1, inplace=True)
df_util.drop(['Rate{}'.format(n) for n in range(1, 6)], axis=1, inplace=True)
df_util.drop(['PurchasedEnergyAdj', 'FuelSurcharge'], axis=1, inplace=True)

# Also have to look to see if a city relies on another city
# for its fuel prices
for ix, cty in df_city.query('FuelRefer > 0').iterrows():
    # get the city referred to
    cty_fuel = df_city.loc[int(cty.FuelCityID)]
    # Transfer over fuel prices
    for c in df_city.columns:
        if c.endswith('Price'):
            df_city.loc[ix, c] = cty_fuel[c]
            

# %% [markdown]
# #### Link Cities to Census Areas and Other Geographic Areas
# 
# Also, determine typical monthly residential consumption for each city.

# %%
# read in the data linking ARIS cities to Census Areas
df_city_to_census = pd.read_csv('other_data/aris_city_to_census_lookups.csv')

# convert Hub column to boolean
df_city_to_census['Hub'] = df_city_to_census.Hub.astype('bool')

# rename some columns
df_city_to_census.rename(columns={'Hub': 'hub', 'ARIS_cities': 'aris_city'}, inplace=True)

df_city_to_census.head()

# %%
# For each city in the main City DataFrame, find the matching ARIS city (using
# fuzzy matching) and add a column for that

aris_cities = df_city_to_census.aris_city.values
matching_cities = []
for akw_cty in df_city.Name:
    mtch_cty, ratio = process.extractOne(akw_cty, aris_cities)
    if ratio >= 90:
        matching_cities.append(mtch_cty)
    else:
        print(f'Closest match for {akw_cty} is {mtch_cty} ({ratio}). Add a better match to aris_city_to_census_lookups.csv.')
        matching_cities.append('')

df_city['aris_city'] = matching_cities
df_city[['Name', 'aris_city']].head()

# %%
# Merge in the Census & Geographic area data
print(len(df_city))
# Need to do the merge this way in order to preserve the Left index
df_city = df_city.join(df_city_to_census.set_index('aris_city'), how='left', on='aris_city')
print(len(df_city))
df_city.iloc[0]

# %%
# read in the data that links Hub cities and Census Areas non-hub cities to 
# average residential use per month.
df_avg_use = pd.read_csv('other_data/monthly_average_kwh_per_res_customer_by_census_area_and_hub.csv')

# Rename some columns
df_avg_use.rename(columns={'Census Area': 'census_area', 'City': 'city'}, inplace=True)

# Make a column with the monthly values as a list, also calc average of the months
uses = []
ann_avg = []
for ix, row in df_avg_use.iterrows():
    mo_uses = [row[str(i)] for i in range(1, 13)]
    avg_use = sum(mo_uses) / 12.0
    uses.append(mo_uses)
    ann_avg.append(avg_use)
df_avg_use['use_list'] = uses
df_avg_use['annual_avg'] = ann_avg
df_avg_use.head()

# %%
# Determine a monthly profile to be used by cities that are not covered.
# Because the above data came from PCE, non-covered cities are primarily Urban.
# Take the average of the Hub cities that have annual usages > 500 kWh/month
df_hubs = df_avg_use.query('city != "non hub"').copy()
df_lg_hubs = df_hubs.query('annual_avg > 500')
df_lg_hubs

# %%
# Average those cities together to get the default usage value.
means = df_lg_hubs.mean()
default_use = [means[str(i)] for i in range(1, 13)]
default_use

# %%
# Add the average use info as a list to the city DataFrame.

# get the list of hub cities that have average usage data
cities = set(df_avg_use.city) - {'non hub'}

# get the list of Census Areas that have non-hub average uses
census_areas = set(df_avg_use.query('city == "non hub"').census_area)

mo_usages = []
for ix, row in df_city.iterrows():
    city = row.Name
    c_area = row.census_area
    is_hub = row.hub
    
    # see if the city is a hub city in the list of cities with data
    if is_hub:
        mtch_cty, ratio = process.extractOne(city, cities)
        if ratio >= 90:
            rec = df_avg_use.query('city == @mtch_cty').iloc[0]
            usages = [rec[str(i)] for i in range(1, 13)]
            mo_usages.append(usages)
        else:
            mo_usages.append(default_use)
    else:
        # non-hub cities
        mtch_census, ratio = process.extractOne(c_area, census_areas)
        if ratio >= 90:
            rec = df_avg_use.query('census_area == @mtch_census and city == "non hub"').iloc[0]
            usages = [rec[str(i)] for i in range(1, 13)]
            mo_usages.append(usages)
        else:
            # No match for census area
            # These are probably cities in the Railbelt that aren't hub, or in SE.
            # So, they probably still have relatively high usage.
            mo_usages.append(default_use)
df_city['avg_elec_usage'] = mo_usages

# %% [markdown]
# #### Save the DataFrames

# %%
# Save the created DataFrames
au.save_df(df_city, 'data/city-util/proc/city')
au.save_df(df_util, 'data/city-util/proc/utility')
au.save_df(misc_info, 'data/city-util/proc/misc_info')  # this routine works with Pandas Series as well

