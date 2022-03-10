"""Utilities for ak-energy-data administrative tasks.
"""
import os
import math
import numbers
from glob import glob

def clear_dir(dir_path):
    """Deletes all the files found in the 'dir_path' directory on the
    local machine, except for a file named '.gitignore'.  NOTE: glob
    naturally ignores files starting with a dot (.).
    """
    for fn in glob(os.path.join(dir_path, '*')):
        if os.path.isfile(fn):
            print(f'deleting {fn}')
            os.remove(fn)

def save_df(df, dest_path):
    """Saves locally a Pandas DataFrame, 'df', as both a bz2 compressed Pickle file
    and a CSV file.  The path to the saved file is 'dest_path', but the extension
    '.pkl' is added for the Pickle version, and the extension '.csv' is added for the
    CSV version.
    """
    print(f'saving DataFrame to {dest_path}.pkl and .csv')
    df.to_pickle(f'{dest_path}.pkl', compression='bz2')
    df.to_csv(f'{dest_path}.csv')

def chg_nonnum(val, sub_val):
    """Changes a nan or anything that is not a number to 'sub_val'.  
    Otherwise returns val.
    """
    if isinstance(val, numbers.Number):
        if math.isnan(val):
            return sub_val
        else:
            return val
    else:
        return sub_val

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between a point on earth
    and an array of other points.  Lat/Lon in decimal degrees.
    lat1 & lon1 are the single point, lat2 and lon2 are numpy
    arrays.
    """
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2. * np.arcsin(np.sqrt(a))
    km = 6367. * c
    return km

