#!/usr/bin/env python3
"""This script acquires the most recent AkWarm Energy Library and 
converts it to a SQLite database, writing that database into the 'data01'
folder of this repository.  It also stores the name of the AkWarm Library
in the 'cur-lib-name.txt' file in the 'data01' folder.
"""

import sqlite3
import gzip
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil
import requests

def download_library(output_dir):
    """Downloads the current AkWarm Energy Library, stores the name of the library in 
    'cur-lib-name.txt' in the 'output_dir' directory, and then returns the library as a 
    dictionary of tables.
    """
    # Get name of current AkWarm Energy Library & save it in text file.
    resp = requests.get('https://analysisnorth.com/AkWarm/update_combined/Library_Info.txt')
    cur_lib_name = resp.text.splitlines()[-1].split('\t')[0]
    open(Path(output_dir) / 'cur-lib-name.txt', 'w').write(cur_lib_name)

    # Download the library, decode and decompress it into an XML string.
    resp = requests.get(f'https://analysisnorth.com/AkWarm/update_combined/{cur_lib_name}')
    binaryContent = resp.content
    unencryptedContent = bytearray(x ^ 30 for x in binaryContent)
    uncompressedContent = gzip.decompress(unencryptedContent)
    library_xml = uncompressedContent.decode("utf-8")

    # build a dictionary of energy library tables
    energy_library = {}
    for xml_item in ET.fromstring(library_xml).findall('item'):
        item_key = xml_item.find('key')[0].text
        energy_library[item_key] = [{x.tag: x.text for x in xml_record}
                                    for xml_record in list(xml_item.find('value')[0])]

    return energy_library

def get_clean_vals(rec, flds):
    """Returns a list of the values in the 'rec' record from an Energy Library table.
    'rec' is a dictionary, with field names being the keys.  'flds' are the complete
    list of fields for the table, in the order that the values should be returned in.
    If one of the fields in 'flds' is not present in the record, a None is returned.
    Some value substitions are made (see code), such as substituting the Python constant
    True for the string 'true'.
    """
    final = []
    for f in flds:
        v = rec.get(f, None)
        if v == 'false': v = False
        if v == 'true': v = True
        final.append(v)
    return final

def download_and_convert(output_dir):
    """Downloads the current AkWarm Energy library, converts it to a SQLite database
    and saves it as the file name 'lib.db' in the diretory 'output_dir'
    """

    # path to final library SQLite file
    lib_path = Path(output_dir) / 'lib.db'
    lib_bak_path = Path(str(lib_path) + '.bak')

    # backup the old SQLite database, if present and then delete
    if lib_path.exists():
        shutil.copy(lib_path, lib_bak_path)
        lib_path.unlink()

    try:
        # Download the current library and return as a dictionary of tables.
        lib = download_library(output_dir)

        # Create a new SQLite database from the AkWarm Energy Library.
        conn = sqlite3.connect(lib_path)
        cur = conn.cursor()

        # Loop through the tables in the library
        for tbl in lib.keys():
            print(tbl)
            if tbl.startswith('Pa'): continue

            # Not all the fields may be present in the first record (weirdly).  Pull the
            # field list from the record with the longest field list
            flds = []       # complete list of fields for the table
            for rec in lib[tbl]:
                if len(rec.keys()) > len(flds):
                    flds = rec.keys()

            # When creating the tables in SQLite, make them all NUMERIC tables so that
            # SQLite will attempt to convert values to Integers or Reals if possible.  If
            # the field can't be converted, it will simply be stored as TEXT, as SQLite
            # fields can store any Type (event if designated NUMERIC).
            fields = [f"'{fld}' NUMERIC" for fld in flds]
            field_phrase = ','.join(fields)
            cur.execute(f"CREATE TABLE {tbl} ({field_phrase});")

            # Insert the records into the newly created table
            values = [get_clean_vals(rec, flds) for rec in lib[tbl]]
            val_phrase = ','.join(['?'] * len(fields))
            cur.executemany(f"INSERT INTO {tbl} VALUES ({val_phrase})", values)

            conn.commit()

    except Exception as e:
        if lib_bak_path.exists():
            shutil.copy(lib_bak_path, lib_path)
        raise e

    finally:
        lib_bak_path.unlink(missing_ok=True)  # delete backup file
        try:
            conn.close()
        except:
            pass
