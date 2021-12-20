#!/usr/bin/env python3
# %%
import sqlite3
import gzip
import xml.etree.ElementTree as ET
from pathlib import Path

# %%
def import_library(lib_file):
    # extract the XML representation of the energy library file
    with open(lib_file, mode='rb') as lib:
        binaryContent = bytearray(lib.read())
    unencryptedContent = bytearray(x ^ 30 for x in binaryContent)
    uncompressedContent = gzip.decompress(unencryptedContent)
    library_xml = uncompressedContent.decode("utf-8")

    # build a dictionary of energy library entities
    energy_library = {}
    for xml_item in ET.fromstring(library_xml).findall('item'):
        item_key = xml_item.find('key')[0].text
        energy_library[item_key] = [{x.tag: x.text for x in xml_record}
                                    for xml_record in list(xml_item.find('value')[0])]

    return energy_library

def get_clean_vals(rec, flds):
    final = []
    for f in flds:
        v = rec.get(f, None)
        if v == 'false': v = False
        if v == 'true': v = True
        final.append(v)
    return final

# %%
lib = import_library('2021-08-31.lib')

# %%
Path('data/lib.db').unlink(missing_ok=True)
conn = sqlite3.connect('data/lib.db')
cur = conn.cursor()

for tbl in lib.keys():
    print(tbl)
    if tbl.startswith('Pass'): continue
    # Not all the fields may be present in the first record (weirdly).  Pull the
    # field list from the record with the longest field list
    flds = []
    for rec in lib[tbl]:
        if len(rec.keys()) > len(flds):
            flds = rec.keys()

    fields = [f"'{fld}' NUMERIC" for fld in flds]
    field_phrase = ','.join(fields)
    values = [get_clean_vals(rec, flds) for rec in lib[tbl]]
    cur.execute(f"CREATE TABLE {tbl} ({field_phrase});")
    val_phrase = ','.join(['?'] * len(fields))
    cur.executemany(f"INSERT INTO {tbl} VALUES ({val_phrase})", values)
    conn.commit()
