#!/bin/bash

./akwlib_to_sqlite.py
sqlite3 -header -csv data01/lib.db < queries/city.sql > data01/city.csv
