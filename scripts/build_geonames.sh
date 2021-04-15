#!/usr/bin/env bash

###
# Create a consolidated file for geo data ingestion in wm-iass
# Presumes you have curl and zip utilities available
###

BASE="http://download.geonames.org/export/dump/allCountries.zip"
EXTRA="http://clulab.cs.arizona.edu/models/gadm_woredas.txt"

echo "Downloading extra geodata file"
curl $EXTRA -o extra.txt

echo "Downloading base geodata file"
curl $BASE -o base.zip


echo "### Clean up"
unzip base.zip

echo "### Rows before"
wc allCountries.txt

echo "### Concatenating"
cat extra.txt >> allCountries.txt

echo "### Rows after"
wc allCountries.txt

echo "### Recreating zip"
rm geo.zip
zip geo.zip allCountries.txt

echo "### Cleanup"
rm extra.txt
rm base.zip
rm allCountries.txt
