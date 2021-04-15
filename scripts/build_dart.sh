#!/usr/bin/env bash

# TwoSix CDR download
# 1 - download zip to tmp
# 2 - extract and rebulid json-l
# 3 - clean up

# Clean up
rm raw_data.zip
rm -rf tmp

mkdir tmp

# Download
DART="https://wm-ingest-pipeline-rest-1.prod.dart.worldmodelers.com/dart/api/v1"
USERNAME="XXXXXXX"
PASS="XXXXXXX"
AUTH=`echo -n $USERNAME:$PASS | base64`

curl -XGET \
  -H "Accept: application/zip"  \
  -H "Authorization: Basic $AUTH" \
  "$DART/cdrs/archive" -o raw_data.zip

# Extract
unzip -j raw_data.zip -d tmp

rm dart_cdr.json
for f in tmp/*.json; do
  cat $f >> dart_cdr.json
  echo >> dart_cdr.json
done
