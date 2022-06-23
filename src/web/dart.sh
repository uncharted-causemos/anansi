#!/usr/bin/env bash 

# Clean up
echo "Starting DART CDR extraction"
rm -f ${NAME}-dart.zip
rm -rf ${NAME}-dart_cdr
rm -rf ${WATCH_FOLDER}/${NAME}-dart_cdr.json

# curl -XGET -H "Accept: application/zip" "http://localhost/dart/api/v1/cdrs/archive" -o ${NAME}-dart.zip
curl -XGET -H "Accept: application/zip" "http://10.64.16.209:4005/dart-may-2021/auto-test.zip" -o ${NAME}-dart.zip
unzip -j ${NAME}-dart.zip -d ${NAME}-dart_cdr 

for f in ${NAME}-dart_cdr/*.json; do
  cat $f >> ${WATCH_FOLDER}/${NAME}-dart_cdr.json
  echo >> ${WATCH_FOLDER}/${NAME}-dart_cdr.json
done

echo "Done DART CDR extraction"
exit 0
