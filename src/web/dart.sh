#!/usr/bin/env sh

# Clean up
echo "Starting DART CDR extraction"
rm -f ${NAME}-dart.zip
rm -rf ${NAME}-dart_cdr
rm -rf ${WATCH_FOLDER}/${NAME}-dart_cdr.json

# curl -XGET -H "Accept: application/zip" "http://localhost/dart/api/v1/cdrs/archive" -o ${NAME}-dart.zip
curl -XGET -H "Accept: application/zip" "${DART_CDR_URL}" -o ${NAME}-dart.zip
unzip -j ${NAME}-dart.zip -d ${NAME}-dart_cdr 

for f in ${NAME}-dart_cdr/*.json; do
  cat $f >> ${WATCH_FOLDER}/${NAME}-dart_cdr.json
  echo >> ${WATCH_FOLDER}/${NAME}-dart_cdr.json
done

echo "Done DART CDR extraction"
exit 0
