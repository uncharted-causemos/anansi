import os
import pandas as pd
import numpy as np
from elastic import Elastic


"""
This script is meant to update the geo index.
The data comes from the following URLs:
http://download.geonames.org/export/dump/allCountries.zip
http://clulab.cs.arizona.edu/models/gadm_woredas.txt

Those files are needed locally in the root folder

It keeps the geo_id, name, lat and lon columns, as well as filter out rows where the
feature column contains U, T, R, L, H, S, V.
"""


def filter_rows_and_columns(df, columns):
    return df[~df["feature"].isin(["U", "T", "R", "L", "H", "S", "V"])][columns]


def format_document(df, columns):
    return [{columns[0]: row[0], columns[1]: row[1], columns[2]: row[2], columns[3]: row[3]} for row in df[columns].to_numpy()]

root = os.getcwd()
ES_url = os.environ.get("ES_url")

all_countries_df = pd.read_csv(root + "/allCountries.txt", delimiter = "\t", header=None)
all_countries_df.columns = ["geo_id", "name", "asciiname", "alternatenames", "lat", "lon", "feature",
 "feature code", "country code", "cc2", "admin1 code", "admin2 code", "admin3 code", "admin4 code",
  "population", "elevation", "dem", "timezone", "modification date"]


relevant_cols = ["geo_id", "name", "lat", "lon"]
all_countries_df = filter_rows_and_columns(all_countries_df, relevant_cols)

es = Elastic(ES_url)

es_documents = format_document(all_countries_df, relevant_cols)

es.bulk_write("geo", es_documents, "geo_id")

all_countries_df = None

gadm_woredas_df = pd.read_csv(root + "/gadm_woredas.txt", delimiter = "\t", header=None)
gadm_woredas_df.columns = ["geo_id", "name", "asciiname", "alternatenames", "lat", "lon", "feature",
 "feature code", "country code", "cc2", "admin1 code", "admin2 code", "admin3 code", "admin4 code",
  "population", "elevation", "dem", "timezone", "modification date"]

gadm_woredas_df = filter_rows_and_columns(gadm_woredas_df, relevant_cols)

es_documents = format_document(gadm_woredas_df, relevant_cols)

es.bulk_write("geo", es_documents)
