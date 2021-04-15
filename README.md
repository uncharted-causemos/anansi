### Anansi
Anansi is the WM knowledge data ingestion pipelines. It provides a suite of tools for dealing with and transforming INDRA statement and DART CDRs.

#### Prereq
We assume the geolocation reference dataset is in place and ready to be used. To install the geolocation data please see TODO

#### Ussage scenarios
There are three separate scenarios where we would need to ingest knowledge
- Adhoc ingestion, for internal testing or one-off requests
- An initial data ingestion when the corpus is first released to the wild
- Any subsequent incremental changes that are caused by addition of new documents

Despite these different cases, all knowledge ingestion will more or less follow the same sequence of steps:
- Collect new documents
- Transform documents and load onto ElasticSerch
- Collect new statements
- For each statement, transfrom inject document context and geolocation context
- Index into ElasticSearch
