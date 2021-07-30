## Anansi
Anansi is the WM knowledge data ingestion pipelines. It provides a suite of tools for dealing with and transforming INDRA statement and DART CDRs.

### Prereq
In all steps we assume the geolocation reference dataset is in place and ready to be used as it doesn't change very often and doesn't need to be updated every data load. 
To install the geolocation data please see `src/geo_loader.py`

### Usage scenarios
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

### Main scripts
- `src/knowledge_pipeline.py`: This is an adhoc pipeline used for dev purposes
- `src/incremental_pipeline.py`: This is a Prefect-based pipeline for incremental-assembly


### Loading new INDRA data
This section describes the steps to load INDRA dataset. Assumes geo index is ready and populated.
- Ensure you have the right environement and dependencies, e.g. virutualenv
- Download the DART document archive. This will create a `dart_cdr.json` JSON-L file. Note you will need DART user/pass credentials, they are the same as the ones found here: https://gitlab.uncharted.software/WM/wm-env/-/blob/main/dev/causemos.env

```
./scripts/build_dart.sh
```

- Download the correct INDRA dataset, each dataset should have two files: a statements file anda  metadatafile. Note you will need the correct AWS credentials in ~/.aws/credentials, ask your team lead.

```
python scripts/download_indra_s3.py
```

- Run the following command upated with the paths from above.

```
SOURCE_ES="<ES_URL>" \
TARGET_ES="<ES_URL>" \
DART_DATA="<PATH_TO_dart_cdr.json>" \
INDRA_DATASET="<PATH_TO_INDRA_DIRECTORY>" \
python src/knowledge_pipeline.py
```


### Prefect Background and Commands

The `incremental_pipeline.py` file is decorated and structured to be runnable through our [Prefect server](http://10.65.18.52:8080/default).

There is a [Prefect agent](https://docs.prefect.io/orchestration/agents/overview.html) running on that machine, labelled with `non-dask`. Flows with the same tag will be run by this agent instead of a Dask agent that will attempt to run some tasks in parallel.

Code changes currently need to be manually copied over, including local dependencies like `dart`, `elastic`, etc. GitLab is run on `10.64` and therefore we can't `git clone` from it on the `10.65` machine.

TODO: Set up a script to copy the files over and fetch any dependencies or look into automating this from GitLab directly when code is merged in.

After SSHing into `10.65.18.52`:

#### To rerun the agent:
```
# 1. Connext to tmux session
tmux a -t seq-agent

# 2. Cancel sesssion CTRL+C

# 3. Refresh environment
source flows/.incremental_assembly_secrets.sh

# 4. Restart agent
PREFECT__ENGINE__EXECUTOR__DEFAULT_CLASS="prefect.executors.LocalExecutor" PYTHONPATH="${PYTHONPATH}:/home/centos/flows" prefect agent local start --api "http://10.65.18.52:4200/graphql" --label "non-dask"

# 5. Detach tmux session (CTRL+B D)
```

#### To run/register the flow:
```
# 1. Set the "shouldRegister" flag in the python file

# 2. Activate sequential/anansi environment
conda activate prefect-seq

# 3. Register flow
python incremental_assembly.py
```

#### To create the agent in the first place:

`conda create -n prefect-seq -c conda-forge "python>=3.8.0" prefect "elasticsearch==7.11.0" "boto3==1.17.18" "smart_open==5.0.0" python-dateutil requests`
