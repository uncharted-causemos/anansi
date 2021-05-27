## Anansi
Anansi is the WM knowledge data ingestion pipelines. It provides a suite of tools for dealing with and transforming INDRA statement and DART CDRs.

### Prereq
We assume the geolocation reference dataset is in place and ready to be used. To install the geolocation data please see TODO

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


### Prefect Background and Commands

The `incremental_pipeline.py` file is decorated and structured to be runnable through our [Prefect server](http://10.65.18.52:8080/default).

There is a [Prefect agent](https://docs.prefect.io/orchestration/agents/overview.html) running on that machine, labelled with `non-dask`. Flows with the same tag will be run by this agent instead of a Dask agent that will attempt to run some tasks in parallel.

Code changes currently need to be manually copied over, including local dependencies like `dart`, `elastic`, etc. GitLab is run on `10.64` and therefore we can't `git clone` from it on the `10.65` machine.

TODO: Set up a script to copy the files over and fetch any dependencies or look into automating this from GitLab directly when code is merged in.

After SSHing into `10.65.18.52`:

#### To rerun the agent:

- `tmux a -t seq-agent`
- CTRL+C to cancel it
- `source flows/.incremental_assembly_secrets.sh` to learn any secrets that have changed
- `PREFECT__ENGINE__EXECUTOR__DEFAULT_CLASS="prefect.executors.LocalExecutor" PYTHONPATH="${PYTHONPATH}:/home/centos/flows" prefect agent local start --api "http://10.65.18.52:4200/graphql" --label "non-dask"` to run the non-dask agent
- (CTRL+B D to exit the tmux session)

#### To run/register the flow:

- Set the `shouldRegister` flag in the python file
- `conda activate prefect-seq` to switch to a python env with the correct dependencies
- `python incremental_assembly.py` to run the file and either run the flow locally or register it with the prefect server

#### To create the agent in the first place:

`conda create -n prefect-seq -c conda-forge "python>=3.8.0" prefect "elasticsearch==7.11.0" "boto3==1.17.18" "smart_open==5.0.0" python-dateutil requests`
