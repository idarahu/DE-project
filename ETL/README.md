# Getting Started with the ETL pipeline

This document describes how to prepare and execute the ETL pipeline.

## Extract

The arXiv dataset is available at https://www.kaggle.com/datasets/Cornell-University/arxiv.

We use splits to simulate periodic updates. The following command splits the dataset into splits of 50k publications and saves them to `./airflow/data/inputs`:

```zsh
$ python split_dataset.py --arxiv_path <path-to-arxiv-dataset-json> --output_dir ./airflow/data/inputs
```

## Transform

### Prerequisites

1. [manual task] Airflow connection to PostgreSQL database:
   ```
   connection_id: airflow_pg
   conn_type: Postgres
   host: postgres
   login: airflow
   password: airflow
   port: 5432
   ```
2. [builds with docker-compose] Custom-built Airflow Docker. It should be built with `docker-compose`. Manual build can be done with the following commands:
   ```zsh
   $ cd docker_context_airflow
   $ docker build -t airflow-custom .
   ```

### Run (in Airflow's UI)

Run the following DAGs in the following order:

1. `transform_create_tables`
2. `transform_articles`

## Load

### Data Warehouse

#### Pre-requisites

1. [manual task] Airflow connection to PostgreSQL database:
   ```
   connection_id: citus_warehouse
   conn_type: Postgres
   host: citus-db
   schema: warehouse
   login: citususer
   password: cituspass
   port: 5432
   ```


#### Run (in Airflow's UI)

After the transformation step, run:

1. `load_dwh_db`
2. Connect to the database that was mapped to the `localhost:25432` using the following connection string:
   ```
   postgresql://citususer:cituspass@localhost:25432/warehouse
   ```
   The schema `warehouse` should be added manually if tables aren't shown in your database client.

### Neo4j Graph Database

#### Pre-requisites

1. [manual task] Make sure `neo4j-script` custom Docker image is present:
   ```zsh
    $ cd ./docker_context_neo4j
    $ docker build -t neo4j-script -f neo4j-script.dockerfile .
    ```
2. [comes with repo] Make sure neo4j plugins are located at `./neo4j/plugins`
    1. APOC
    2. Graph Data Science Library
3. [builds with docker-compose] Note that `docker-proxy` container exposes the host's `/var/run/docker.sock` to
   containers via TCP (don't know where `docker.sock` is located on Windows machines,
   this [thread](https://stackoverflow.com/questions/36765138/bind-to-docker-socket-on-windows) might help)

#### Run (in Airflow's UI)

After the transformation step, run:

1. Run the `transform_for_graph_injection` DAG. This will create import CSVs at `./neo4j/import`.
2. Run the `load_graph_db` DAG. This will start a new container with ports 7474 and 7687 exposed. The container might
   take some seconds to start.
3. Go to http://localhost:7474/browser/ and run the following Cypher query to check that the graph was loaded correctly:
   ```cypher
   MATCH (n) RETURN n LIMIT 25
   ```
4. When finished, stop the DAG by marking its `load_container` task with the "Mark Success" button in Airflow's UI. This
   will remove the container. Graph database files are preserved and located at `./neo4j/data`.
