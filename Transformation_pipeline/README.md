# ETL Pipeline

## Extract

...

## Transform

...

## Load

### Neo4j Graph Database

#### Pre-requisites

1. [manual task] Make sure `neo4j-script` custom Docker image is present:
   ```zsh
    cd ./neo4j_docker_context
    docker build -t neo4j-script -f neo4j-script.dockerfile .
    ```
2. [comes with repo] Make sure neo4j plugins are located at `./neo4j/plugins`
   1. APOC
   2. Graph Data Science Library
3. [comes with docker-compose] Note that `docker-proxy` container exposes the host's `/var/run/docker.sock` to containers via TCP (don't know where `docker.sock` is located on Windows machines, this [thread](https://stackoverflow.com/questions/36765138/bind-to-docker-socket-on-windows) might help)

#### Run

After the transformation step, 

1. Run the `transform_for_batch_injection_batch` DAG. This will create import CSVs at `./neo4j/import`.
2. Run the `load_graph_db` DAG. This will start a new container with ports 7474 and 7687 exposed. The container might take some seconds to start.
3. Go to http://localhost:7474/browser/ and run the following Cypher query to check that the graph was loaded correctly:
   ```cypher
   MATCH (n) RETURN n LIMIT 25
   ```
4. When finished, stop the DAG by marking its `load_container` task with the "Mark Success" button in Airflow's UI. This will remove the container. Graph database files are preserved and located at `./neo4j/data`.