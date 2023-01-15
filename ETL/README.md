# ETL Pipeline

## Extract

The arXiv dataset is available at https://www.kaggle.com/datasets/Cornell-University/arxiv.

We use splits to simulate periodic updates:

```zsh
python split_dataset.py --arxiv_path <path-to-arxiv-dataset-json> --output_dir ./inputs
```

## Transform

...

## Load

### Data Warehouse

#### Pre-requisites

1. Airflow connection to PostgreSQL database:
   ```
   connection_id: citus-warehouse
   conn_type: Postgres
   host: citus-db
   schema: warehouse
   login: citususer
   password: cituspass
   port: 5432
   ```

### Neo4j Graph Database

#### Pre-requisites

1. [manual task] Make sure `neo4j-script` custom Docker image is present:
   ```zsh
    cd ./docker_context_neo4j
    docker build -t neo4j-script -f neo4j-script.dockerfile .
    ```
2. [comes with repo] Make sure neo4j plugins are located at `./neo4j/plugins`
    1. APOC
    2. Graph Data Science Library
3. [comes with docker-compose] Note that `docker-proxy` container exposes the host's `/var/run/docker.sock` to
   containers via TCP (don't know where `docker.sock` is located on Windows machines,
   this [thread](https://stackoverflow.com/questions/36765138/bind-to-docker-socket-on-windows) might help)

#### Run (in Airflow's UI)

After the transformation step,

1. Run the `transform_for_batch_injection` DAG. This will create import CSVs at `./neo4j/import`.
2. Run the `load_graph_db` DAG. This will start a new container with ports 7474 and 7687 exposed. The container might
   take some seconds to start.
3. Go to http://localhost:7474/browser/ and run the following Cypher query to check that the graph was loaded correctly:
   ```cypher
   MATCH (n) RETURN n LIMIT 25
   ```
4. When finished, stop the DAG by marking its `load_container` task with the "Mark Success" button in Airflow's UI. This
   will remove the container. Graph database files are preserved and located at `./neo4j/data`.

# Graph Database Queries

## Basic Queries

Getting an author:

| Question                                             | Cypher Query                                                                                                                                                    | Notes |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| who collaborates with a given author                 | `MATCH (author1:Author)-[:COLLABORATES_WITH]->(author2:Author) WHERE author1.author_id = "224" RETURN author2 LIMIT 25`                                         |       |
| who collaborates with a given author in a given year | `MATCH (author1:Author {author_id: "224"})-[:COLLABORATES_WITH]-(author2:Author)-[:AUTHOR_OF]-(p:Publication {year: 2007}) RETURN author2 LIMIT 25`             |       |
| who writes in a given scientific domain              | `MATCH (author:Author)-[:AUTHOR_OF]->(p:Publication)-[:BELONGS_TO]->(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" RETURN author LIMIT 25`           |       |
| who writes in a given venue                          | `MATCH (author:Author)-[:AUTHOR_OF]->(p:Publication)-[:PUBLISHED_IN]->(v:Venue) WHERE v.full_name = "Lecture Notes in Computer Science" RETURN author LIMIT 25` |       |
| who writes for a given affiliation                   | `MATCH (a:Author)-[:WORKS_IN]-(af:Affiliation) WHERE af.name = "Princeton University" RETURN a LIMIT 25`                                                        |       |

Getting a publication:

| Question                            | Cypher Query                                                                                                                  | Notes |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|-------|
| cited by a given publication        | `MATCH (p:Publication)-[:CITED_BY]->(:Publication {publication_id: "44324"}) RETURN p LIMIT 25`                               |       | 
| cited by a given author             | `MATCH (p:Publication)-[:CITED_BY]->(:Publication)-[:AUTHOR_OF]-(a:Author {author_id: "6616"}) RETURN p LIMIT 25`             |       | 
| published in a given venue          | `MATCH (p:Publication)-[:PUBLISHED_IN]-(v:Venue {full_name: "Lecture Notes in Computer Science"}) RETURN p LIMIT 25`          |       | 
| affiliated with a given affiliation | `MATCH (p:Publication)-[:AUTHOR_OF]-(a:Author)-[:WORKS_IN]-(af:Affiliation {name: "Princeton University"}) RETURN p LIMIT 25` |       | 
| from a given scientific domain      | `MATCH (p:Publication)-[:BELONGS_TO]-(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" RETURN p LIMIT 25`             |       |

Getting an affiliation:

| Question                               | Cypher Query                                                                                                  | Notes |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------|-------|
| that covers a given scientific domain  | `MATCH (a:Affiliation)-[:COVERS]-(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" RETURN a LIMIT 25` |       |
| publishes in a given publication venue | `MATCH (a:Affiliation)-[:PUBLISHES_IN]-(v:Venue) WHERE v.full_name = "Physical Review D" RETURN a LIMIT 25`   |       |
| employs a given author                 | `MATCH (a:Author)-[:WORKS_IN]->(af:Affiliation) WHERE a.full_name = "E. Bloomer" RETURN af LIMIT 25`          |       |

Getting a scientific domain:

| Question                                     | Cypher Query                                                                                                                       | Notes |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-------|
| that is covered by a given affiliation       | `MATCH (:Affiliation {name: "Princeton University"})-[:COVERS]-(d:ScientificDomain) RETURN d LIMIT 25`                             |       |
| that is covered by a given publication venue | `MATCH (d:ScientificDomain)-[:COVERS]-(:Affiliation)-[:PUBLISHES_IN]-(v:Venue {full_name: "Physical Review D"}) RETURN d LIMIT 25` |       |
| that is covered by a given author            | `MATCH (:Author {author_id: "224"})-[:AUTHOR_OF]-(:Publication)-[:BELONGS_TO]-(d:ScientificDomain) RETURN d LIMIT 25`              |       |

Getting a publication venue:

| Question                               | Cypher Query                                                                                                                         | Notes |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-------|
| that covers a given scientific domain  | `MATCH (:ScientificDomain {sub_category: "physical sciences"})-[:COVERS]-(:Affiliation)-[:PUBLISHES_IN]-(v:Venue) RETURN v LIMIT 25` |       |
| that publishes for a given affiliation | `MATCH (a:Affiliation)-[:PUBLISHES_IN]-(v:Venue) WHERE a.name = "Iowa State University" RETURN v LIMIT 25`                           |       |
| that publishes for a given author      | `MATCH (a:Author {author_id: "224"})-[:AUTHOR_OF]-(:Publication)-[:PUBLISHED_IN]-(v:Venue) RETURN v LIMIT 25`                        |       |

## Influential publications using PageRank

To find the most influential publications, we use the [Page Rank](https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/) algorithm.

First, we create a graph projection for using with GDS:

```cypher
CALL gds.graph.project.cypher('influential_publications', 'MATCH (p:Publication) RETURN id(p) AS id', 'MATCH (p1:Publication)-[:CITED_BY]->(p2:Publication) RETURN id(p1) AS source, id(p2) AS target')
```

Then, we run the algorithm:

```cypher
CALL gds.pageRank.stream('influential_publications') 
YIELD nodeId, score 
RETURN gds.util.asNode(nodeId).title AS title, score 
ORDER BY score DESC 
LIMIT 25
```

Results:

<img width="1285" alt="Screenshot 2023-01-15 at 12 47 26" src="https://user-images.githubusercontent.com/6259054/212536500-c62d98c9-cc7b-4783-bb8c-9cd489d1fcb9.png">

## Communities detection using Louvain

To find communities of authors that cover a particular scientific domain, we use the [Louvain](https://neo4j.com/docs/graph-data-science/current/algorithms/louvain/#algorithms-louvain-examples-stream) method from GDS with the following Cypher queries.

First, we project the graph:

```cyper
CALL gds.graph.project.cypher('community_by_domain', 'MATCH (a:Author) RETURN id(a) AS id', 'MATCH (a1:Author)-[:AUTHOR_OF]->(p:Publication)-[:BELONGS_TO]->(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" MATCH (a2:Author)-[:AUTHOR_OF]->(p) WHERE a1 <> a2 RETURN id(a1) AS source, id(a2) AS target')
```

Then, we can write the community_by_domain ID to the authors' nodes as a property:

```cyper
CALL gds.louvain.stream('community_by_domain') 
YIELD nodeId, communityId 
WITH gds.util.asNode(nodeId) AS a, communityId AS communityId SET a.community_by_domain = communityId
```

After that, we query a community where the amount of authors is greater than 1:

```cyper
MATCH (a:Author) 
WHERE a.community_by_domain IS NOT NULL 
WITH a.community_by_domain AS communityId, COUNT(a) AS amount WHERE amount > 1 
RETURN communityId, amount 
ORDER BY amount DESC
```

<img width="1427" alt="Screenshot 2023-01-15 at 12 53 51" src="https://user-images.githubusercontent.com/6259054/212536634-e642feee-d8af-4147-a47f-1549aafd6ee0.png">

Finally, we can take the biggest community and display it with the query:
```cyper
MATCH (a:Author {community_by_domain: 35739}) RETURN a LIMIT 25
```

<img width="1429" alt="Screenshot 2023-01-15 at 12 54 21" src="https://user-images.githubusercontent.com/6259054/212536657-9f554d2c-cc29-431b-9721-297ca293ff79.png">

## Missing links between authors using Delta-Stepping Single-Source Shortest Path

To search for a missing link between two authors, we use the [Single-Source Shortest Path](https://neo4j.com/docs/graph-data-science/current/algorithms/delta-single-source/) from GDS.

First, we create a projection:

```cypher
CALL gds.graph.project.cypher('missing_link', 'MATCH (a:Author) RETURN id(a) AS id', 'MATCH (a1:Author)-[:COLLABORATES_WITH]-(a2:Author) RETURN id(a1) AS source, id(a2) AS target')
```

Then, we pick two authors who have not collaborated with each other, e.g., "T. Nagao" and "T.H. Puzia":

<img width="1427" alt="Screenshot 2023-01-15 at 12 36 55" src="https://user-images.githubusercontent.com/6259054/212536677-2e474212-6ef5-49c9-8ffb-bca78ea301b3.png">

Finally, we find the shortest path between the authors with author_id 36102 and 34512:

```cypher
MATCH (source:Author {author_id: "36102"})
CALL gds.allShortestPaths.delta.stream('missing_link_2', {sourceNode: source})
YIELD index, sourceNode, targetNode, path
WHERE gds.util.asNode(targetNode).author_id = "34512"
RETURN index, gds.util.asNode(sourceNode).full_name AS sourceNodeName, gds.util.asNode(targetNode).full_name AS targetNodeName, nodes(path) as path
ORDER BY index
LIMIT 25
```

<img width="1431" alt="Screenshot 2023-01-15 at 12 29 56" src="https://user-images.githubusercontent.com/6259054/212536728-d1640553-c8f7-4963-a635-bdc5d03e895a.png">
