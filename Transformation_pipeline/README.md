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

Getting an author:

| Question                                             | Cypher Query                                                                                                                                                    | Notes |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| who collaborates with a given author                 | `MATCH (author1:Author)-[:COLLABORATES_WITH]->(author2:Author) WHERE author1.author_id = "224" RETURN author2 LIMIT 25`                                         |       |
| who collaborates with a given author in a given year | `MATCH (author1:Author {author_id: "224"})-[:COLLABORATES_WITH]-(author2:Author)-[:AUTHOR_OF]-(p:Publication {year: 2007}) RETURN author2 LIMIT 25`             |       |
| who writes in a given scientific domain              | `MATCH (author:Author)-[:AUTHOR_OF]->(p:Publication)-[:BELONGS_TO]->(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" RETURN author LIMIT 25`           |       |
| who writes in a given venue                          | `MATCH (author:Author)-[:AUTHOR_OF]->(p:Publication)-[:PUBLISHED_IN]->(v:Venue) WHERE v.full_name = "Lecture Notes in Computer Science" RETURN author LIMIT 25` |       |
| who writes for a given affiliation                   | `MATCH (a:Author)-[:WORKS_IN]-(af:Affiliation) WHERE af.name = "Princeton University" RETURN a LIMIT 25`                                                        |       |

Getting a publication:

| Question                            | Cypher Query                                                                                                                  | Notes           |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|-----------------|
| cited by a given publication        | ``                                                                                                                            | Not enough data | 
| cited by a given author             | ``                                                                                                                            | Not enough data | 
| published in a given venue          | `MATCH (p:Publication)-[:PUBLISHED_IN]-(v:Venue {full_name: "Lecture Notes in Computer Science"}) RETURN p LIMIT 25`          |                 | 
| affiliated with a given affiliation | `MATCH (p:Publication)-[:AUTHOR_OF]-(a:Author)-[:WORKS_IN]-(af:Affiliation {name: "Princeton University"}) RETURN p LIMIT 25` |                 | 
| from a given scientific domain      | `MATCH (p:Publication)-[:BELONGS_TO]-(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" RETURN p LIMIT 25`             |                 |

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

What is the most influential publication:

| Question                     | Cypher Query                                                                                                                                                                                                                                               | Notes           |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|
| in a given year              | `MATCH (p:Publication)-[:CITED_BY]->(cited:Publication) WHERE p.year = 2007 WITH p, COUNT(cited) AS citedCount ORDER BY citedCount DESC LIMIT 1 RETURN p`                                                                                                  | Not enough data |
| in a given scientific domain | `MATCH (p:Publication)-[:BELONGS_TO]-(d:ScientificDomain) WHERE d.sub_category =~ "computer.*" MATCH (p:Publication)-[:CITED_BY]->(cited:Publication) WITH p, COUNT(DISTINCT cited) AS citedCount ORDER BY citedCount DESC LIMIT 1 RETURN p`               | Not enough data |
| in a given venue             | `MATCH (p:Publication)-[:PUBLISHED_IN]-(v:Venue) WHERE v.full_name = "International Journal of Astrobiology" MATCH (p:Publication)-[:CITED_BY]->(cited:Publication) WITH p, COUNT(DISTINCT cited) AS citedCount ORDER BY citedCount DESC LIMIT 1 RETURN p` | Not enough data |
| in a given affiliation       | `MATCH (af:Affiliation {name: "Princeton University"})-[:WORKS_IN]-(:Author)-[:AUTHOR_OF]-(p:Publication)-[:CITED_BY]->(cited:Publication) WITH p, COUNT(cited) AS citedCount ORDER BY citedCount DESC LIMIT 1 RETURN p`                                   | Not enough data |

What is a community of authors:

`CALL gds.graph.project('community_domain', 'Author', {COLLABORATES_WITH: {orientation: 'UNDIRECTED'}})`

| Question                                     | Cypher Query | Notes |
|----------------------------------------------|--------------|-------|
| that covers a given scientific domain        | ``           |       |
| that publishes in a given publication venue? | ``           |       |
| that publishes for a given affiliation?      | ``           |       |

Other queries:

| Question                                                                                                | Cypher Query | Notes |
|---------------------------------------------------------------------------------------------------------|--------------|-------|
| Which author has the most self-citations (or citations to other authors from the same affiliation)?     | ``           |       |
| Which author has the most collaborations?                                                               | ``           |       |
| Is there a connection between co-authors and where they publish their papers?                           | ``           |       |
| What is the missing link between two authors from different affiliations who have not collaborated yet? | ``           |       |