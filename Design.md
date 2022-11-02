# Design


## DWH

### Queries

### Schema

Entities:
- Author
- Publication
- Affiliation
- ScientificDomain
- Venue


### Change Management Policy

### Technologies


## Graph

To design the graph database, we use the labeled property graph model, because we like to think of nodes as objects with properties. It makes the graph look more concise than using the RDF model. The database is designed to be able to answer the following queries:

- Getting an author
    - who collaborates with a given author
    - who collaborates with a given author in a given year
    - who writes in a given scientific domain
    - who writes in a given venue
    - who writes for a given affiliation

- Getting a publication:
    - cited by a given publication
    - cited by a given author
    - published in a given venue
    - affiliated with a given affiliation
    - from a given scientific domain

- Getting an affiliation
    - that covers a given scientific domain
    - publishes in a given publication venue
    - employs a given author

- Getting a scientific domain
    - that is covered by a given affiliation
    - that is covered by a given publication venue
    - that is covered by a given author

- Getting a publication venue
    - that covers a given scientific domain
    - that publishes for a given affiliation
    - that publishes for a given author

Besides that, we also want to answer:

- What's a community of authors
    - that covers a given scientific domain?
    - that publishes in a given publication venue?
    - that publishes for a given affiliation?
- What author has the most self-citations (citations to other others from the same affiliation)?
- What author has the most collaborations?
- Is there a connection between collaborators and where they publish their papers?

### Schema

![Graph Schema](graph_diagram/out/graph/Graph.png)

#### Entities with properties

- Author
    - DWH ID
    - Name
- Publication
    - DWH ID
    - Title
    - Year
    - Venue
    - DOI
- Affiliation
    - DWH ID
    - Name
- ScientificDomain
    - DWH ID
    - Name
- Venue
    - DWH ID
    - Name

#### Relationships

- `AUTHOR_OF` (Author)-[:AUTHOR_OF]->(Paper)
- `COLLABORATES_WITH` (Author)-[:COLLABORATES_WITH]->(Author)
- `WORKS_AT` (Author)-[:WORKS_AT]->(Affiliation)

- `PUBLISHED_IN` (Publication)-[:PUBLISHED_IN]->(Venue)
- `PUBLISHED_BY` (Publication)-[:PUBLISHED_BY]->(Author)
- `CITED_BY` (Publication)-[:CITED_BY]->(Publication)]
- `CITES` (Publication)-[:CITES]->(Publication)
- `BELONGS_TO` (Publication)-[:COVERS]->(ScientificDomain)

- `COVERS` (Affiliation)-[:COVERS]->(ScientificDomain)
- `EMPLOYS` (Affiliation)-[:EMPLOYS]->(Author)
- `COLLABORATES_WITH` (Affiliation)-[:COLLABORATES_WITH]->(Affiliation)
- `PUBLISHED_IN` (Affiliation)-[:PUBLISHED_IN]->(Venue)

- `COVERED_BY` (ScientificDomain)-[:COVERED_BY]->(Affiliation)
- `COVERED_BY` (ScientificDomain)-[:COVERED_BY]->(Venue)
- `CONTAINS` (ScientificDomain)-[:CONTAINS]->(Publication)

### Technologies

Neo4j with Cypher as query language.

## Data Transformation

...


## Pipeline

...