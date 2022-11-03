# Design


## DWH

### Queries
- Getting authors (or ranking them)
    - with the most publications in a given year, scientific domain and/or publication venue
    - with the most citations in a given year, scientific domain and/or publication venue
    - with the highest h-index in a give time period
    - with the broadest horizon (authors who have written papers in the largest amount of different scientific domains)
- Getting institutions (or ranking them)
    - with the most publications in a given year, scientific domain and/or publication venue
    - that have the highest impact in scientific world (institutions that have papers which have been cited the most in a given year, scientific domain and/or publication venue)
- Getting publications (or ranking them)
    - with the  most citations in a given year, scientific domain and/or publication venue
- Getting journals (or ranking them)
    - with the highest h-index in a given year and/or scientific domain 
- What are the year's hottest topics (categories of scientific disciplines)?
- How does the number of publications on a given topic change during a given time frame (histograms of the number of publications on a given topic over a given period of time)?
- Who is the author whose h-index has increased the most during the given time?
- Which journal h-index has increased the most during the given time?

### Schema
Our designed data warehouse (DWH) for storing data about scientific publications would have the following schema containing a fact table "PUBLICATIONS" and five dimension tables: "AUTHORS", "AUTHORS' AFFILIATIONS", "PUBLICATION VENUES", "SCIENTIFIC DOMAINS" and "TIME".

![image](https://user-images.githubusercontent.com/102286655/199743726-0b463af2-a1e9-4ea5-b0b7-6fa78740bc0d.png)

The fact table "PUBLICATIONS" will store the primary keys of dimension tables (or dimension group keys in cases where bridge tables are used) as foreign keys together with additional information (like title, DOI etc.) about the record. 

In the dimension table "AUTHORS", all the relevant data about the publications' authors (name and h-index) will be stored. Since one author can have several publications and one publication can have several authors (many-to-many relationship), the bridge table will be used to connect the author's dimension with specific facts. Additionally, the h-index of an author is a variable that changes over time. For BI queries (for example, getting the author whose h-index increased the most during the last year), tracking that change is essential. Therefore, the type 2 slowly changing dimensions concept is used – when the author's h-index changes, a new dimension record is generated. At the same time, the old record will be assigned a non-active effective date, and the new record will be assigned an active effective date. The bridge table will also contain effective and expiration timestamps to avoid incorrect linkages between authors and publications.

In the dimension table "AUTHORS' AFFILIATIONS", information about the institutions (name and location) of the authors of the publications will be gathered. Similarly to the authors' dimension, in this case, there could be a many-to-many relationship between the dimension and fact. In other words, there could be many publications from one institution, and authors of the same publication can have different affiliations. Therefore, the bridge table will be used to connect the dimension table records with the fact table records.

Dimension table "PUBLICATION VENUES" will store data about the venues of the publications. In this table, there could be many fields that do not apply to all the records. For example, if the type is "book", the field "h_index_calculated" is irrelevant. However, if the field h-index is applicable (for journals), similarly to the "AUTHORS" dimension table, tracking its changes is essential from the BI point of view. Therefore, this table will also use the type 2 slowly changing dimensions concept. 

In the dimension table "SCIENTIFIC DOMAINS", the categories (three levels) of scientific disciplines of publications will be gathered. Again, the bridge table will be used to overcome the shortcomings related to many-to-many relationships between publications and scientific domains (one publication can belong to many scientific domains, and many publications can have the same domain).

The "TIME" dimension will hold all the relevant (from the BI point of view) time information about the publications. Besides the timestamp of the publication, it also has separate fields for year, month and day. 

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
