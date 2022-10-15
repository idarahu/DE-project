# Design Document

Sections of the design document:
- RDBMS schema
- RDBMS queries
- Graph schema
- Graph queries
- Graph types of relationships (?)
- Data transformation and enrichment
- Pipeline diagram

## RDBMS schema

```sql
CREATE TABLE IF NOT EXISTS publications (
    id INTEGER PRIMARY KEY,
    submitter TEXT,
    authors TEXT,
    title TEXT,
    comments TEXT,
    journal_ref TEXT,
    doi TEXT,
    report_no TEXT,
    categories TEXT,
    license TEXT,
    abstract TEXT,
    updated_at TIMESTAMP

    -- skipping versions array
    -- skipping authors_parsed array
);
```

From the `.authors_parsed` array:

```sql
CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY,
    first_name TEXT,
    second_name TEXT,
    third_name TEXT,
);
```

From the `.versions` array:

```sql
CREATE TABLE IF NOT EXISTS versions (
    id INTEGER PRIMARY KEY,
    version TEXT,
    created_at TIMESTAMP
);
```

Additional models:

- Journals model (?)
- Publishers model (?)
- Scientific domains model (?)
- Affiliations model (?)

## Data transformation and enrichment

- Parsing of `journal_ref` to get journal name, volume, year
- Parsing of `categories` to get scientific domain
- Using `doi` (Crossref REST API)
  - to get references and reference count
  - to get publisher name
  - to get affiliations (? empty values)
  - to get ISSNs (that should be transformed to query Crossref API for more journal information)


## Graph schema

(https://neo4j.com/docs/getting-started/current/cypher-intro/schema/)

### Entities with attributes

- Author (first_name, second_name, third_name)
- Paper (title, abstract, doi, report_no, license, updated_at)
- Journal (name, volume, year, issn)
- Publisher  (name)
- Scientific domain (name)
- Affiliation (name)

### Relationships

- `AUTHOR_OF` (Author)-[:AUTHOR_OF]->(Paper)
- `PUBLISHED_IN` (Paper)-[:PUBLISHED_IN]->(Journal)
- `PUBLISHED_BY` (Journal)-[:PUBLISHED_BY]->(Publisher)
- `PUBLISHED_IN_DOMAIN` (Paper)-[:PUBLISHED_IN_DOMAIN]->(Scientific domain)
- `PUBLISHED_IN_AFFILIATION` (Paper)-[:PUBLISHED_IN_AFFILIATION]->(Affiliation)
- `CITES` (Paper)-[:CITES]->(Paper)