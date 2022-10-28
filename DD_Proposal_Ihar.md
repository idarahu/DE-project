# Design Document

Sections of the design document:

- RDBMS schema
- RDBMS queries
- Graph schema
- Graph queries
- Graph types of relationships (?)
- Data transformation and enrichment
- Pipeline diagram


## Additional data description

- [arXiv API](https://arxiv.org/help/api/user-manual#_lt_summary_gt_lt_author_gt_and_lt_category_gt)
- [arXiv some fields explanation](https://arxiv.org/help/prep)

## Additional data sources

- [arXiv catagory taxonomy](https://arxiv.org/category_taxonomy)

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


### Crossref Publication data by DOI

```
curl -X GET "https://api.crossref.org/works/10.1103%2FPhysRevD.76.013009" -H "accept: application/json"
```

```json
{
  "status": "ok",
  "message-type": "work",
  "message-version": "1.0.0",
  "message": {
    "indexed": {
      "date-parts": [
        [
          2022,
          9,
          10
        ]
      ],
      "date-time": "2022-09-10T01:50:13Z",
      "timestamp": 1662774613813
    },
    "reference-count": 46,
    "publisher": "American Physical Society (APS)",
    "issue": "1",
    "license": [
      {
        "start": {
          "date-parts": [
            [
              2007,
              7,
              31
            ]
          ],
          "date-time": "2007-07-31T00:00:00Z",
          "timestamp": 1185840000000
        },
        "content-version": "vor",
        "delay-in-days": 0,
        "URL": "http://link.aps.org/licenses/aps-default-license"
      }
    ],
    "content-domain": {
      "domain": [],
      "crossmark-restriction": false
    },
    "short-container-title": [
      "Phys. Rev. D"
    ],
    "DOI": "10.1103/physrevd.76.013009",
    "type": "journal-article",
    "created": {
      "date-parts": [
        [
          2007,
          7,
          31
        ]
      ],
      "date-time": "2007-07-31T19:58:41Z",
      "timestamp": 1185911921000
    },
    "source": "Crossref",
    "is-referenced-by-count": 46,
    "title": [
      "Calculation of prompt diphoton production cross sections at Fermilab Tevatron and CERN LHC energies"
    ],
    "prefix": "10.1103",
    "volume": "76",
    "author": [
      {
        "given": "C.",
        "family": "Balázs",
        "sequence": "first",
        "affiliation": []
      },
      {
        "given": "E. L.",
        "family": "Berger",
        "sequence": "additional",
        "affiliation": []
      },
      {
        "given": "P.",
        "family": "Nadolsky",
        "sequence": "additional",
        "affiliation": []
      },
      {
        "given": "C.-P.",
        "family": "Yuan",
        "sequence": "additional",
        "affiliation": []
      }
    ],
    "member": "16",
    "published-online": {
      "date-parts": [
        [
          2007,
          7,
          31
        ]
      ]
    },
    "reference": [
      {
        "key": "PhysRevD.76.013009Cc1R1",
        "doi-asserted-by": "publisher",
        "DOI": "10.1103/PhysRevLett.95.022003"
      },
      {
        "key": "PhysRevD.76.013009Cc2R1",
        "doi-asserted-by": "publisher",
        "DOI": "10.1016/j.physletb.2006.04.017"
      },
      {
        "key": "PhysRevD.76.013009Cc3R1",
        "doi-asserted-by": "crossref",
        "first-page": "013008",
        "DOI": "10.1103/PhysRevD.76.013008",
        "volume": "76",
        "author": "P. Nadolsky",
        "year": "2007",
        "journal-title": "Phys. Rev. D",
        "ISSN": "http://id.crossref.org/issn/0556-2821",
        "issn-type": "print"
      }
    ],
    "container-title": [
      "Physical Review D"
    ],
    "original-title": [],
    "language": "en",
    "link": [
      {
        "URL": "http://link.aps.org/article/10.1103/PhysRevD.76.013009",
        "content-type": "unspecified",
        "content-version": "vor",
        "intended-application": "syndication"
      },
      {
        "URL": "http://harvest.aps.org/v2/journals/articles/10.1103/PhysRevD.76.013009/fulltext",
        "content-type": "unspecified",
        "content-version": "vor",
        "intended-application": "similarity-checking"
      }
    ],
    "deposited": {
      "date-parts": [
        [
          2017,
          6,
          17
        ]
      ],
      "date-time": "2017-06-17T17:33:18Z",
      "timestamp": 1497720798000
    },
    "score": 1,
    "resource": {
      "primary": {
        "URL": "https://link.aps.org/doi/10.1103/PhysRevD.76.013009"
      }
    },
    "subtitle": [],
    "short-title": [],
    "issued": {
      "date-parts": [
        [
          2007,
          7,
          31
        ]
      ]
    },
    "references-count": 46,
    "journal-issue": {
      "issue": "1",
      "published-print": {
        "date-parts": [
          [
            2007,
            7
          ]
        ]
      }
    },
    "URL": "http://dx.doi.org/10.1103/physrevd.76.013009",
    "relation": {},
    "ISSN": [
      "1550-7998",
      "1550-2368"
    ],
    "issn-type": [
      {
        "value": "1550-7998",
        "type": "print"
      },
      {
        "value": "1550-2368",
        "type": "electronic"
      }
    ],
    "subject": [
      "Nuclear and High Energy Physics"
    ],
    "published": {
      "date-parts": [
        [
          2007,
          7,
          31
        ]
      ]
    },
    "article-number": "013009"
  }
}
```

### Crossref Journal data sample by ISSN

```
curl -X GET "https://api.crossref.org/journals/15507998" -H "accept: application/json"
```

```json
{
    "status": "ok",
    "message-type": "journal",
    "message-version": "1.0.0",
    "message": {
        "last-status-check-time": 1665821306541,
        "counts": {
            "current-dois": 0,
            "backfile-dois": 50391,
            "total-dois": 50391
        },
        "breakdowns": {
            "dois-by-issued-year": [
                [
                    2014,
                    3560
                ],
                [
                    2015,
                    3473
                ]
            ]
        },
        "publisher": "American Physical Society",
        "coverage": {
            "affiliations-current": 0.0,
            "similarity-checking-current": 0.0,
            "descriptions-current": 0.0,
            "ror-ids-current": 0.0,
            "funders-backfile": 0.138377884939771,
            "licenses-backfile": 0.9951380206782957,
            "funders-current": 0.0,
            "affiliations-backfile": 0.0,
            "resource-links-backfile": 0.07558889484233296,
            "orcids-backfile": 0.0,
            "update-policies-current": 0.0,
            "ror-ids-backfile": 0.0,
            "orcids-current": 0.0,
            "similarity-checking-backfile": 1.0,
            "references-backfile": 0.9809886686114584,
            "descriptions-backfile": 0.0,
            "award-numbers-backfile": 0.06945684745291818,
            "update-policies-backfile": 0.0,
            "licenses-current": 0.0,
            "award-numbers-current": 0.0,
            "abstracts-backfile": 0.0,
            "resource-links-current": 0.0,
            "abstracts-current": 0.0,
            "references-current": 0.0
        },
        "title": "Physical Review D",
        "subjects": [
            {
                "ASJC": 3106,
                "name": "Nuclear and High Energy Physics"
            }
        ],
        "coverage-type": {
            "all": {
                "last-status-check-time": 1665821306541,
                "affiliations": 0.0,
                "abstracts": 0.0,
                "orcids": 0.0,
                "licenses": 0.9951380206782957,
                "references": 0.9809886686114584,
                "funders": 0.138377884939771,
                "similarity-checking": 1.0,
                "award-numbers": 0.06945684745291818,
                "ror-ids": 0.0,
                "update-policies": 0.0,
                "resource-links": 0.07558889484233296,
                "descriptions": 0.0
            },
            "backfile": {
                "last-status-check-time": 1665821306541,
                "affiliations": 0.0,
                "abstracts": 0.0,
                "orcids": 0.0,
                "licenses": 0.9951380206782957,
                "references": 0.9809886686114584,
                "funders": 0.138377884939771,
                "similarity-checking": 1.0,
                "award-numbers": 0.06945684745291818,
                "ror-ids": 0.0,
                "update-policies": 0.0,
                "resource-links": 0.07558889484233296,
                "descriptions": 0.0
            },
            "current": {
                "last-status-check-time": 1665821306541,
                "affiliations": 0.0,
                "abstracts": 0.0,
                "orcids": 0.0,
                "licenses": 0.0,
                "references": 0.0,
                "funders": 0.0,
                "similarity-checking": 0.0,
                "award-numbers": 0.0,
                "ror-ids": 0.0,
                "update-policies": 0.0,
                "resource-links": 0.0,
                "descriptions": 0.0
            }
        },
        "flags": {
            "deposits-abstracts-current": false,
            "deposits-orcids-current": false,
            "deposits": true,
            "deposits-affiliations-backfile": false,
            "deposits-update-policies-backfile": false,
            "deposits-similarity-checking-backfile": true,
            "deposits-award-numbers-current": false,
            "deposits-resource-links-current": false,
            "deposits-ror-ids-current": false,
            "deposits-articles": true,
            "deposits-affiliations-current": false,
            "deposits-funders-current": false,
            "deposits-references-backfile": true,
            "deposits-ror-ids-backfile": false,
            "deposits-abstracts-backfile": false,
            "deposits-licenses-backfile": true,
            "deposits-award-numbers-backfile": true,
            "deposits-descriptions-current": false,
            "deposits-references-current": false,
            "deposits-resource-links-backfile": true,
            "deposits-descriptions-backfile": false,
            "deposits-orcids-backfile": false,
            "deposits-funders-backfile": true,
            "deposits-update-policies-current": false,
            "deposits-similarity-checking-current": false,
            "deposits-licenses-current": false
        },
        "ISSN": [
            "1550-7998",
            "1089-4918"
        ],
        "issn-type": [
            {
                "value": "1550-7998",
                "type": "print"
            },
            {
                "value": "1089-4918",
                "type": "electronic"
            }
        ]
    }
}
```

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