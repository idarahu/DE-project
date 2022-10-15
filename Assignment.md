# Data pipeline to analyze scientific publications data

## Description

**Goal**: Create a data pipeline to analyze scientific publications data.

### RDBMS

**Schema**:

- Data about scientific publications.
- Example dimensions are authors, publication venues, scientific domain, year of publication, and authors’ affiliation.
- Handle changes in values.

Example **queries for RDBMS**:

- ranking authors in a scientific discipline
- computing h-index for 
  - authors 
  - journals
  - publishers
- computing the number of publications in a given topic per time period
- (and more: citations per publisher, per journal, etc.)

### Graph

Propose a graph **schema**:

> For example, authors, papers, and journals, or other publication venues are good candidates to act as **nodes** in the graph. **Edges** can represent authorship between an author and a paper, co-authorship among authors, works-for between author and affiliation, cites relationship among papers, etc.

Example **queries for a graph database**:

- co-authorship prediction
- community analysis (based on co-authorship on being in the same scientific domain)
- find influential papers, authors, journals, publishers

## Dataset

### Publications data

[arXiv Dataset](https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download)

> a repository of 1.7 million articles, with relevant features such as article titles, authors, categories, abstracts, full text PDFs, and more.

### References data

- [scholar.py](http://www.icir.org/christian/scholar.html) — A Parser for Google Scholar (icir.org)
- [Crossref](https://www.crossref.org/documentation/retrieve-metadata/rest-api/) REST API 
- [DBLP](https://dblp.org/faq/13501473.html) API for computer science publications
- [Publish or Perish](https://harzing.com/resources/publish-or-perish) (harzing.com)
- citations - Retrieving the references in a publication automatically - [Academia Stack Exchange](https://academia.stackexchange.com/questions/15596/retrieving-the-references-in-a-publication-automatically)
