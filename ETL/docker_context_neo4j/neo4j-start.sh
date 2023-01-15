#!/usr/bin/env bash

cd /import

neo4j-admin database import full \
  --nodes venues_header.csv,venues.csv \
  --nodes authors_header.csv,authors.csv \
  --nodes affiliations_header.csv,affiliations.csv \
  --nodes publications_header.csv,publications.csv \
  --nodes domains_header.csv,domains.csv \
  --relationships author_of_header.csv,author_of.csv \
  --relationships author_collaborates_with_header.csv,author_collaborates_with.csv \
  --relationships works_at_header.csv,works_at.csv \
  --relationships published_in_header.csv,published_in.csv \
  --relationships belongs_to_header.csv,belongs_to.csv \
  --relationships cited_by_header.csv,cited_by.csv \
  --relationships covers_header.csv,covers.csv \
  --relationships affiliation_collaborates_with_header.csv,affiliation_collaborates_with.csv \
  --relationships affiliation_publishes_in_header.csv,affiliation_publishes_in.csv \
  --overwrite-destination

neo4j console