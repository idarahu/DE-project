CREATE TABLE IF NOT EXISTS affiliations (
affiliation_ID serial PRIMARY KEY,
institution_name VARCHAR(255) UNIQUE,
institution_place VARCHAR(255));
