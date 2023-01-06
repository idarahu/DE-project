CREATE TABLE IF NOT EXISTS author2affiliation (
author2affiliation_ID serial PRIMARY KEY,
author_ID INT NOT NULL,
affiliation_ID INT NOT NULL,
UNIQUE (author_ID, affiliation_ID));
