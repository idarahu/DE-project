CREATE TABLE IF NOT EXISTS affiliations_temp (
publication_ID INT,
institution_name VARCHAR(255),
institution_place VARCHAR(255),
author_last_name VARCHAR(100) NOT NULL,
author_first_name_abbr VARCHAR(10) NOT NULL);
