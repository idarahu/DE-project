CREATE TABLE IF NOT EXISTS author2publication (
author2pub_ID serial PRIMARY KEY,
author_ID INT NOT NULL,
publication_ID INT NOT NULL,
UNIQUE (author_ID, publication_ID));
