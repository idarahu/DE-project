CREATE TABLE IF NOT EXISTS affiliation2publication (
affiliation2pub_ID serial PRIMARY KEY,
affiliation_ID INT NOT NULL,
publication_ID INT NOT NULL,
UNIQUE (affiliation_ID, publication_ID));
