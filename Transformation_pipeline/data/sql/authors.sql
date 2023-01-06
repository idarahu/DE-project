CREATE TABLE IF NOT EXISTS authors (
author_ID serial PRIMARY KEY,
last_name VARCHAR(255) NOT NULL,
first_name VARCHAR(255),
first_name_abbr VARCHAR(25) NOT NULL,
extra VARCHAR(100),
position TEXT,
h_index_real INT,
updated_at TIMESTAMP WITH TIME ZONE,
UNIQUE (last_name, first_name_abbr));
