CREATE TABLE IF NOT EXISTS authors_temp (
publication_ID INT,
last_name VARCHAR(100) NOT NULL,
first_name VARCHAR(100),
first_name_abbr VARCHAR(10) NOT NULL,
extra VARCHAR(100),
position TEXT,
h_index_real INT,
updated_at TIMESTAMP WITH TIME ZONE);