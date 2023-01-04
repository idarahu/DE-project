CREATE TABLE IF NOT EXISTS venues (
venue_ID INT PRIMARY KEY,
full_name VARCHAR(255),
abbreviation VARCHAR(50) UNIQUE,
print_issn VARCHAR(50),
electronic_issn VARCHAR(50));
