-- Ranking authors with the most publications in a given year
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
JOIN warehouse.publication_time pub_time
	ON pub.time_id = pub_time.id
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
WHERE pub_time.year = '2023'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most publications in scientific domain
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
WHERE scientific_domain.id = 1
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most publications in publication venue
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.venue_id
WHERE pub_venues.id = 1
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most publications in a given year, scientific domain and/or publication venue
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.venue_id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.time_id = pub_time.id
WHERE
	pub_time.year = '2023'
	AND scientific_domain.id = 1
	AND pub_venues.id = 1
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most citations in a given year
SELECT
	aut.full_name,
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.pub_id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
GROUP BY aut.id
ORDER BY number_of_citations DESC;

-- Ranking authors with the most citations in scientific domain
SELECT 
	aut.full_name, 
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.pub_id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
WHERE
	scientific_domain.id = 1
GROUP BY aut.id
ORDER BY number_of_citations DESC;

-- Ranking authors with the most citations in publication venue
SELECT 
	aut.full_name, 
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.pub_id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.id = 1
GROUP BY aut.id
ORDER BY number_of_citations DESC;

-- Ranking authors with the most citations in a given year, scientific domain and/or publication venue
SELECT 
	aut.full_name, 
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.pub_id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
	AND scientific_domain.id = 1
	AND pub_venues.id = 1
GROUP BY aut.id
ORDER BY number_of_citations DESC;

-- Ranking authors with the highest h-index (real) in a given time period
SELECT au.full_name, au.h_index_real FROM (
	SELECT
		DISTINCT ON (aut.full_name)
		aut.full_name AS full_name,
		aut.h_index_real AS h_index_real
	FROM warehouse.authors aut
	WHERE
	    (aut.valid_from, aut.valid_to) OVERLAPS ('2012-01-01'::DATE, '2012-04-12'::DATE)
	ORDER BY aut.full_name, aut.valid_to DESC
) au
ORDER BY au.h_index_real DESC;

-- Ranking authors with the highest h-index (calculated) in a given time period
SELECT au.full_name, au.h_index_calculated FROM (
	SELECT
		DISTINCT ON (aut.full_name)
		aut.full_name AS full_name,
		aut.h_index_calculated AS h_index_calculated
	FROM warehouse.authors aut
	WHERE
	    (aut.valid_from, aut.valid_to) OVERLAPS ('2012-01-01'::DATE, '2012-04-12'::DATE)
	ORDER BY aut.full_name, aut.valid_to DESC
) au
ORDER BY au.h_index_calculated DESC;

-- Ranking authors with the broadest horizon
-- (authors who have written papers in the largest 
-- amount of different scientific domains)
SELECT 
	COUNT(DISTINCT scientific_domain.id) AS num_of_domains,
	aut.full_name
FROM warehouse.publications pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
GROUP BY aut.id
ORDER BY num_of_domains DESC;
