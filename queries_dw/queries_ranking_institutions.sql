-- Ranking institutions with the most publications in a given year
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.time_id = pub_time.id
WHERE
	pub_time.year = '2023'
GROUP BY inst.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most publications in scientific domain
SELECT
	inst.name,
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
WHERE
	scientific_domain.id = 1
GROUP BY inst.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most publications in publication venue
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.venue_id
WHERE
	pub_venues.id = 1
GROUP BY inst.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most publications in a given year, scientific domain, and in publication venue
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
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
GROUP BY inst.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most citations in a given year
SELECT
	inst.name,
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
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
GROUP BY inst.id
ORDER BY number_of_citations DESC;

-- Ranking institutions with the most citations in a given scientific domain
SELECT
	inst.name,
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
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
WHERE
	scientific_domain.id = 1
GROUP BY inst.id
ORDER BY number_of_citations DESC;

-- Ranking institutions with the most citations in a given publication venue
SELECT
	inst.name,
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
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.id = 1
GROUP BY inst.id
ORDER BY number_of_citations DESC;

-- Ranking institutions with the most citations in a given year, scientific domain, and in publication venue
SELECT
	inst.name, 
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
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
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
GROUP BY inst.id
ORDER BY number_of_citations DESC;






























	
