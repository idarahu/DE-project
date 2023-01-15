-- What are the year's hottest topics (categories of scientific disciplines)?
-- HOTTEST = most publications
-- by time, by venue, by institution
-- Exact category
SELECT
	scientific_domain.id,
	scientific_domain.major_field,
	scientific_domain.sub_category,
	scientific_domain.exact_category,
	scientific_domain.arxivx_category,
	COUNT(DISTINCT pub.pub_doi) AS num_of_publications
FROM
	-- use latest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
	AND inst.id = 1
	AND pub_venues.full_name = ''
GROUP BY scientific_domain.id
ORDER BY num_of_publications DESC;

-- What are the year's hottest topics (categories of scientific disciplines)?
-- HOTTEST = most publications
-- by time, by venue, by institution
-- Major field
SELECT
	scientific_domain.major_field,
	COUNT(DISTINCT pub.pub_doi) AS num_of_publications
FROM
	-- use latest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
	AND inst.id = 1
	AND pub_venues.full_name = ''
GROUP BY scientific_domain.major_field
ORDER BY num_of_publications DESC;

-- What are the year's hottest topics (categories of scientific disciplines)?
-- HOTTEST = most publications
-- by time
-- in major field
SELECT
	scientific_domain.major_field,
	COUNT(DISTINCT pub.pub_doi) AS num_of_publications
FROM
	-- use latest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
GROUP BY scientific_domain.major_field
ORDER BY num_of_publications DESC;

-- What are the year's hottest topics (categories of scientific disciplines)?
-- HOTTEST = most publications
-- by venue
-- in major field
SELECT
	scientific_domain.major_field,
	COUNT(DISTINCT pub.pub_doi) AS num_of_publications
FROM
	-- use latest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.full_name = ''
GROUP BY scientific_domain.major_field
ORDER BY num_of_publications DESC;

-- What are the year's hottest topics (categories of scientific disciplines)?
-- HOTTEST = most publications
-- by institution
-- in major field
SELECT
	scientific_domain.major_field,
	COUNT(DISTINCT pub.pub_doi) AS num_of_publications
FROM
	-- use latest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
WHERE
	inst.id = 1
GROUP BY scientific_domain.major_field
ORDER BY num_of_publications DESC;