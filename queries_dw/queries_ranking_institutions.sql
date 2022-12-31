-- Ranking institutions with the most publications in a given year
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM 
	warehouse.publications pub
	JOIN warehouse.publication_institution pub_inst
	JOIN warehouse.institution inst
	JOIN warehouse.publication_time pub_time
	ON pub_inst.institution_id = inst.id
	AND pub_inst.publication_id = pub.id
	AND pub.pub_id = pub_time.publication_id
WHERE 
	DATE_PART('year', pub_time.date::date) = 'year_val'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most publications in scientific domain
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM 
	warehouse.publications pub
	JOIN warehouse.publication_institution pub_inst
	JOIN warehouse.institution inst
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	ON pub_inst.institution_id = inst.id
	AND pub_inst.publication_id = pub.id
	AND pub_domain.domain_id = scientific_domain.id
WHERE 
	scientific_domain.id = 'scientific_domain_id'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most publications in publication venue
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM 
	warehouse.publications pub
	JOIN warehouse.publication_institution pub_inst
	JOIN warehouse.institution inst
	JOIN warehouse.publication_venues pub_venues
	ON pub_inst.institution_id = inst.id
	AND pub_inst.publication_id = pub.id
	AND pub_venues.publication_id = pub.id
WHERE 
	pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking institutions with the most publications in a given year, scientific domain, and in publication venue
SELECT 
	inst.name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM 
	warehouse.publications pub
	JOIN warehouse.publication_institution pub_inst
	JOIN warehouse.institution inst
	JOIN warehouse.publication_time pub_time
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	JOIN warehouse.publication_venues pub_venues
	ON pub_inst.institution_id = inst.id
	AND pub_inst.publication_id = pub.id
	AND pub.pub_id = pub_time.publication_id
	AND pub_domain.domain_id = scientific_domain.id
	AND pub_venues.publication_id = pub.id
WHERE 
	DATE_PART('year', pub_time.date::date) = 'year_val'
	AND scientific_domain.id = 'scientific_domain_id'
	AND pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY num_of_publications DESC;


-----------------------------------------------------------------------------------------------------------


-- Ranking institutions with the most citations in a given year, scientific domain, and in publication venue
SELECT 
	inst.name, 
	SUM(pub.number_of_citations) AS number_of_citations
FROM 
	(
		-- Only look at the latest DOI (multiple rows can have same DOI). 
		-- Otherwise citations of same paper are counted multiple times.
		SELECT 
			DISTINCT ON (pu.doi)
			pu.id AS pub_id
		FROM warehouse.publications pu
		ORDER BY pu.snapshot_valid_to DESC
	) pub
	JOIN warehouse.publication_institution pub_inst
	JOIN warehouse.institution inst
	JOIN warehouse.publication_time pub_time
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	JOIN warehouse.publication_venues pub_venues
	ON pub_inst.institution_id = inst.id
	AND pub_inst.publication_id = pub.id
	AND pub.pub_id = pub_time.publication_id
	AND pub_domain.domain_id = scientific_domain.id
	AND pub_venues.publication_id = pub.id
WHERE 
	DATE_PART('year', pub_time.date::date) = 'year_val'
	AND scientific_domain.id = 'scientific_domain_id'
	AND pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY number_of_citations DESC;






























	
