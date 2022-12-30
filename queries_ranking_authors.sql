-- Ranking authors
-- 1 with the most publications in a given year, scientific domain and/or publication venue
-- 2 with the most citations in a given year, scientific domain and/or publication venue
-- 3 with the highest h-index in a given time period
-- 4 with the broadest horizon (authors who have written papers in the largest amount of different scientific domains) 




-- 1
-- Ranking authors with the most publications in a given year
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
	JOIN warehouse.publication_time pub_time
	JOIN warehouse.authors aut
	JOIN warehouse.publication_author pub_auth
ON pub.id = pub_time.publication_id
	AND pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
WHERE 
	DATE_PART('year', pub_time.date::date) = 'year_val'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most publications in scientific domain
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.authors aut
	JOIN warehouse.publications pub
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub_domain.domain_id = scientific_domain.id
WHERE scientific_domain.id = 'scientific_domain_id'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most publications in publication venue
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.authors aut
	JOIN warehouse.publications pub
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_venues pub_venues
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub_venues.publication_id = pub.id
WHERE pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY num_of_publications DESC;

-- Ranking authors with the most publications in a given year, scientific domain and/or publication venue
SELECT 
	aut.full_name, 
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.authors aut
	JOIN warehouse.publications pub
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_time pub_time
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	JOIN warehouse.publication_venues pub_venues
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub.id = pub_time.publication_id
	AND pub_domain.publication_id = pub.id
	AND pub_domain.domain_id = scientific_domain.id
	AND pub_venues.publication_id = pub.id
WHERE
	DATE_PART('year', pub_time.date::date) = 'year_val'
	AND scientific_domain.id = 'scientific_domain_id'
	AND pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY num_of_publications DESC;





-- 2
-- Ranking authors with the most citations in a given year
SELECT 
	aut.full_name, 
	SUM(pub.number_of_citations) AS number_of_citations
FROM 
	(
		SELECT -- Only look at the latest DOI (multiple rows can have same DOI). Otherwise citations of same paper are countet multiple times.
			DISTINCT ON (pu.doi)
			pu.id AS pub_id
		FROM warehouse.publications pu
		ORDER BY pu.snapshot_valid_to DESC
	) pub
	JOIN warehouse.authors aut
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_time pub_time
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub.id = pub_time.publication_id
WHERE 
	DATE_PART('year', pub_time.date::date) = 'year_val'
GROUP BY aut.id
ORDER BY number_of_citations DESC;


-- Ranking authors with the most citations in scientific domain
SELECT 
	aut.full_name, 
	SUM(pub.number_of_citations) AS number_of_citations
FROM 
	(
		SELECT -- Only look at the latest DOI (multiple rows can have same DOI). Otherwise citations of same paper are countet multiple times.
			DISTINCT ON (pu.doi)
			pu.id AS pub_id
		FROM warehouse.publications pu
		ORDER BY pu.snapshot_valid_to DESC
	) pub
	JOIN warehouse.authors aut
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub_domain.domain_id = scientific_domain.id
WHERE scientific_domain.id = 'scientific_domain_id'
GROUP BY aut.id
ORDER BY number_of_citations DESC;

-- Ranking authors with the most citations in publication venue
SELECT 
	aut.full_name, 
	SUM(pub.number_of_citations) AS number_of_citations
FROM 
	(
		SELECT -- Only look at the latest DOI (multiple rows can have same DOI). Otherwise citations of same paper are countet multiple times.
			DISTINCT ON (pu.doi)
			pu.id AS pub_id
		FROM warehouse.publications pu
		ORDER BY pu.snapshot_valid_to DESC
	) pub
	JOIN warehouse.authors aut
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_venues pub_venues
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub_venues.publication_id = pub.id
WHERE pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY number_of_citations DESC;

-- Ranking authors with the most citations in a given year, scientific domain and/or publication venue
SELECT 
	aut.full_name, 
	SUM(pub.number_of_citations) AS number_of_citations
FROM 
	(
		SELECT -- Only look at the latest DOI (multiple rows can have same DOI). Otherwise citations of same paper are countet multiple times.
			DISTINCT ON (pu.doi)
			pu.id AS pub_id
		FROM warehouse.publications pu
		ORDER BY pu.snapshot_valid_to DESC
	) pub
	JOIN warehouse.authors aut
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_time pub_time
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	JOIN warehouse.publication_venues pub_venues
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub.id = pub_time.publication_id
	AND pub_domain.publication_id = pub.id
	AND pub_domain.domain_id = scientific_domain.id
	AND pub_venues.publication_id = pub.id
WHERE
	DATE_PART('year', pub_time.date::date) = 'year_val'
	AND scientific_domain.id = 'scientific_domain_id'
	AND pub_venues.id = 'pub_venues_id'
GROUP BY aut.id
ORDER BY number_of_citations DESC;




-- 3
-- Ranking authors with the highest h-index in a given time period
SELECT au.full_name, au.h_index FROM (
	SELECT 
		DISTINCT ON (aut.id)
		aut.full_name AS full_name,
		aut.h_index_real AS h_index
	FROM warehouse.authors aut
	WHERE 
		('date2' BETWEEN aut.valid_from AND aut.valid_to)
		OR
		('date1' BETWEEN aut.valid_from AND aut.valid_to)
		OR
		('date1' <= aut.valid_from AND 'date2' >= aut.valid_to)
	ORDER BY aut.valid_to DESC
) au
ORDER BY au.h_index_real DESC;



-- 4
-- Ranking authors with the broadest horizon 
-- (authors who have written papers in the largest 
-- amount of different scientific domains)
SELECT 
	COUNT(DISTINCT scientific_domain.id) AS num_of_domains
	aut.full_name
FROM warehouse.authors aut
	JOIN warehouse.publications pub
	JOIN warehouse.publication_author pub_auth
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	ON pub_auth.publication_id = pub.id
	AND pub_auth.author_id = aut.id
	AND pub_domain.domain_id = scientific_domain.id
GROUP BY aut.id
ORDER BY num_of_domains DESC;
















-- function to filter out publications by date (probably not needed)
create or replace function get_publications_between_dates(date1 date, date2 date)
	returns table (
		pub_id bigint,
		doi text,
		title text,
		number_of_authors int,
		submitter text,
		lang text,
		volume int,
		issue int,
		page_numbers text,
		number_of_pages int,
		number_of_references int,
		no_ver_arxiv int,
		date_of_first_version date,
		number_of_citations int
	)
as $$
begin
    return query 
	SELECT pub.id
	FROM warehouse.publications pub
		JOIN warehouse.publication_time pub_time
	ON pub.id = pub_time.publication_id
	WHERE 
		pub_time.date BETWEEN 'date1' AND 'date2'
		AND
		('date2' BETWEEN pub.snapshot_valid_from AND pub.snapshot_valid_to)
		OR
		('date1' BETWEEN pub.snapshot_valid_from AND pub.snapshot_valid_to)
		OR
		('date1' <= pub.snapshot_valid_from AND 'date2' >= pub.snapshot_valid_to)
end;$$
language plpgsql;










	