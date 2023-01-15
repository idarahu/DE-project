-- Ranking authors with the broadest horizon
-- (authors who have written papers in the largest
-- amount of different scientific domains)
SELECT
	COUNT(DISTINCT scientific_domain.id) AS num_of_domains,
	aut.full_name,
	aut.first_name
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