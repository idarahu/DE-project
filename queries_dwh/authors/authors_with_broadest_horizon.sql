-- Ranking authors with the broadest horizon
-- (authors who have written papers in the largest
-- amount of different scientific domains)
SELECT
	aut.full_name,
	aut.first_name,
	COUNT(DISTINCT pub_domain.domain_id) AS num_of_domains
FROM warehouse.publications pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.id
GROUP BY aut.id
ORDER BY num_of_domains DESC;