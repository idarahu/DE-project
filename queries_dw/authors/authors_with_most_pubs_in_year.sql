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