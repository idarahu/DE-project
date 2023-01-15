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