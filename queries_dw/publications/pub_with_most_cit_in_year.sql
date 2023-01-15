-- Ranking publications based on nr of citations in a given year
SELECT
	pub.pub_title,
	pub.pub_doi,
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		WHERE pu.snapshot_valid_to is NULL
		LIMIT 1
	) pub
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
GROUP BY pub.pub_doi, pub.pub_title
ORDER BY number_of_citations DESC;