-- Ranking publications based on nr of citations in a publication venue
SELECT
	pub.pub_title,
	pub.pub_doi,
	SUM(pub.pub_num_of_citations) AS number_of_citations
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
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.full_name = ''
GROUP BY pub.pub_doi, pub.pub_title
ORDER BY number_of_citations DESC;