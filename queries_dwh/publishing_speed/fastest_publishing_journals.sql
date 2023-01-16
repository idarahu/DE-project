-- Are there any journals where publishing takes much more time compared to others?
SELECT
	pub_latest.pub_venue_name,
	AVG(pub_latest.latest_release_date - pub_earliest.earliest_release_date) AS avg_difference
FROM
	-- get latest DOIs
	(
		SELECT
			pu.title AS pub_title,
			pu.doi AS pub_doi,
			pub_time.date AS latest_release_date,
			pub_venues.full_name AS pub_venue_name
		FROM warehouse.publications pu
		-- join time with publications
		JOIN warehouse.publication_time pub_time
			ON pu.time_id = pub_time.id
		-- join venues with publications
		JOIN warehouse.publication_venues pub_venues
			ON pub_venues.id = pu.venue_id
		WHERE pu.snapshot_valid_to is NULL
	) pub_latest
JOIN
	-- get earliest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.doi AS pub_doi,
			pub_time.date AS earliest_release_date
		FROM warehouse.publications pu
		-- join time with publications
		JOIN warehouse.publication_time pub_time
			ON pu.time_id = pub_time.id
		-- join venues with publications
		JOIN warehouse.publication_venues pub_venues
			ON pub_venues.id = pu.venue_id
		ORDER BY pu.doi, pub_time.date
	) pub_earliest
	ON pub_latest.pub_doi = pub_earliest.pub_doi
GROUP BY pub_latest.pub_venue_name
ORDER BY avg_difference DESC;