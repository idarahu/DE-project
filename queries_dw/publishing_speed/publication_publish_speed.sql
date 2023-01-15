-- Which papers have the most prolonged period between the first and last version?
SELECT
	pub_latest.pub_title,
	pub_latest.pub_doi,
	pub_latest.latest_release_date - pub_earliest.earliest_release_date AS difference
FROM
	-- get latest DOIs
	(
		SELECT
			pu.title AS pub_title,
			pu.doi AS pub_doi,
			pub_time.date AS latest_release_date
		FROM warehouse.publications pu
		JOIN warehouse.publication_time pub_time
			ON pu.time_id = pub_time.id
		WHERE pu.snapshot_valid_to is NULL
		LIMIT 1
	) pub_latest
JOIN
	-- get earliest DOIs
	(
		SELECT DISTINCT ON (pu.doi)
			pu.doi AS pub_doi,
			pub_time.date AS earliest_release_date
		FROM warehouse.publications pu
		JOIN warehouse.publication_time pub_time
			ON pu.time_id = pub_time.id
		ORDER BY pu.doi, pu.snapshot_valid_to
	) pub_earliest
	ON pub_latest.pub_doi = pub_earliest.pub_doi
ORDER BY difference DESC;