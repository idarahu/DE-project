-- Ranking institutions with the most citations in a given scientific domain
SELECT
	inst.name,
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id
		FROM warehouse.publications pu
		-- join time with publications
        JOIN warehouse.publication_time pub_time
            ON pu.time_id = pub_time.id
		WHERE pu.snapshot_valid_to is NULL
	) pub
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
WHERE
	scientific_domain.id = 1
GROUP BY inst.id
ORDER BY number_of_citations DESC;