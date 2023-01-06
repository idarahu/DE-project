INSERT INTO affiliation2publication (affiliation_id, publication_id)
SELECT DISTINCT t2.affiliation_id, t1.publication_id
FROM affiliations_temp t1
JOIN affiliations t2 ON t1.institution_name = t2.institution_name AND t1.institution_place = t2.institution_place;
