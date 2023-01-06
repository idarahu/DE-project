INSERT INTO author2affiliation (author_id, affiliation_id)
SELECT DISTINCT t2.author_id, t3.affiliation_id
FROM affiliations_temp t1
JOIN authors t2 ON t1.author_last_name = t2.last_name AND t1.author_first_name_abbr = t2.first_name_abbr
JOIN affiliations t3 ON t1.institution_name = t3.institution_name AND t1.institution_place = t3.institution_place;
