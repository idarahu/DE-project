INSERT INTO affiliations (institution_name, institution_place)
SELECT DISTINCT institution_name, institution_place
FROM affiliations_temp
WHERE NOT EXISTS (
SELECT * FROM affiliations
WHERE
affiliations.institution_name = affiliations_temp.institution_name
AND affiliations.institution_place = affiliations_temp.institution_place);
