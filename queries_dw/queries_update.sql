-- Update venue
BEGIN;
UPDATE warehouse.publication_venues venue
SET
	valid_to = now()::date;
WHERE full_name = 'full_name';


INSERT INTO warehouse.publication_venues(
    publication_id,
    full_name,
    abbreviation,
    type,
    h_index_calculated,
    valid_from
)
VALUES ('publication_id', 'full_name', 'abbreviation', 'type',
		'h_index_calculated', now()::date);
COMMIT;