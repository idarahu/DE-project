-- Update venue (if fact table had dim foreign keys)
BEGIN;
UPDATE warehouse.publication_venues venue
SET
	venue.valid_to = now()::date;
WHERE venue.full_name = 'full_name' AND venue.valid_to IS NULL;


INSERT INTO warehouse.publication_venues(
    full_name,
    abbreviation,
    type,
    h_index_calculated,
    valid_from
)
VALUES ('full_name', 'abbreviation', 'type',
		'h_index_calculated', now()::date);

UPDATE warehouse.publications pub
SET
	pub.venue_id = (
		SELECT venue.id FROM warehouse.publication_venues venue
		WHERE venue.full_name = 'full_name' AND venue.valid_to IS NULL
		LIMIT 1
	)
WHERE
	pub.venue_id IN (
		SELECT venue.id FROM warehouse.publication_venues
		WHERE venue.full_name = 'full_name'
	)
COMMIT;