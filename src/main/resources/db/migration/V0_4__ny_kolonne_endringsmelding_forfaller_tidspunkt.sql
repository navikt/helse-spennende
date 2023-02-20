ALTER TABLE endringsmelding ADD COLUMN neste_forfallstidspunkt TIMESTAMP DEFAULT NULL;

UPDATE endringsmelding SET neste_forfallstidspunkt = lest + INTERVAL '5 minutes'
WHERE neste_forfallstidspunkt is NULL;

ALTER TABLE endringsmelding ALTER COLUMN neste_forfallstidspunkt SET NOT NULL;
