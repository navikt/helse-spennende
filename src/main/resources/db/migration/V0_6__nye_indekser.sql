drop index if exists endringsmelding_person_id_sendt_idx;
-- reduserer indeksen ved å fokusere kun på rader hvor sendt er null, siden
-- det er disse radene vi trenger å hente ut for å potensielt sende (og oppdatere etterpå)
create index endringsmelding_til_forfall_idx on endringsmelding(person_id, sendt) where sendt is null;