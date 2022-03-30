create table person
(
    id  BIGSERIAL PRIMARY KEY,
    fnr varchar unique not null
);

create index "idx_person_fnr" on person (fnr);

create table endringsmelding
(
    id                  BIGSERIAL PRIMARY KEY,
    person_id           BIGINT references person (id) not null,
    hendelse_id         BIGINT                        not null, -- usikker på hvorvidt denne er unik eller ikke; spesielt dersom Infotrygd gjør ny datalast/baseline av replikabasene
    innkommende_melding TEXT                          NOT NULL,
    utgående_melding    TEXT,
    lest                timestamp default now()       not null,
    sendt               timestamp
);
