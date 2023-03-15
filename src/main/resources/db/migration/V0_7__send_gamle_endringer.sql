-- sender ut endringsmeldinger som skjedde før spleis hadde kode for å ta dem inn

insert into endringsmelding(person_id, hendelse_id, innkommende_melding, neste_forfallstidspunkt)
select person_id, 1::bigint, '', '2023-03-15 16:00:00+00'
from endringsmelding
group by person_id
having MAX(sendt) < '2023-02-23 23:59:59';