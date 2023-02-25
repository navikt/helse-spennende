package no.nav.helse.spennende

import kotliquery.Session
import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.LocalDateTime.now
import javax.sql.DataSource

internal class PostgresRepository(dataSourceGetter: () -> DataSource) {
    private companion object {
        private val publiclog = LoggerFactory.getLogger(PostgresRepository::class.java)
        private val logger = LoggerFactory.getLogger("tjenestekall")

        private val nesteForfall = Duration.ofMinutes(5)

        @Language("PostgreSQL")
        private const val INSERT_PERSON = """INSERT INTO person (fnr) VALUES (:fnr) ON CONFLICT DO NOTHING"""
        @Language("PostgreSQL")
        private const val INSERT_ENDRINGSMELDING = """INSERT INTO endringsmelding (person_id, hendelse_id, innkommende_melding, neste_forfallstidspunkt) VALUES ((SELECT id FROM person WHERE fnr = :fnr), :hendelseId, :melding, :neste_forfallstidspunkt)"""
        @Language("PostgreSQL")
        private const val UPDATE_UTGÅENDE = """UPDATE endringsmelding SET utgående_melding=:melding WHERE id=:endringsmeldingId"""
        @Language("PostgreSQL")
        // ... 'and sendt is null' er for å hjelpe query planneren til å innse at den kan bruke indeksen opprettet i V0_6 mye, mye mer effektivt
        private const val SET_NESTE_FORFALLSDATO_FOR_PERSON = """UPDATE endringsmelding SET neste_forfallstidspunkt=:neste_forfallstidspunkt WHERE person_id=:person_id and sendt is null;"""
        @Language("PostgreSQL")
        private const val FINN_SENDEKLARE_ENDRINGSMELDINGER = """
            WITH alleIkkeSendteEndringsmeldinger AS (
                SELECT * FROM endringsmelding
                WHERE sendt IS NULL AND utgående_melding IS NULL
                FOR UPDATE
                SKIP LOCKED
            )
            SELECT p.id as person_id, p.fnr, MAX(e.id) as siste_endringsmelding_id
            FROM alleIkkeSendteEndringsmeldinger e
            INNER JOIN person p ON e.person_id = p.id
            GROUP BY p.id
            HAVING MAX(neste_forfallstidspunkt) <= now();
            """

        @Language("PostgreSQL")
        private const val MARKER_ENDRINGSMELDINGER_SOM_SENDT = """
            UPDATE endringsmelding SET sendt = now() 
            WHERE person_id = (SELECT person_id from endringsmelding WHERE id = :endringsmeldingId)  
            AND sendt is NULL AND id <= :endringsmeldingId
        """
    }

    private val dataSource by lazy(dataSourceGetter)

    private fun transactionally(f: TransactionalSession.() -> Unit) {
        sessionOf(dataSource).use { session ->
            session.transaction { f(it) }
        }
    }

    internal fun hentSendeklareEndringsmeldinger(block: (SendeklarEndringsmelding) -> Unit) {
        transactionally {
            run(queryOf(FINN_SENDEKLARE_ENDRINGSMELDINGER).map { row ->
                SendeklarEndringsmelding(
                    row.long("person_id"),
                    row.string("fnr"),
                    row.long("siste_endringsmelding_id")
                )
            }.asList)
                .onEach { melding -> melding.oppdaterForfallstidspunkt(this) }
                .onEach(block)
        }
    }

    internal fun markerEndringsmeldingerSomSendt(endringsmeldingId: Long): Boolean {
        return sessionOf(dataSource).use {
            it.run(
                queryOf(
                    MARKER_ENDRINGSMELDINGER_SOM_SENDT, mapOf(
                        "endringsmeldingId" to endringsmeldingId
                    )
                ).asUpdate
            ) > 0
        }
    }

    internal class SendeklarEndringsmelding(
        private val personId: Long,
        val fnr: String,
        val endringsmeldingId: Long
    ) {
        internal fun oppdaterForfallstidspunkt(session: TransactionalSession) {
            setNesteForfallstidspunkt(session, personId)
            publiclog.info("Setter neste forfallstidspunkt for endringsmeldingId $endringsmeldingId")
            logger.info("Setter neste forfallstidspunkt for fnr $fnr (endringsmeldingId $endringsmeldingId)")
        }

        private fun setNesteForfallstidspunkt(session: TransactionalSession, personId: Long) {
            session.run(queryOf(SET_NESTE_FORFALLSDATO_FOR_PERSON, mapOf(
                "person_id" to personId,
                "neste_forfallstidspunkt" to now() + nesteForfall
            )).asUpdate)
        }
    }

    internal fun lagreEndringsmelding(fnr: String, hendelseId: Long, json: String): Long =
        requireNotNull(lagreEndringsmeldingOgReturnerId(fnr, hendelseId, json)) { "kunne ikke inserte endringsmelding eller person" }

    internal fun lagreUtgåendeMelding(endringsmeldingId: Long, json: String): Boolean {
        return 1 == sessionOf(dataSource).use { session ->
            session.run(queryOf(UPDATE_UTGÅENDE, mapOf(
                "endringsmeldingId" to endringsmeldingId,
                "melding" to json
            )).asUpdate)
        }
    }

    private fun lagreEndringsmeldingOgReturnerId(fnr: String, hendelseId: Long, json: String) =
        sessionOf(dataSource, returnGeneratedKey = true).use { session ->
            sikrePersonFinnes(session, fnr)
            session.run(queryOf(INSERT_ENDRINGSMELDING, mapOf(
                "fnr" to fnr,
                "hendelseId" to hendelseId,
                "melding" to json,
                "neste_forfallstidspunkt" to now() + nesteForfall
            )).asUpdateAndReturnGeneratedKey)
        }

    private fun sikrePersonFinnes(session: Session, fnr: String) {
        session.run(queryOf(INSERT_PERSON, mapOf("fnr" to fnr)).asExecute)
    }
}