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
        private val logger = LoggerFactory.getLogger("tjenestekall")

        private val nesteForfall = Duration.ofMinutes(5)

        @Language("PostgreSQL")
        private const val INSERT_PERSON = """INSERT INTO person (fnr) VALUES (:fnr) ON CONFLICT DO NOTHING"""
        @Language("PostgreSQL")
        private const val INSERT_ENDRINGSMELDING = """INSERT INTO endringsmelding (person_id, hendelse_id, innkommende_melding, neste_forfallstidspunkt) VALUES ((SELECT id FROM person WHERE fnr = :fnr), :hendelseId, :melding, :neste_forfallstidspunkt)"""
        @Language("PostgreSQL")
        private const val SELECT_ACTIVITY = """SELECT sendt FROM endringsmelding WHERE sendt IS NOT NULL AND person_id=(SELECT person_id FROM endringsmelding WHERE id=:endringsmeldingId) ORDER BY sendt DESC LIMIT 1"""
        @Language("PostgreSQL")
        private const val UPDATE_UTGÅENDE = """UPDATE endringsmelding SET utgående_melding=:melding WHERE id=:endringsmeldingId"""
        @Language("PostgreSQL")
        private const val UPDATE_ACTIVITY = """UPDATE endringsmelding SET sendt=now() WHERE id=:endringsmeldingId"""
        @Language("PostgreSQL")
        private const val SET_NESTE_FORFALLSDATO_FOR_PERSON = """UPDATE ENDRINGSMELDING SET neste_forfallstidspunkt=:neste_forfallstidspunkt WHERE person_id=(SELECT id FROM person WHERE fnr=:fnr)"""
        @Language("PostgreSQL")
        private const val FINN_SENDEKLARE_ENDRINGSMELDINGER = """
            WITH alleIkkeSendteEndringsmeldinger AS (
                SELECT * FROM endringsmelding
                WHERE sendt IS NULL AND utgående_melding IS NULL
                FOR UPDATE
                SKIP LOCKED
            )
            SELECT p.fnr, MAX(e.id) as siste_endringsmelding_id
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

    fun transactionally(f: TransactionalSession.() -> Unit) {
        sessionOf(dataSource).use { session ->
            session.transaction { f(it) }
        }
    }

    internal fun hentSendeklareEndringsmeldinger(session: TransactionalSession): List<SendeklarEndringsmelding> {
        return session.run(queryOf(FINN_SENDEKLARE_ENDRINGSMELDINGER).map { row ->
            SendeklarEndringsmelding(
                row.string("fnr"),
                row.long("siste_endringsmelding_id")
            )
        }.asList)
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

    internal fun setNesteForfallstidspunkt(session: TransactionalSession, fnr: String): Boolean {
        return session.run(
            queryOf(
                SET_NESTE_FORFALLSDATO_FOR_PERSON, mapOf(
                    "fnr" to fnr,
                    "neste_forfallstidspunkt" to now() + nesteForfall
                )
            ).asUpdate
        ) > 0
    }

    internal class SendeklarEndringsmelding(
        val fnr: String,
        val endringsmeldingId: Long
    )

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

    internal fun skalRepublisere(endringsmeldingId: Long, ratebegrensning: Duration): Boolean {
        val sisteAktivitet = hentSisteAktivitet(endringsmeldingId)
        val nå = LocalDateTime.now()
        val skalRepublisere = (sisteAktivitet == null || Duration.between(sisteAktivitet, nå) >= ratebegrensning)
        if (skalRepublisere) lagreSisteAktivitet(endringsmeldingId)
        return skalRepublisere
    }

    private fun hentSisteAktivitet(endringsmeldingId: Long) =
        sessionOf(dataSource).use { session ->
            session.run(queryOf(SELECT_ACTIVITY, mapOf("endringsmeldingId" to endringsmeldingId))
                .map { row -> row.localDateTimeOrNull("sendt") }.asSingle
            )
        }

    internal fun lagreSisteAktivitet(endringsmeldingId: Long) {
        sessionOf(dataSource).use { session ->
            session.run(queryOf(UPDATE_ACTIVITY, mapOf("endringsmeldingId" to endringsmeldingId)).asUpdate)
        }
    }
}