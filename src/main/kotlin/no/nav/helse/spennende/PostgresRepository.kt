package no.nav.helse.spennende

import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.time.Duration
import java.time.LocalDateTime
import javax.sql.DataSource

internal class PostgresRepository(dataSourceGetter: () -> DataSource) {
    private companion object {
        @Language("PostgreSQL")
        private const val INSERT_PERSON = """INSERT INTO person (fnr) VALUES (:fnr) ON CONFLICT DO NOTHING"""
        @Language("PostgreSQL")
        private const val INSERT_ENDRINGSMELDING = """INSERT INTO endringsmelding (person_id, hendelse_id, innkommende_melding) VALUES ((SELECT id FROM person WHERE fnr = :fnr), :hendelseId, :melding)"""
        @Language("PostgreSQL")
        private const val SELECT_ACTIVITY = """SELECT sendt FROM endringsmelding WHERE sendt IS NOT NULL AND person_id=(SELECT person_id FROM endringsmelding WHERE id=:endringsmeldingId) ORDER BY sendt DESC LIMIT 1"""
        @Language("PostgreSQL")
        private const val UPDATE_UTGÅENDE = """UPDATE endringsmelding SET utgående_melding=:melding WHERE id=:endringsmeldingId"""
        @Language("PostgreSQL")
        private const val UPDATE_ACTIVITY = """UPDATE endringsmelding SET sendt=now() WHERE id=:endringsmeldingId"""
    }
    private val dataSource by lazy(dataSourceGetter)

    internal fun lagreEndringsmelding(fnr: String, hendelseId: Long, json: String): Long =
        requireNotNull(lagreEndringsmeldingOgReturnerId(fnr, hendelseId, json)) { "kunne ikke inserte endringsmelding eller person" }

    internal fun lagreUtgåendeMelding(endringsmeldingId: Long, json: String): Boolean {
        return 1 == sessionOf(dataSource, returnGeneratedKey = true).use { session ->
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
                "melding" to json
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

    private fun lagreSisteAktivitet(endringsmeldingId: Long) {
        sessionOf(dataSource).use { session ->
            session.run(queryOf(UPDATE_ACTIVITY, mapOf("endringsmeldingId" to endringsmeldingId)).asUpdate)
        }
    }
}