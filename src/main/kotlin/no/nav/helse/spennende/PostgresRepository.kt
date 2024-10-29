package no.nav.helse.spennende

import com.github.navikt.tbd_libs.speed.IdentResponse
import kotliquery.Session
import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime.now
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

internal class PostgresRepository(dataSourceGetter: () -> DataSource) {
    private companion object {
        private val publiclog = LoggerFactory.getLogger(PostgresRepository::class.java)
        private val logger = LoggerFactory.getLogger("tjenestekall")

        private val nesteForfallUtsettelse = Duration.ofMinutes(3)
        private val nesteForfallstidspunkt get() = now().truncatedTo(ChronoUnit.MINUTES) + nesteForfallUtsettelse

        @Language("PostgreSQL")
        private const val INSERT_PERSON = """INSERT INTO person (fnr, aktor_id) VALUES (:fnr, :aktor_id) ON CONFLICT(fnr) DO UPDATE SET aktor_id = EXCLUDED.aktor_id"""
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
                SELECT MAX(e.id) as siste_endringsmelding_id, e.person_id
                FROM endringsmelding e
                WHERE sendt IS NULL AND utgående_melding IS NULL
                GROUP BY person_id
                HAVING MAX(neste_forfallstidspunkt) <= :naavaerendeTidspunkt
                LIMIT 30000
            )
            SELECT p.id as person_id, p.fnr, p.aktor_id, e.siste_endringsmelding_id
            FROM person p
            INNER JOIN alleIkkeSendteEndringsmeldinger e ON e.person_id=p.id
            FOR UPDATE SKIP LOCKED
            """

        @Language("PostgreSQL")
        private const val MARKER_ENDRINGSMELDINGER_SOM_SENDT = """
            UPDATE endringsmelding SET sendt = :naavaerendeTidspunkt 
            WHERE person_id = (SELECT person_id from endringsmelding WHERE id = :endringsmeldingId)  
            AND sendt is NULL AND id <= :endringsmeldingId
        """
    }

    private val dataSource by lazy(dataSourceGetter)

    private fun <R> transactionally(f: TransactionalSession.() -> R) =
        sessionOf(dataSource).use { session ->
            session.transaction { f(it) }
        }

    internal fun hentSendeklareEndringsmeldinger(block: (SendeklarEndringsmelding, TransactionalSession) -> Unit) {
        transactionally {
            run(
                queryOf(
                    FINN_SENDEKLARE_ENDRINGSMELDINGER,
                    mapOf("naavaerendeTidspunkt" to now())
                ).map { row ->
                    SendeklarEndringsmelding(
                        row.long("person_id"),
                        row.string("fnr"),
                        row.string("aktor_id"),
                        row.long("siste_endringsmelding_id")
                    )
                }.asList)
                .also {
                    if (!it.isEmpty()){
                        publiclog.info("Skal sende ${it.size} endringsmeldinger")
                        logger.info("Skal sende ${it.size} endringsmeldinger")
                    }
                }
                .onEach { melding ->
                    melding.oppdaterForfallstidspunkt(this)
                }
                .onEach { block(it, this) }
                .also {
                    if (!it.isEmpty()){
                        publiclog.info("Har håndtert ${it.size} endringsmeldinger")
                        logger.info("Har håndtert ${it.size} endringsmeldinger")
                    }
                }
        }
    }

    internal class SendeklarEndringsmelding(
        private val personId: Long,
        val fnr: String,
        val aktørId: String,
        val endringsmeldingId: Long
    ) {
        internal fun oppdaterForfallstidspunkt(session: TransactionalSession) {
            //logger.info("Setter neste forfallstidspunkt for personId $personId fnr $fnr (endringsmeldingId $endringsmeldingId)")
            //publiclog.info("Setter neste forfallstidspunkt for personId $personId endringsmeldingId $endringsmeldingId")
            setNesteForfallstidspunkt(session, personId)
        }

        internal fun lagreUtgåendeMelding(session: TransactionalSession, json: String): Boolean {
            if (!markerEndringsmeldingerSomSendt(session, endringsmeldingId)) return false
            return session.run(queryOf(UPDATE_UTGÅENDE, mapOf(
                "endringsmeldingId" to endringsmeldingId,
                "melding" to json
            )).asUpdate) == 1
        }

        private fun markerEndringsmeldingerSomSendt(session: TransactionalSession, endringsmeldingId: Long) =
            session.run(queryOf(MARKER_ENDRINGSMELDINGER_SOM_SENDT, mapOf(
                "endringsmeldingId" to endringsmeldingId,
                "naavaerendeTidspunkt" to now()
            )).asUpdate) > 0

        private fun setNesteForfallstidspunkt(session: TransactionalSession, personId: Long) {
            session.run(queryOf(SET_NESTE_FORFALLSDATO_FOR_PERSON, mapOf(
                "person_id" to personId,
                "neste_forfallstidspunkt" to nesteForfallstidspunkt
            )).asUpdate)
        }
    }

    internal fun lagreEndringsmelding(identer: IdentResponse, hendelseId: Long, json: String): Long =
        requireNotNull(lagreEndringsmeldingOgReturnerId(identer, hendelseId, json)) { "kunne ikke inserte endringsmelding eller person" }

    private fun lagreEndringsmeldingOgReturnerId(identer: IdentResponse, hendelseId: Long, json: String) =
        sessionOf(dataSource, returnGeneratedKey = true).use { session ->
            sikrePersonFinnes(session, identer)
            session.run(queryOf(INSERT_ENDRINGSMELDING, mapOf(
                "fnr" to identer.fødselsnummer,
                "hendelseId" to hendelseId,
                "melding" to json,
                "neste_forfallstidspunkt" to nesteForfallstidspunkt
            )).asUpdateAndReturnGeneratedKey)
        }

    private fun sikrePersonFinnes(session: Session, identer: IdentResponse) {
        session.run(queryOf(INSERT_PERSON, mapOf(
            "fnr" to identer.fødselsnummer,
            "aktor_id" to identer.aktørId
        )).asExecute)
    }
}