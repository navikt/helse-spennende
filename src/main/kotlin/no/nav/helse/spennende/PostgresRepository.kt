package no.nav.helse.spennende

import com.github.navikt.tbd_libs.speed.IdentResponse
import java.time.LocalDateTime
import java.time.LocalDateTime.now
import javax.sql.DataSource
import kotliquery.Session
import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory

internal class PostgresRepository(dataSourceGetter: () -> DataSource) {
    private companion object {
        private val publiclog = LoggerFactory.getLogger(PostgresRepository::class.java)
        private val logger = LoggerFactory.getLogger("tjenestekall")

        @Language("PostgreSQL")
        private const val INSERT_PERSON = """INSERT INTO person (fnr) VALUES (:fnr) ON CONFLICT(fnr) DO NOTHING"""
        @Language("PostgreSQL")
        private const val INSERT_ENDRINGSMELDING = """INSERT INTO endringsmelding (person_id, hendelse_id, innkommende_melding, neste_forfallstidspunkt) VALUES ((SELECT id FROM person WHERE fnr = :fnr), :hendelseId, :melding, :neste_forfallstidspunkt)"""
        @Language("PostgreSQL")
        private const val FINN_SENDEKLARE_ENDRINGSMELDINGER = """
            WITH alleIkkeSendteEndringsmeldinger AS (
                SELECT MAX(e.id) as siste_endringsmelding_id, e.person_id
                FROM endringsmelding e
                WHERE sendt IS NULL
                GROUP BY person_id
                HAVING MAX(neste_forfallstidspunkt) <= :naavaerendeTidspunkt
                LIMIT 30000
            )
            SELECT p.id as person_id, p.fnr, e.siste_endringsmelding_id
            FROM person p
            INNER JOIN alleIkkeSendteEndringsmeldinger e ON e.person_id=p.id
            FOR UPDATE SKIP LOCKED
            """

        @Language("PostgreSQL")
        private const val MARKER_ENDRINGSMELDINGER_SOM_SENDT = """
            UPDATE endringsmelding SET sendt = :naavaerendeTidspunkt 
            WHERE person_id = :personId  
            AND sendt is NULL AND id <= :endringsmeldingId
        """
    }

    private val dataSource by lazy(dataSourceGetter)

    private fun <R> transactionally(f: TransactionalSession.() -> R) =
        sessionOf(dataSource).use { session ->
            session.transaction { f(it) }
        }

    internal fun hentSendeklareEndringsmeldinger(block: (SendeklarEndringsmelding) -> Unit) {
        transactionally {
            run(
                queryOf(
                    FINN_SENDEKLARE_ENDRINGSMELDINGER,
                    mapOf("naavaerendeTidspunkt" to now())
                ).map { row ->
                    SendeklarEndringsmelding(
                        row.long("person_id"),
                        row.string("fnr"),
                        row.long("siste_endringsmelding_id")
                    )
                }.asList)
                .also {
                    if (!it.isEmpty()){
                        publiclog.info("Skal sende ${it.size} endringsmeldinger")
                        logger.info("Skal sende ${it.size} endringsmeldinger")
                    }
                }
                .onEach { block(it) }
                .onEach {
                    it.markerEndringsmeldingerSomSendt(this)
                }
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
        val endringsmeldingId: Long
    ) {
        internal fun markerEndringsmeldingerSomSendt(session: TransactionalSession) =
            session.run(queryOf(MARKER_ENDRINGSMELDINGER_SOM_SENDT, mapOf(
                "personId" to personId,
                "endringsmeldingId" to endringsmeldingId,
                "naavaerendeTidspunkt" to now()
            )).asUpdate) > 0
    }

    internal fun lagreEndringsmelding(identer: IdentResponse, hendelseId: Long, json: String, forfallstidspunkt: LocalDateTime): Long =
        requireNotNull(lagreEndringsmeldingOgReturnerId(identer, hendelseId, json, forfallstidspunkt)) { "kunne ikke inserte endringsmelding eller person" }

    private fun lagreEndringsmeldingOgReturnerId(identer: IdentResponse, hendelseId: Long, json: String, forfallstidspunkt: LocalDateTime) =
        sessionOf(dataSource, returnGeneratedKey = true).use { session ->
            sikrePersonFinnes(session, identer)
            session.run(queryOf(INSERT_ENDRINGSMELDING, mapOf(
                "fnr" to identer.fødselsnummer,
                "hendelseId" to hendelseId,
                "melding" to json,
                "neste_forfallstidspunkt" to forfallstidspunkt
            )).asUpdateAndReturnGeneratedKey)
        }

    private fun sikrePersonFinnes(session: Session, identer: IdentResponse) {
        session.run(queryOf(INSERT_PERSON, mapOf(
            "fnr" to identer.fødselsnummer
        )).asExecute)
    }
}
