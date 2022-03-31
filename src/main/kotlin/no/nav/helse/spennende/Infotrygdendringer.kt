package no.nav.helse.spennende

import io.prometheus.client.Counter
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

internal class Infotrygdendringer(rapidsConnection: RapidsConnection, repo: PostgresRepository) {
    private companion object {
        private val logger = LoggerFactory.getLogger("tjenestekall")
        private val ratebegrensning = Duration.ofMinutes(1)
        private const val RATE_BEGRENSET = "1"
        private const val IKKE_RATE_BEGRENSET = "0"
        private val endringer = Counter
            .build()
            .name("infotrygdendringer")
            .labelNames("ratebegrenset")
            .help("Teller alle innkommende infotrygdendringer, og angir hvorvidt de er rate-begrenset (1) eller ikke (0). Ikke-ratebegrensede endringer sendes videre for å mappe fnr til aktørId")
            .register()
        private val publiserteEndringer = Counter
            .build()
            .name("publiserte_infotrygdendringer")
            .help("Antall infotrygdendringer sendt videre på rapid etter at fnr er mappet til aktørId")
            .register()
    }
    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name", "@behov")
                it.demandKey("after.F_NR")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TABELLNAVN")
            }
        }.register(InfotrygdendringerUtenAktørId(repo))
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "infotrygdendring_uten_aktør")
                it.demandKey("@løsning.HentIdenter")
                it.demandValue("@final", true)
                it.requireKey("@løsning.HentIdenter.fødselsnummer", "@løsning.HentIdenter.aktørId")
                it.requireKey("@id", "@opprettet", "endringsmeldingId")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.F_NR", "after.TABELLNAVN")
            }
        }.register(InfotrygdendringerMedAktør(repo))
    }

    private class InfotrygdendringerUtenAktørId(private val repo: PostgresRepository) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val hendelseId = packet["after.HENDELSE_ID"].asText().trim().toLong()
            val fnr = packet["after.F_NR"].asText().trim()
            try {
                val endringsmeldingId = repo.lagreEndringsmelding(fnr, hendelseId, packet.toJson())
                republiser(endringsmeldingId, fnr, packet, context)
            } catch (err: Exception) {
                logger.error("Feil ved lagring av endringsmelding for {} {}: {}", keyValue("hendelse_id", hendelseId), keyValue("fnr", fnr), err.message, err)
            }
        }

        override fun onError(problems: MessageProblems, context: MessageContext) {
            logger.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
        }

        private fun republiser(endringsmeldingId: Long, fnr: String, packet: JsonMessage, context: MessageContext) {
            if (!repo.skalRepublisere(endringsmeldingId, ratebegrensning)) {
                endringer.labels(RATE_BEGRENSET).inc()
                return logger.info("republiserer ikke {} for {} pga rate-begrensning",
                    keyValue("endringsmeldingId", endringsmeldingId), keyValue("fnr", fnr))
            }
            endringer.labels(IKKE_RATE_BEGRENSET).inc()
            packet["@id"] = UUID.randomUUID()
            packet["@event_name"] = "infotrygdendring_uten_aktør"
            packet["@opprettet"] = LocalDateTime.now()
            packet["@behovId"] = UUID.randomUUID()
            packet["@behov"] = listOf("HentIdenter")
            packet["endringsmeldingId"] = endringsmeldingId
            packet["ident"] = fnr
            logger.info("leste infotrygdendring, sender behov for å finne identer:\n${packet.toJson()}")
            context.publish(fnr, packet.toJson())
        }
    }

    private class InfotrygdendringerMedAktør(private val repo: PostgresRepository) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val endringsmeldingId = packet["endringsmeldingId"].asLong()
            val fnr = packet["@løsning.HentIdenter.fødselsnummer"].asText()
            val aktørId = packet["@løsning.HentIdenter.aktørId"].asText()

            val message = JsonMessage.newMessage("infotrygdendring", mapOf(
                "fødselsnummer" to fnr,
                "aktørId" to aktørId,
                "endringsmeldingId" to endringsmeldingId
            ))
            val utgående = message.toJson()
            if (!repo.lagreUtgåendeMelding(endringsmeldingId, utgående)) return logger.error("republiserer ikke melding fordi vi klarte ikke lagre til db for {} {}:\n$utgående", keyValue("endringsmeldingId", endringsmeldingId), keyValue("fnr", fnr))
            publiserteEndringer.inc()
            logger.info("republiserer infotrygdendring med fnr:\n\t$utgående\n\nInnkommende:\n\t${packet.toJson()}")
            context.publish(fnr, utgående)
        }

        override fun onError(problems: MessageProblems, context: MessageContext) {
            logger.info("forstod ikke infotrygdendring (med svar på behov):\n${problems.toExtendedReport()}")
        }
    }
}