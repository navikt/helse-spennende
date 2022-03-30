package no.nav.helse.spennende

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
            if (!repo.skalRepublisere(endringsmeldingId, ratebegrensning)) return logger.info("republiserer ikke {} for {} pga rate-begrensning",
                keyValue("endringsmeldingId", endringsmeldingId), keyValue("fnr", fnr))
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
            logger.info("republiserer infotrygdendring med fnr:\n$utgående")
            context.publish(fnr, utgående)
        }

        override fun onError(problems: MessageProblems, context: MessageContext) {
            logger.info("forstod ikke infotrygdendring (med svar på behov):\n${problems.toExtendedReport()}")
        }
    }
}