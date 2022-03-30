package no.nav.helse.spennende

import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*

internal class Infotrygdendringer(rapidsConnection: RapidsConnection) {
    private companion object {
        private val logger = LoggerFactory.getLogger("tjenestekall")
    }
    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name", "@behov")
                it.demandKey("after.F_NR")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TABELLNAVN")
            }
        }.register(InfotrygdendringerUtenAktørId())
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "infotrygdendring_uten_aktør")
                it.demandKey("@løsning.HentIdenter")
                it.requireKey("@løsning.HentIdenter.fødselsnummer", "@løsning.HentIdenter.aktørId")
                it.requireKey("@id", "@opprettet")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.F_NR", "after.TABELLNAVN")
            }
        }.register(InfotrygdendringerMedAktør())
    }

    private class InfotrygdendringerUtenAktørId : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val fnr = packet["after.F_NR"].asText().trim()
            packet["@id"] = UUID.randomUUID()
            packet["@event_name"] = "infotrygdendring_uten_aktør"
            packet["@opprettet"] = LocalDateTime.now()
            packet["@behovId"] = UUID.randomUUID()
            packet["@behov"] = listOf("HentIdenter")
            packet["ident"] = fnr
            logger.info("leste infotrygdendring, sender behov for å finne identer:\n${packet.toJson()}")
            context.publish(fnr, packet.toJson())
        }

        override fun onError(problems: MessageProblems, context: MessageContext) {
            logger.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
        }
    }

    private class InfotrygdendringerMedAktør : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val fnr = packet["@løsning.HentIdenter.fødselsnummer"].asText()
            val aktørId = packet["@løsning.HentIdenter.aktørId"].asText()

            val message = JsonMessage.newMessage("infotrygdendring", mapOf(
                "fødselsnummer" to fnr,
                "aktørId" to aktørId
            ))
            logger.info("republiserer infotrygdendring med fnr:\n${message.toJson()}")
            context.publish(fnr, message.toJson())
        }

        override fun onError(problems: MessageProblems, context: MessageContext) {
            logger.info("forstod ikke infotrygdendring (med svar på behov):\n${problems.toExtendedReport()}")
        }
    }
}