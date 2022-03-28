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
                it.demandKey("table")
                it.requireValue("after.TYPE_YTELSE", "SP")
                it.requireKey("op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TYPE_HENDELSE", "after.AKTOR_ID")
            }
        }.register(InfotrygdendringerUtenFødselsnummer())
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "infotrygdendring_uten_fnr")
                it.demandKey("@løsning.HentIdenter")
                it.requireKey("@løsning.HentIdenter.fødselsnummer", "@løsning.HentIdenter.aktørId")
                it.requireKey("@id", "@opprettet")
                it.requireValue("after.TYPE_YTELSE", "SP")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TYPE_HENDELSE", "after.AKTOR_ID")
            }
        }.register(InfotrygdendringerMedFødselsnummer())
    }

    private class InfotrygdendringerUtenFødselsnummer : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val aktørId = packet["after.AKTOR_ID"].asText().trim()
            packet["@id"] = UUID.randomUUID()
            packet["@event_name"] = "infotrygdendring_uten_fnr"
            packet["@opprettet"] = LocalDateTime.now()
            packet["@behovId"] = UUID.randomUUID()
            packet["@behov"] = listOf("HentIdenter")
            packet["ident"] = aktørId
            logger.info("leste infotrygdendring, sender behov for å finne identer:\n${packet.toJson()}")
            context.publish(aktørId, packet.toJson())
        }

        override fun onError(problems: MessageProblems, context: MessageContext) {
            logger.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
        }
    }

    private class InfotrygdendringerMedFødselsnummer : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val fnr = packet["@løsning.HentIdenter.fødselsnummer"].asText()
            val aktørId = packet["@løsning.HentIdenter.aktørId"].asText()

            val message = JsonMessage.newMessage(mapOf(
                "@id" to UUID.randomUUID(),
                "@event_name" to "infotrygdendring",
                "@forårsaket_av" to mapOf(
                    "event_name" to packet["@event_name"].asText(),
                    "id" to packet["@id"].asText(),
                    "opprettet" to packet["@opprettet"].asText()
                ),
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