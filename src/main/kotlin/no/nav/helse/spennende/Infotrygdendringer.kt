package no.nav.helse.spennende

import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class Infotrygdendringer(rapidsConnection: RapidsConnection) : River.PacketListener {
    private companion object {
        private val logger = LoggerFactory.getLogger("tjenestekall")
    }
    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandKey("table")
                it.requireValue("after.TYPE_YTELSE", "SP")
                it.requireKey("op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TYPE_HENDELSE", "after.AKTOR_ID")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        logger.info("leste infotrygdendring:\n${packet.toJson()}")
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        logger.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
    }
}