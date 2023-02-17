package no.nav.helse.spennende

import net.logstash.logback.argument.StructuredArguments
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class InfotrygdhendelseRiver(rapidsConnection: RapidsConnection, val repo: PostgresRepository) : River.PacketListener {

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name", "@behov")
                it.demandKey("after.F_NR")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TABELLNAVN")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val hendelseId = packet["after.HENDELSE_ID"].asText().trim().toLong()
        val fnr = packet["after.F_NR"].asText().trim()
        try {
            repo.lagreEndringsmelding(fnr, hendelseId, packet.toJson())
        } catch (err: Exception) {
            sikkerlogg.error("Feil ved lagring av endringsmelding for {} {}: {}",
                StructuredArguments.keyValue("hendelse_id", hendelseId),
                StructuredArguments.keyValue("fnr", fnr), err.message, err)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerlogg.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
    }

}