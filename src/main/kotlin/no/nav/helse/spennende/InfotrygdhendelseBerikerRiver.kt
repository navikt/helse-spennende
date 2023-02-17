package no.nav.helse.spennende

import net.logstash.logback.argument.StructuredArguments
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class InfotrygdhendelseBerikerRiver(rapidsConnection: RapidsConnection, val repo: PostgresRepository) : River.PacketListener {

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.demandValue("@final", true)
                it.requireKey("@løsning.HentIdenter.fødselsnummer", "@løsning.HentIdenter.aktørId")
                it.requireKey("@id", "@opprettet", "endringsmeldingId")
            }
        }.register(this)
    }

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
        if (!repo.markerEndringsmeldingerSomLest(endringsmeldingId)) return sikkerlogg.info("Allerede løst dette behovet")
        if (!repo.lagreUtgåendeMelding(endringsmeldingId, utgående)) return sikkerlogg.error("republiserer ikke melding fordi vi klarte ikke lagre til db for {} {}:\n$utgående",
            StructuredArguments.keyValue("endringsmeldingId", endringsmeldingId),
            StructuredArguments.keyValue("fnr", fnr)
        )
        context.publish(fnr, utgående)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerlogg.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
    }

}