package no.nav.helse.spennende

import io.prometheus.client.Counter
import net.logstash.logback.argument.StructuredArguments
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class InfotrygdhendelseBerikerRiver(rapidsConnection: RapidsConnection, val repo: PostgresRepository) : River.PacketListener {

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val publiserteEndringer = Counter
            .build()
            .name("publiserte_infotrygdendringer")
            .help("Antall infotrygdendringer sendt videre på rapid etter at fnr er mappet til aktørId")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.demandKey("endringsmeldingId")
                it.demandValue("@final", true)
                it.requireKey("@løsning.HentIdenter.fødselsnummer", "@løsning.HentIdenter.aktørId")
                it.requireKey("@id", "@opprettet")
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
        if (!repo.lagreUtgåendeMelding(endringsmeldingId, utgående)) return sikkerlogg.warn("republiserer ikke melding fordi vi klarte ikke lagre til db for {} {}:\n$utgående",
            keyValue("endringsmeldingId", endringsmeldingId),
            keyValue("fnr", fnr)
        )
        publiserteEndringer.inc()
        //sikkerlogg.info("Viderepubliserer infotrygdmelding for endringsmeldingId $endringsmeldingId med fnr $fnr")
        context.publish(fnr, utgående)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerlogg.info("Forstod ikke infotrygdendring med løsning på behov:\n${problems.toExtendedReport()}")
    }

}