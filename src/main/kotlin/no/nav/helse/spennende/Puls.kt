package no.nav.helse.spennende

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

internal class Puls(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository
) : River.PacketListener {

    private val publiclog = LoggerFactory.getLogger(Puls::class.java)
    private val logger = LoggerFactory.getLogger("tjenestekall")

    init {
        River(rapidsConnection)
            .validate {
                it.demandValue("@event_name", "minutt")
            }
            .register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        pulser(context)
    }

    private fun pulser(context: MessageContext) {
        publiclog.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        logger.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        repo.hentSendeklareEndringsmeldinger { melding ->
            publiserBehov(melding.endringsmeldingId, melding.fnr, context).also {
                publiclog.info("Publiserer behov for endringsmeldingId ${melding.endringsmeldingId}")
                logger.info("Publiserer behov for endringsmeldingId ${melding.endringsmeldingId} med fnr ${melding.fnr}")
            }
        }
    }
    private fun publiserBehov(
        endringsmeldingId: Long,
        fødselsnummer: String,
        context: MessageContext
    ) {
        val behov = JsonMessage.newNeed(
            listOf("HentIdenter"),
            mapOf("endringsmeldingId" to endringsmeldingId, "ident" to fødselsnummer)
        )
        context.publish(fødselsnummer, behov.toJson())
    }
}
