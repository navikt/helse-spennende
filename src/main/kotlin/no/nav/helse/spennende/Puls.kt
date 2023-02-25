package no.nav.helse.spennende

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

internal class Puls(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository,
    val skalPulsere: (forrigePulsering: LocalDateTime) -> Boolean = { it < LocalDateTime.now().minusSeconds(30) }
) : River.PacketListener {

    private var forrigePulsering = LocalDateTime.MIN
    private val publiclog = LoggerFactory.getLogger(Puls::class.java)
    private val logger = LoggerFactory.getLogger("tjenestekall")

    init {
        River(rapidsConnection).register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        if (!skalPulsere(forrigePulsering)) return
        pulser(context)
        forrigePulsering = LocalDateTime.now()
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
