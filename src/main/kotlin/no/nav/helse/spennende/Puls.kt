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
        repo.transactionally {
            repo.hentSendeklareEndringsmeldinger(this).forEach { melding ->
                publiserBehov(melding.endringsmeldingId, melding.fnr, context).also {
                    logger.info("Publiserer behov for ${melding.endringsmeldingId} med ${melding.fnr}")
                }
                repo.setNesteForfallstidspunkt(this, melding.fnr).also {
                    logger.info("Setter neste forfallstidspunkt for ${melding.fnr}")
                }
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
