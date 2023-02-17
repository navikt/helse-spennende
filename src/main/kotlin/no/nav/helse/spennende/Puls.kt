package no.nav.helse.spennende

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.time.LocalDateTime

internal class Puls(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository,
    val skalPulsere: (forrigePulsering: LocalDateTime) -> Boolean = { it < LocalDateTime.now().minusSeconds(30) }
) : River.PacketListener {

    private var forrigePulsering = LocalDateTime.MIN

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
                publiserBehov(melding.endringsmeldingId, melding.fnr, context)
                repo.markerEndringsmeldingerSomLest(this, melding.endringsmeldingId, melding.lest)
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
