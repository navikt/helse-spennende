package no.nav.helse.spennende

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

internal class Puls(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository,
    val infotrygdendringutsender: Infotrygdendringutsender
) : River.PacketListener {
    private companion object {
        private val publiclog = LoggerFactory.getLogger(Puls::class.java)
        private val logger = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection)
            .validate { it.demandValue("@event_name", "minutt") }
            .register(this)
        River(rapidsConnection)
            .validate { it.demandValue("@event_name", "kjør_spennende") }
            .register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        infotrygdendringutsender.utsending {
            pulser(this, context)
        }
    }

    private fun pulser(kø: Utsendingskø, context: MessageContext) {
        publiclog.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        logger.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        repo.hentSendeklareEndringsmeldinger { melding ->
            try {
                val message = JsonMessage.newMessage("infotrygdendring", mapOf(
                    "fødselsnummer" to melding.fnr,
                    "endringsmeldingId" to melding.endringsmeldingId
                ))
                val utgående = message.toJson()
                //sikkerlogg.info("Viderepubliserer infotrygdmelding for endringsmeldingId $endringsmeldingId med fnr $fnr")
                kø.sendEndringsmelding(melding.fnr, utgående)
            } catch (err: Exception) {
                publiclog.error("Klarte ikke hente ident for endringsmelding: ${err.message}", err)
                logger.error("Klarte ikke hente ident for endringsmelding (se sikker logg)")
            }
        }
    }
}
