package no.nav.helse.spennende

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory

internal class Puls(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository,
    val speedClient: SpeedClient
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

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        pulser(context)
    }

    private fun pulser(context: MessageContext) {
        publiclog.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        logger.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        repo.hentSendeklareEndringsmeldinger { melding, dbsession ->
            try {
                val identer = retryBlocking { speedClient.hentFødselsnummerOgAktørId(melding.fnr) }
                val message = JsonMessage.newMessage("infotrygdendring", mapOf(
                    "fødselsnummer" to identer.fødselsnummer,
                    "aktørId" to identer.aktørId,
                    "endringsmeldingId" to melding.endringsmeldingId
                ))
                val utgående = message.toJson()
                if (!melding.lagreUtgåendeMelding(dbsession, utgående)) {
                    logger.warn("republiserer ikke melding fordi vi klarte ikke lagre til db for {} {}:\n$utgående", kv("endringsmeldingId", melding.endringsmeldingId), kv("fnr", identer.fødselsnummer))
                } else {
                    Counter.builder("publiserte_infotrygdendringer")
                        .description("Antall infotrygdendringer sendt videre på rapid etter at fnr er mappet til aktørId")
                        .register(PrometheusMeterRegistry(PrometheusConfig.DEFAULT))
                        .increment()
                    //sikkerlogg.info("Viderepubliserer infotrygdmelding for endringsmeldingId $endringsmeldingId med fnr $fnr")
                    context.publish(identer.fødselsnummer, utgående)
                }
            } catch (err: Exception) {
                publiclog.error("Klarte ikke hente ident for endringsmelding: ${err.message}", err)
                logger.error("Klarte ikke hente ident for endringsmelding (se sikker logg)")
            }
        }
    }
}
