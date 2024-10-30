package no.nav.helse.spennende

import com.github.navikt.tbd_libs.kafka.ConsumerProducerFactory
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

internal class Puls(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository,
    val topic: String,
    val consumerProducerFactory: ConsumerProducerFactory
) : River.PacketListener {
    private companion object {
        private val publiclog = LoggerFactory.getLogger(Puls::class.java)
        private val logger = LoggerFactory.getLogger("tjenestekall")
        private val publiserteEndringer = Counter.builder("publiserte_infotrygdendringer")
            .description("Antall infotrygdendringer sendt videre på rapid etter at fnr er mappet til aktørId")
            .register(PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM))
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
        consumerProducerFactory.createProducer(withShutdownHook = false).use { producer ->
            pulser(producer, context)
            producer.flush()
        }
    }

    private fun pulser(producer: KafkaProducer<String, String>, context: MessageContext) {
        publiclog.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        logger.info("Pulserer, sjekker for sendeklare infotrygdendringsmeldinger")
        repo.hentSendeklareEndringsmeldinger { melding ->
            try {
                val message = JsonMessage.newMessage("infotrygdendring", mapOf(
                    "fødselsnummer" to melding.fnr,
                    "aktørId" to melding.aktørId,
                    "endringsmeldingId" to melding.endringsmeldingId
                ))
                val utgående = message.toJson()
                publiserteEndringer.increment()
                //sikkerlogg.info("Viderepubliserer infotrygdmelding for endringsmeldingId $endringsmeldingId med fnr $fnr")
                producer.send(ProducerRecord(topic, melding.fnr, utgående))
            } catch (err: Exception) {
                publiclog.error("Klarte ikke hente ident for endringsmelding: ${err.message}", err)
                logger.error("Klarte ikke hente ident for endringsmelding (se sikker logg)")
            }
        }
    }
}
