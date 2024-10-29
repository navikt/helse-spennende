package no.nav.helse.spennende

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal class AktørIdMigrering(
    rapidsConnection: RapidsConnection,
    val dataSourceGetter: () -> DataSource,
    val speedClient: SpeedClient
) : River.PacketListener {
    private companion object {
        private val publiclog = LoggerFactory.getLogger(Puls::class.java)
        private val logger = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection)
            .validate { it.demandValue("@event_name", "spennende_aktør_migrering") }
            .validate { it.requireKey("fødselsnummer") }
            .register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val fnr = packet["fødselsnummer"].asText()
        retryBlocking { speedClient.hentFødselsnummerOgAktørId(fnr) }.aktørId.also {
            sessionOf(dataSourceGetter()).use {
                it.run(queryOf("update person set aktor_id = ? where fnr = ?", it, fnr).asUpdate)
            }
        }
    }
}
