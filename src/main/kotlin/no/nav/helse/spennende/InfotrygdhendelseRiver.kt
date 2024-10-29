package no.nav.helse.spennende

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import net.logstash.logback.argument.StructuredArguments
import org.slf4j.LoggerFactory

internal class InfotrygdhendelseRiver(
    rapidsConnection: RapidsConnection,
    val repo: PostgresRepository,
    val speedClient: SpeedClient
) : River.PacketListener {

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name", "@behov")
                it.demandKey("after.F_NR")
                it.requireKey("table", "op_type", "op_ts", "current_ts", "pos",
                    "after.HENDELSE_ID", "after.TABELLNAVN")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val hendelseId = packet["after.HENDELSE_ID"].asText().trim().toLong()
        val fnr = packet["after.F_NR"].asText().trim()
        try {
            val identer = retryBlocking { speedClient.hentFødselsnummerOgAktørId(fnr) }

            val endringsmeldingId = repo.lagreEndringsmelding(identer, hendelseId, packet.toJson())
            sikkerlogg.info("leste infotrygdendring som ble lagret med endringsmeldingId $endringsmeldingId for fnr $fnr")

            Counter.builder("infotrygdendringer")
                .description("Teller alle innkommende infotrygdendringer, og angir tabellnavn")
                .tag("tabellnavn", packet["after.TABELLNAVN"].asText())
                .register(PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM))
                .increment()
        } catch (err: Exception) {
            sikkerlogg.error("Feil ved lagring av endringsmelding for {} {}: {}",
                StructuredArguments.keyValue("hendelse_id", hendelseId),
                StructuredArguments.keyValue("fnr", fnr), err.message, err)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerlogg.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
    }

}