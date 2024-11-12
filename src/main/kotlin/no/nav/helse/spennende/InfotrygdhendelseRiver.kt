package no.nav.helse.spennende

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.Result
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
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

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val hendelseId = packet["after.HENDELSE_ID"].asText().trim().toLong()
        val fnr = packet["after.F_NR"].asText().trim()
        try {
            sikkerlogg.info("henter gjeldende identer for fnr $fnr")
            val identer = retryBlocking {
                when (val svar = speedClient.hentFødselsnummerOgAktørId(fnr)) {
                    is Result.Error -> throw RuntimeException(svar.error, svar.cause)
                    is Result.Ok -> svar.value
                }
            }

            val endringsmeldingId = repo.lagreEndringsmelding(identer, hendelseId, packet.toJson())
            sikkerlogg.info("leste infotrygdendring som ble lagret med endringsmeldingId $endringsmeldingId for fnr $fnr")

            Counter.builder("infotrygdendringer")
                .description("Teller alle innkommende infotrygdendringer, og angir tabellnavn")
                .tag("tabellnavn", packet["after.TABELLNAVN"].asText())
                .register(meterRegistry)
                .increment()
        } catch (err: Exception) {
            sikkerlogg.error("Feil ved lagring av endringsmelding for {} {}: {}",
                StructuredArguments.keyValue("hendelse_id", hendelseId),
                StructuredArguments.keyValue("fnr", fnr), err.message, err)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerlogg.info("forstod ikke infotrygdendring:\n${problems.toExtendedReport()}")
    }

}