package no.nav.helse.spennende

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.createAzureTokenClientFromEnvironment
import com.github.navikt.tbd_libs.kafka.AivenConfig
import com.github.navikt.tbd_libs.kafka.ConsumerProducerFactory
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.core.instrument.Clock
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import no.nav.helse.rapids_rivers.RapidApplication
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.net.http.HttpClient
import java.time.Duration
import javax.sql.DataSource

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)
private val log = LoggerFactory.getLogger("no.nav.helse.spennende.App")

private val hikariConfig by lazy {
    val env = System.getenv()
    HikariConfig().apply {
        jdbcUrl = String.format(
            "jdbc:postgresql://%s:%s/%s?user=%s",
            env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_HOST"),
            env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_PORT"),
            env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_DATABASE"),
            env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_USERNAME")
        )
        password = env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_PASSWORD")
        maximumPoolSize = 2
        connectionTimeout = Duration.ofSeconds(5).toMillis()
        maxLifetime = Duration.ofMinutes(30).toMillis()
        initializationFailTimeout = Duration.ofMinutes(1).toMillis()
    }
}

fun main() {
    val env = System.getenv()
    val factory = ConsumerProducerFactory(AivenConfig.default)
    val topicForInfotygdendringer = env.getValue("TOPIC_FOR_INFOTYGDENDRINGER")

    val producer = when {
        env["HINDRE_UTSENDING"].toBoolean() -> InfotrygdendringProducer.TomProducer()
        else -> InfotrygdendringProducer.RapidProducer(topicForInfotygdendringer, factory.createProducer(withShutdownHook = true))
    }
    val infotrygdendringutsending = Infotrygdendringutsender(producer)

    startApplication(RapidApplication.create(env, factory, meterRegistry), infotrygdendringutsending, hikariConfig, env)
}

internal fun startApplication(rapidsConnection: RapidsConnection, infotrygdendringutsender: Infotrygdendringutsender, hikariConfig: HikariConfig, env: Map<String, String>): RapidsConnection {
    val dataSourceInitializer = DataSourceInitializer(hikariConfig)
    val repo = PostgresRepository(dataSourceInitializer::dataSource)

    val httpClient: HttpClient = HttpClient.newHttpClient()
    val azureClient = createAzureTokenClientFromEnvironment(env)
    val speedClient = SpeedClient(httpClient, jacksonObjectMapper().registerModule(JavaTimeModule()), azureClient)

    return rapidsConnection.apply {
        register(dataSourceInitializer)
        InfotrygdhendelseRiver(this, repo, speedClient)
        Puls(this, repo, infotrygdendringutsender)
    }.also { it.start() }
}

class Infotrygdendringutsender(
    private val producer: InfotrygdendringProducer
) {
    fun startUtsending() = Utsendingskø(producer)
    fun utsending(block: Utsendingskø.() -> Unit) {
        startUtsending().use {
            block(it)
        }
    }
}

class Utsendingskø(private val producer: InfotrygdendringProducer) : AutoCloseable {
    fun sendEndringsmelding(fnr: String, melding: String) {
        producer.sendEndringsmelding(fnr, melding)
    }

    override fun close() {
        producer.tømKø()
    }
}

interface InfotrygdendringProducer {
    fun sendEndringsmelding(fnr: String, melding: String)
    fun tømKø()

    class TomProducer : InfotrygdendringProducer {
        override fun sendEndringsmelding(fnr: String, melding: String) {
            log.info("Sender IKKE endringsmelding videre fordi applikasjonen er konfigurert til å stoppe utsendinger.")
        }
        override fun tømKø() {}
    }
    class RapidProducer(val topic: String, val kafkaProducer: KafkaProducer<String, String>) : InfotrygdendringProducer {
        override fun sendEndringsmelding(fnr: String, melding: String) {
            kafkaProducer.send(ProducerRecord(topic, fnr, melding))
        }

        override fun tømKø() {
            kafkaProducer.flush()
        }
    }
}

private class DataSourceInitializer(private val hikariConfig: HikariConfig) : RapidsConnection.StatusListener {
    internal val dataSource: DataSource by lazy { HikariDataSource(hikariConfig) }

    override fun onStartup(rapidsConnection: RapidsConnection) {
        migrate(dataSource)
    }

    private companion object {
        fun migrate(dataSource: DataSource) {
            Flyway
                .configure()
                .dataSource(dataSource)
                .lockRetryCount(-1)
                .load()
                .migrate()
        }
    }
}