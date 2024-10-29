package no.nav.helse.spennende

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.createAzureTokenClientFromEnvironment
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.net.http.HttpClient
import java.time.Duration
import javax.sql.DataSource

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
    startApplication(RapidApplication.create(env), hikariConfig, env)
}

internal fun startApplication(rapidsConnection: RapidsConnection, hikariConfig: HikariConfig, env: Map<String, String>): RapidsConnection {
    val dataSourceInitializer = DataSourceInitializer(hikariConfig)
    val repo = PostgresRepository(dataSourceInitializer::dataSource)

    val httpClient: HttpClient = HttpClient.newHttpClient()
    val azureClient = createAzureTokenClientFromEnvironment(env)
    val speedClient = SpeedClient(httpClient, jacksonObjectMapper().registerModule(JavaTimeModule()), azureClient)

    return rapidsConnection.apply {
        register(dataSourceInitializer)
        InfotrygdhendelseRiver(this, repo)
        Puls(this, repo, speedClient)
    }.also { it.start() }
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