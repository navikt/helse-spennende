package no.nav.helse.spennende

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.net.ConnectException
import javax.sql.DataSource
import kotlin.reflect.KClass

private val log = LoggerFactory.getLogger("no.nav.helse.spennende.App")

private val env = System.getenv()
private val hikariConfig = HikariConfig().apply {
    jdbcUrl = String.format(
        "jdbc:postgresql://%s:%s/%s?user=%s",
        env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_HOST"),
        env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_PORT"),
        env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_DATABASE"),
        env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_USERNAME")
    )
    password = env.getValue("NAIS_DATABASE_SPENNENDE_SPENNENDE_PASSWORD")
    initializationFailTimeout = 5000
    maximumPoolSize = 3
    minimumIdle = 1
    idleTimeout = 10001
    connectionTimeout = 1000
    maxLifetime = 30001
}

private val dataSourceInitializer = DataSourceInitializer(hikariConfig)
// TODO: use repo for something
private val repo = PostgresRepository(dataSourceInitializer::getDataSource)

private val rapidsConnection = RapidApplication.create(env).apply {
    register(dataSourceInitializer)
    // TODO add rivers
}

fun main() {
    rapidsConnection.start()
}

private class PostgresRepository(dataSourceGetter: () -> DataSource) {
    private val dataSource by lazy(dataSourceGetter)
}

private class DataSourceInitializer(private val hikariConfig: HikariConfig) : RapidsConnection.StatusListener {
    private lateinit var dataSource: DataSource

    fun getDataSource(): DataSource {
        check(this::dataSource.isInitialized) { "The data source has not been initialized yet!" }
        return dataSource
    }

    override fun onStartup(rapidsConnection: RapidsConnection) {
        while (!initializeDataSource()) {
            log.info("Database is not available yet, trying again")
            Thread.sleep(250)
        }
        migrate(dataSource)
    }

    private fun initializeDataSource(): Boolean {
        try {
            dataSource = HikariDataSource(hikariConfig)
            return true
        } catch (err: Exception) {
            err.allow(ConnectException::class)
        }
        return false
    }

    private companion object {
        fun Throwable.allow(clazz: KClass<out Throwable>) {
            if (causes().any { clazz.isInstance(it) }) return
            throw this
        }

        fun Throwable.causes(): List<Throwable> {
            return mutableListOf<Throwable>(this).apply {
                var nextError: Throwable? = cause
                while (nextError != null) {
                    add(nextError)
                    nextError = nextError.cause
                }
            }
        }
        fun migrate(dataSource: DataSource) {
            Flyway.configure().dataSource(dataSource).load().migrate()
        }
    }
}