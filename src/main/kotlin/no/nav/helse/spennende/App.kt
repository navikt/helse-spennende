package no.nav.helse.spennende

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.helse.rapids_rivers.*
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.SocketException
import javax.sql.DataSource
import kotlin.reflect.KClass

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
        initializationFailTimeout = 5000
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
    }
}

fun main() {
    val env = System.getenv()
    startApplication(RapidApplication.create(env), env, hikariConfig)
}

internal fun startApplication(rapidsConnection: RapidsConnection, env: Map<String, String>, hikariConfig: HikariConfig): RapidsConnection {
    val dataSourceInitializer = DataSourceInitializer(hikariConfig)
    // TODO: use repo for something
    val repo = PostgresRepository(dataSourceInitializer::getDataSource)

    return rapidsConnection.apply {
        register(dataSourceInitializer)
        // TODO add rivers
    }.also { it.start() }
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
            err.allow(ConnectException::class, SocketException::class)
        }
        return false
    }

    private companion object {
        fun Throwable.allow(vararg clazz: KClass<out Throwable>) {
            if (causes().any { cause -> clazz.any { clz -> clz.isInstance(cause) } }) return
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