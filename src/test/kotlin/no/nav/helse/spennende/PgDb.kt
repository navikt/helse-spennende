package no.nav.helse.spennende

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.testcontainers.containers.PostgreSQLContainer
import java.time.Duration
import javax.sql.DataSource

internal object PgDb {

    private var state: DBState = NotStarted
    private val postgres = PostgreSQLContainer<Nothing>("postgres:14")
    private lateinit var dataSource: DataSource

    fun start(): PgDb {
        state.start(this)
        return this
    }

    fun reset() {
        state.reset(this)
    }

    fun config() = state.config(this)
    fun connection() = state.connection(this)

    private fun stop(): PgDb {
        state.stop(this)
        return this
    }

    private fun startDatbase() {
        postgres.start()
        dataSource = HikariDataSource(config())
        createSchema(connection())
        Runtime.getRuntime().addShutdownHook(Thread(this::stop))
    }

    private fun createSchema(dataSource: DataSource) {
        @Language("PostgreSQL")
        val initSql = """
            DO
${'$'}do${'$'}
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'cloudsqliamuser') THEN
        create user cloudsqliamuser with encrypted password 'foo';
    END IF;
END
${'$'}do${'$'};
        """.trimIndent()
        Flyway
            .configure()
            .dataSource(dataSource)
            .initSql(initSql)
            .load()
            .migrate()
        sessionOf(dataSource).use { it.run(queryOf(truncateTablesSql).asExecute) }
    }

    private fun resetSchema() {
        sessionOf(connection()).use { it.run(queryOf("SELECT truncate_tables();").asExecute) }
    }

    private fun stopDatabase() {
        postgres.stop()
    }

    private interface DBState {
        fun config(db: PgDb): HikariConfig {
            throw IllegalStateException("Cannot create config in state ${this::class.simpleName}")
        }
        fun connection(db: PgDb): DataSource {
            throw IllegalStateException("Cannot create connection in state ${this::class.simpleName}")
        }
        fun start(db: PgDb) {}
        fun stop(db: PgDb) {}
        fun reset(db: PgDb) {}
    }

    private object NotStarted : DBState {
        override fun start(db: PgDb) {
            state = Started
            db.startDatbase()
        }
    }

    private object Started : DBState {
        override fun stop(db: PgDb) {
            db.state = NotStarted
            db.stopDatabase()
        }

        override fun config(db: PgDb) = HikariConfig().apply {
            jdbcUrl = db.postgres.jdbcUrl
            username = db.postgres.username
            password = db.postgres.password
            connectionTimeout = Duration.ofSeconds(5).toMillis()
            maxLifetime = Duration.ofMinutes(30).toMillis()
            initializationFailTimeout = Duration.ofMinutes(1).toMillis()
        }

        override fun connection(db: PgDb): DataSource {
            return db.dataSource!!
        }

        override fun reset(db: PgDb) {
            db.resetSchema()
        }

    }

    @Language("PostgreSQL")
    private val truncateTablesSql = """
CREATE OR REPLACE FUNCTION truncate_tables() RETURNS void AS ${'$'}${'$'}
DECLARE
    statements CURSOR FOR
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename NOT LIKE 'flyway%';
BEGIN
    FOR stmt IN statements LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(stmt.tablename) || ' CASCADE;';
    END LOOP;
END;
${'$'}${'$'} LANGUAGE plpgsql;
"""
}