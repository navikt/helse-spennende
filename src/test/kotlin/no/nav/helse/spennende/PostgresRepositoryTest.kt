package no.nav.helse.spennende

import kotlinx.coroutines.delay
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PostgresRepositoryTest {
    private val rapid = TestRapid()
    private lateinit var app: RapidsConnection


    private companion object {
        private const val fnr = "12345678911"
    }

    private lateinit var repository: PostgresRepository

    @BeforeAll
    fun createDatabase() {
        PgDb.start()
        repository = PostgresRepository { PgDb.connection() }
    }

    @AfterEach
    fun resetSchema() {
        PgDb.reset()
    }

    @Test
    fun `kan republisere dersom man aldri har gjort det før`() {
        val fnr = "123456789"
        val endringsmeldingId = repository.lagreEndringsmelding(fnr, 1, "{}")
        assertTrue(repository.skalRepublisere(endringsmeldingId, Duration.ofMinutes(1)))
    }

    @Test
    fun `kan ikke republisere før det har gått oppgitt tid siden sist`() {
        val fnr = "123456789"
        val endringsmeldingId = repository.lagreEndringsmelding(fnr, 1, "{}")
        val lengeTil = Duration.ofMinutes(1)
        repository.skalRepublisere(endringsmeldingId, lengeTil)
        assertFalse(repository.skalRepublisere(endringsmeldingId, lengeTil))
        Thread.sleep(20)
        assertTrue(repository.skalRepublisere(endringsmeldingId, Duration.ofMillis(10)))
    }

    @Language("JSON")
    private fun createTestMessage() = """{
  "table": "INFOTRYGD_Q1.TIL_VL_HENDELSE_SP",
  "op_type": "I",
  "op_ts": "2022-03-29 12:54:11.000000",
  "current_ts": "2022-03-29 13:14:27.396000",
  "pos": "00000000430000005465",
  "after": {
    "HENDELSE_ID": 12345678,
    "F_NR": "$fnr",
    "F_NR_SNUDD": "123456789",
    "TK_NR": "0315",
    "REGION": "2",
    "TABELLNAVN": "IS_UTBETALING_15",
    "KOLONNENAVN": " ",
    "KILDE_IS": "K222PBS3    ",
    "TABLE_ROW_ID": 92526463
  }
}
"""
}