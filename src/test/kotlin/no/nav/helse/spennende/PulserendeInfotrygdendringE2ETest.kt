package no.nav.helse.spennende

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.Duration


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PulserendeInfotrygdendringE2ETest {

    private val rapid = TestRapid()

    private companion object {
        private const val fnr = "12345678911"
    }

    private lateinit var repository: PostgresRepository

    @BeforeAll
    fun createDatabase() {
        PgDb.start()
        repository = PostgresRepository { PgDb.connection() }
        InfotrygdhendelseRiver(rapid, repository)
        Puls(rapid, repository) { true }
    }

    @AfterEach
    fun resetSchema() {
        PgDb.reset()
    }

    @BeforeEach
    fun setup() {
        rapid.reset()
    }

    @Test
    fun `publiserer endring først etter fem minutter`() {
        val hendelseId = 1234567L
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(0, rapid.inspektør.size)
        liggOgLurk(hendelseId)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
    }

    private fun puls() {
        rapid.sendTestMessage("""{"@event_name": "ping"}""")
    }

    private fun assertSendtBehov() {
        val sistePåRapid = rapid.inspektør.message(rapid.inspektør.size -1 )
        assertEquals("behov", sistePåRapid.path("@event_name").asText())
        assertEquals("HentIdenter", sistePåRapid.path("@behov").single().asText())
    }

    private fun liggOgLurk(hendelseId: Long, trekkFra: Duration = Duration.ofSeconds(301)) {
        sessionOf(PgDb.connection()).use { session ->
            @Language("PostgreSQL")
            val sql = """
                UPDATE endringsmelding 
                SET lest = (SELECT lest FROM endringsmelding WHERE hendelse_id = ?) - INTERVAL '${trekkFra.seconds} SECONDS' 
                WHERE hendelse_id = ?
            """
            require(session.run(queryOf(sql, hendelseId, hendelseId).asUpdate) == 1)
        }
    }


    @Language("JSON")
    private fun createTestMessage(hendelseId: Long = 12345678) = """{
  "table": "INFOTRYGD_Q1.TIL_VL_HENDELSE_SP",
  "op_type": "I",
  "op_ts": "2022-03-29 12:54:11.000000",
  "current_ts": "2022-03-29 13:14:27.396000",
  "pos": "00000000430000005465",
  "after": {
    "HENDELSE_ID": $hendelseId,
    "F_NR": "${fnr}",
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