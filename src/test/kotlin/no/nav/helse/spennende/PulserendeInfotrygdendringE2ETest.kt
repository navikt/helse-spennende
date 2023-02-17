package no.nav.helse.spennende

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.contains
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.Duration
import java.util.*


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PulserendeInfotrygdendringE2ETest {

    private val rapid = TestRapid()

    private companion object {
        private const val fødselsnummer = "12345678911"
    }

    private lateinit var repository: PostgresRepository

    @BeforeAll
    fun createDatabase() {
        PgDb.start()
        repository = PostgresRepository { PgDb.connection() }
        InfotrygdhendelseRiver(rapid, repository)
        InfotrygdhendelseBerikerRiver(rapid, repository)
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
        mainpulerLestTidspunkt(hendelseId)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
    }

    @Test
    fun `flere infotrygdendringer på samme person`() {
        val hendelseId1 = 1234567L
        val hendelseId2 = 2234567L
        val hendelseId3 = 3234567L
        rapid.sendTestMessage(createTestMessage(hendelseId1))
        rapid.sendTestMessage(createTestMessage(hendelseId2))
        rapid.sendTestMessage(createTestMessage(hendelseId3))
        puls()
        assertEquals(0, rapid.inspektør.size)
        mainpulerLestTidspunkt(hendelseId1)
        mainpulerLestTidspunkt(hendelseId2)
        puls()
        assertEquals(0, rapid.inspektør.size)
        mainpulerLestTidspunkt(hendelseId3)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
    }
    @Test
    fun `flere infotrygdendringer på ulike personer`() {
        val hendelseId1 = 1234567L
        val hendelseId2 = 2234567L
        val hendelseId3 = 3234567L
        rapid.sendTestMessage(createTestMessage(hendelseId1, fnr = "1"))
        rapid.sendTestMessage(createTestMessage(hendelseId2, fnr = "2"))
        rapid.sendTestMessage(createTestMessage(hendelseId3, fnr = "3"))
        puls()
        assertEquals(0, rapid.inspektør.size)

        mainpulerLestTidspunkt(hendelseId1)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov(fnr = "1")

        mainpulerLestTidspunkt(hendelseId2)
        puls()
        assertEquals(2, rapid.inspektør.size)
        assertSendtBehov(fnr = "2")

        mainpulerLestTidspunkt(hendelseId3)
        puls()
        assertEquals(3, rapid.inspektør.size)
        assertSendtBehov(fnr = "3")
    }

    @Test
    fun `publiserer infotrygdendring når vi mottar løsning på behov`() {
        val hendelseId = 1234567L
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(0, rapid.inspektør.size)
        mainpulerLestTidspunkt(hendelseId)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
        assertSendInfotrygdendringVedLøsning()
    }

    private fun puls() {
        rapid.sendTestMessage("""{"@event_name": "ping"}""")
    }

    private fun assertSendInfotrygdendringVedLøsning() {
        val fnr = UUID.randomUUID().toString()
        val aktørId = UUID.randomUUID().toString()
        rapid.sendTestMessage(rapid.sisteMelding().medLøsning(fnr, aktørId).toString())
        val utgående = rapid.sisteMelding()

        Assertions.assertTrue(utgående.contains("@id"))
        assertEquals("infotrygdendring", utgående.path("@event_name").asText())
        assertEquals(fnr, utgående.path("fødselsnummer").asText())
        assertEquals(aktørId, utgående.path("aktørId").asText())
    }

    fun TestRapid.sisteMelding() = inspektør.message(inspektør.size -1)

    private fun JsonNode.medLøsning(fnr: String, aktørId: String) = (this as ObjectNode).apply {
        putObject("@løsning").apply {
            putObject("HentIdenter").apply {
                put("fødselsnummer", fnr)
                put("aktørId", aktørId)
            }
        }
        put("@final", true)
    }

    private fun assertSendtBehov(fnr: String = fødselsnummer) {
        assertEquals("behov", rapid.sisteMelding().path("@event_name").asText())
        assertEquals("HentIdenter", rapid.sisteMelding().path("@behov").single().asText())
        assertEquals(fnr, rapid.sisteMelding().path("ident").asText())
    }

    private fun mainpulerLestTidspunkt(hendelseId: Long, trekkFra: Duration = Duration.ofSeconds(301)) {
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
    private fun createTestMessage(hendelseId: Long = 12345678, fnr: String = fødselsnummer) = """{
  "table": "INFOTRYGD_Q1.TIL_VL_HENDELSE_SP",
  "op_type": "I",
  "op_ts": "2022-03-29 12:54:11.000000",
  "current_ts": "2022-03-29 13:14:27.396000",
  "pos": "00000000430000005465",
  "after": {
    "HENDELSE_ID": $hendelseId,
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