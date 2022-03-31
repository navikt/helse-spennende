package no.nav.helse.spennende

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.contains
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class InfotrygdendringE2ETest {
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

    @BeforeEach
    fun setup() {
        val env = mapOf(
            "LOCAL_DEVELOPMENT" to "true"
        )
        app = startApplication(rapid, PgDb.config())
        rapid.reset()
    }

    @Test
    fun `create test message`() {
        rapid.sendTestMessage(createTestMessage())
        assertEquals(1, rapid.inspektør.size)
        assertLøsning()
    }

    @Test
    fun `reagerer ikke på behov med løsning uten @final`() {
        rapid.sendTestMessage(createTestMessage())
        assertEquals(1, rapid.inspektør.size)
        rapid.sendTestMessage(rapid.inspektør.message(0).medLøsning(fnr, "aktørid").apply {
            remove("@final")
        }.toString())
        assertEquals(1, rapid.inspektør.size)
    }

    @Test
    fun `rate limit`() {
        repeat(10) { rapid.sendTestMessage(createTestMessage()) }
        assertEquals(1, rapid.inspektør.size)
        assertLøsning()
    }

    private fun assertLøsning() {
        val fnr = UUID.randomUUID().toString()
        val aktørId = UUID.randomUUID().toString()
        rapid.sendTestMessage(rapid.inspektør.message(0).medLøsning(fnr, aktørId).toString())
        val utgående = rapid.inspektør.message(1)

        assertTrue(utgående.contains("@id"))
        assertEquals("infotrygdendring", utgående.path("@event_name").asText())
        assertEquals(fnr, utgående.path("fødselsnummer").asText())
        assertEquals(aktørId, utgående.path("aktørId").asText())
    }

    private fun JsonNode.medLøsning(fnr: String, aktørId: String) = (this as ObjectNode).apply {
        putObject("@løsning").apply {
            putObject("HentIdenter").apply {
                put("fødselsnummer", fnr)
                put("aktørId", aktørId)
            }
        }
        put("@final", true)
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