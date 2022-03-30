package no.nav.helse.spennende

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.contains
import com.zaxxer.hikari.HikariConfig
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

internal class InfotrygdendringE2ETest {
    private val rapid = TestRapid()
    private lateinit var app: RapidsConnection


    private companion object {
        private const val fnr = "12345678911"
    }

    @BeforeEach
    fun setup() {
        val env = mapOf(
            "LOCAL_DEVELOPMENT" to "true"
        )
        app = startApplication(rapid, env, HikariConfig())
        rapid.reset()
    }

    @Test
    fun `create test message`() {
        val fnr = UUID.randomUUID().toString()
        val aktørId = UUID.randomUUID().toString()

        rapid.sendTestMessage(createTestMessage())
        rapid.sendTestMessage((rapid.inspektør.message(0) as ObjectNode).apply {
            putObject("@løsning").apply {
                putObject("HentIdenter").apply {
                    put("fødselsnummer", fnr)
                    put("aktørId", aktørId)
                }
            }
        }.toString())
        val utgående = rapid.inspektør.message(1)

        assertTrue(utgående.contains("@id"))
        assertEquals("infotrygdendring", utgående.path("@event_name").asText())
        assertEquals(fnr, utgående.path("fødselsnummer").asText())
        assertEquals(aktørId, utgående.path("aktørId").asText())
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