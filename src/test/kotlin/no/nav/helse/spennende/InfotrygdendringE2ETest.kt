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
        private const val aktørId = "aktørId"
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
  "table": "INFOTRYGD_Q1.T_HENDELSE",
  "op_type": "I",
  "op_ts": "2022-02-08 06:35:12.000000",
  "current_ts": "2022-02-08 21:56:45.470000",
  "pos": "00000000380000013863",
  "after": {
    "HENDELSE_ID": 12345678,
    "TYPE_HENDELSE": "INNVILGET      ",
    "AKTOR_ID": "$aktørId       ",
    "TYPE_YTELSE": "SP",
    "IDENTDATO": "20211101",
    "FOM": "2021-11-01 00:00:00",
    "SATS": 0,
    "KOBLING_ID": 0,
    "BRUKERID": "ZZZ1234 ",
    "TIDSPUNKT_REG": "2022-02-08 06:34:59.974950000",
    "OPPRETTET": "2022-02-08 06:35:05.462228000",
    "OPPDATERT": "2022-02-08 06:35:05.462228000",
    "DB_SPLITT": "  "
  },
  "system_read_count": 0
}
"""
}