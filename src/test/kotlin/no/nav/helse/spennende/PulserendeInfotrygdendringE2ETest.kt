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


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PulserendeInfotrygdendringE2ETest {

    private val rapid = TestRapid()

    private companion object {
        private const val fødselsnummer = "12345678911"
        private const val aktør = "aktør"
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
        setEndringsmeldingTilForfall(hendelseId)
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
        setEndringsmeldingTilForfall(hendelseId1)
        setEndringsmeldingTilForfall(hendelseId2)
        puls()
        assertEquals(0, rapid.inspektør.size)
        setEndringsmeldingTilForfall(hendelseId3)
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

        setEndringsmeldingTilForfall(hendelseId1)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov(fnr = "1")
        assertSendInfotrygdendringVedLøsning()

        setEndringsmeldingTilForfall(hendelseId2)
        puls()
        assertEquals(3, rapid.inspektør.size)
        assertSendtBehov(fnr = "2")
        assertSendInfotrygdendringVedLøsning()

        setEndringsmeldingTilForfall(hendelseId3)
        puls()
        assertEquals(5, rapid.inspektør.size)
        assertSendtBehov(fnr = "3")
        assertSendInfotrygdendringVedLøsning()
    }

    @Test
    fun `publiserer infotrygdendring når vi mottar løsning på behov`() {
        val hendelseId = 1234567L
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(0, rapid.inspektør.size)
        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
        assertSendInfotrygdendringVedLøsning()
    }

    @Test
    fun `mottar ikke løsning på behovet før neste puls - retry`() {
        val hendelseId = 1234567L
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(0, rapid.inspektør.size)
        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(2, rapid.inspektør.size)
    }

    @Test
    fun `mottar ikke løsning på behovet før neste puls - mottar to løsninger for samme endringsmelding`() {
        val hendelseId = 1234567L
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(0, rapid.inspektør.size)
        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(1, rapid.inspektør.size)
        assertSendtBehov()
        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(2, rapid.inspektør.size)
        assertSendInfotrygdendringVedLøsning()
        val sizeFør = rapid.inspektør.size
        sendLøsningPåBehov()
        val sizeEtter = rapid.inspektør.size
        assertEquals(sizeFør, sizeEtter)
    }

    private fun puls() {
        rapid.sendTestMessage("""{"@event_name": "ping"}""")
    }

    private fun assertSendInfotrygdendringVedLøsning(fnr: String = fødselsnummer, aktørId: String = aktør) {
        sendLøsningPåBehov(fnr, aktørId)
        assertSendtInfotrygdendring(rapid.sisteMelding(), fnr, aktørId)
    }

    private fun assertSendtInfotrygdendring(utgående: JsonNode, fnr: String, aktørId: String) {
        Assertions.assertTrue(utgående.contains("@id"))
        assertEquals("infotrygdendring", utgående.path("@event_name").asText())
        assertEquals(fnr, utgående.path("fødselsnummer").asText())
        assertEquals(aktørId, utgående.path("aktørId").asText())
    }

    private fun sendLøsningPåBehov(fnr: String = fødselsnummer, aktørId: String = aktør) {
        rapid.sendTestMessage(rapid.sisteBehov().medLøsning(fnr, aktørId).toString())
    }

    fun TestRapid.sisteMelding() = inspektør.message(inspektør.size -1)
    fun TestRapid.sisteBehov(): JsonNode {
        val index = (0 until inspektør.size).last { inspektør.message(it).path("@event_name").asText() == "behov" }
        return rapid.inspektør.message(index)
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

    private fun assertSendtBehov(fnr: String = fødselsnummer) {
        assertEquals("behov", rapid.sisteMelding().path("@event_name").asText())
        assertEquals("HentIdenter", rapid.sisteMelding().path("@behov").single().asText())
        assertEquals(fnr, rapid.sisteMelding().path("ident").asText())
    }

    private fun setEndringsmeldingTilForfall(hendelseId: Long) {
        sessionOf(PgDb.connection()).use { session ->
            @Language("PostgreSQL")
            val sql = """
                UPDATE endringsmelding 
                SET neste_forfallstidspunkt = NOW() - INTERVAL '24 HOURS' 
                WHERE hendelse_id = ?
            """
            require(session.run(queryOf(sql, hendelseId).asUpdate) == 1)
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