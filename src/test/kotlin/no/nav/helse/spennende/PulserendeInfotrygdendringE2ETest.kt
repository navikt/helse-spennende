package no.nav.helse.spennende

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.contains
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.result_object.ok
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.github.navikt.tbd_libs.test_support.TestDataSource
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals


internal class PulserendeInfotrygdendringE2ETest {

    private val rapid = TestRapid()

    private companion object {
        private const val fødselsnummer = "12345678911"
        private const val aktør = "aktør"
    }

    private lateinit var dataSource: TestDataSource
    private val speedClient = mockk<SpeedClient>()
    private lateinit var endringsmeldingProducer: TestEndringsmeldingProducer
    private val sendteMeldinger get() = endringsmeldingProducer.sendteMeldinger

    @BeforeEach
    fun setup() {
        rapid.reset()
        endringsmeldingProducer = TestEndringsmeldingProducer()
        dataSource = databaseContainer.nyTilkobling()
        val repository = PostgresRepository { dataSource.ds }
        val infotrygdendringutsender = Infotrygdendringutsender(endringsmeldingProducer)
        InfotrygdhendelseRiver(rapid, repository, speedClient, infotrygdendringutsender)
        Puls(rapid, repository, infotrygdendringutsender)
    }

    private class TestEndringsmeldingProducer : InfotrygdendringProducer {
        val sendteMeldinger = mutableListOf<Pair<String, String>>()

        override fun sendEndringsmelding(fnr: String, melding: String) {
            sendteMeldinger.add(fnr to melding)
        }

        override fun tømKø() {}
    }

    @AfterEach
    fun resetSchema() {
        databaseContainer.droppTilkobling(dataSource)
        clearAllMocks()
    }

    @Test
    fun `publiserer endring først etter fem minutter`() {
        val hendelseId = 1234567L
        every { speedClient.hentFødselsnummerOgAktørId(ident = fødselsnummer, any()) } returns IdentResponse(
            fødselsnummer = fødselsnummer,
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        ).ok()
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(1, sendteMeldinger.size)

        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(2, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning()
    }

    @Test
    fun `flere infotrygdendringer på samme person`() {
        val hendelseId1 = 1234567L
        val hendelseId2 = 2234567L
        val hendelseId3 = 3234567L
        every { speedClient.hentFødselsnummerOgAktørId(ident = fødselsnummer, any()) } returns IdentResponse(
            fødselsnummer = fødselsnummer,
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        ).ok()
        rapid.sendTestMessage(createTestMessage(hendelseId1))
        assertEquals(1, sendteMeldinger.size)
        rapid.sendTestMessage(createTestMessage(hendelseId2))
        assertEquals(1, sendteMeldinger.size)
        rapid.sendTestMessage(createTestMessage(hendelseId3))
        puls()
        assertEquals(1, sendteMeldinger.size)
        setEndringsmeldingTilForfall(hendelseId1)
        setEndringsmeldingTilForfall(hendelseId2)
        puls()
        assertEquals(1, sendteMeldinger.size)
        setEndringsmeldingTilForfall(hendelseId3)
        puls()
        assertEquals(2, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning()
    }

    @Test
    fun `flere infotrygdendringer på ulike personer`() {
        val hendelseId1 = 1234567L
        val hendelseId2 = 2234567L
        val hendelseId3 = 3234567L

        every { speedClient.hentFødselsnummerOgAktørId(ident = "1", any()) } returns IdentResponse(
            fødselsnummer = "1",
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        ).ok()
        every { speedClient.hentFødselsnummerOgAktørId(ident = "2", any()) } returns IdentResponse(
            fødselsnummer = "2",
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        ).ok()
        every { speedClient.hentFødselsnummerOgAktørId(ident = "3", any()) } returns IdentResponse(
            fødselsnummer = "3",
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        ).ok()

        rapid.sendTestMessage(createTestMessage(hendelseId1, fnr = "1"))
        assertEquals(1, sendteMeldinger.size)
        rapid.sendTestMessage(createTestMessage(hendelseId2, fnr = "2"))
        assertEquals(2, sendteMeldinger.size)
        rapid.sendTestMessage(createTestMessage(hendelseId3, fnr = "3"))
        assertEquals(3, sendteMeldinger.size)
        puls()
        assertEquals(3, sendteMeldinger.size)

        setEndringsmeldingTilForfall(hendelseId1)
        puls()
        assertEquals(4, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning(fnr = "1")

        setEndringsmeldingTilForfall(hendelseId2)
        puls()
        assertEquals(5, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning(fnr = "2")

        setEndringsmeldingTilForfall(hendelseId3)
        puls()
        assertEquals(6, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning(fnr = "3")
    }

    private fun puls() {
        rapid.sendTestMessage("""{"@event_name": "minutt"}""")
    }

    private fun assertSendInfotrygdendringVedLøsning(fnr: String = fødselsnummer, aktørId: String = aktør) {
        val sisteMelding = jacksonObjectMapper().readTree(sendteMeldinger.last().second)
        assertSendtInfotrygdendring(sisteMelding, fnr, aktørId)
    }

    private fun assertSendtInfotrygdendring(utgående: JsonNode, fnr: String, aktørId: String) {
        Assertions.assertTrue(utgående.contains("@id"))
        assertEquals("infotrygdendring", utgående.path("@event_name").asText())
        assertEquals(fnr, utgående.path("fødselsnummer").asText())
    }

    private fun setEndringsmeldingTilForfall(hendelseId: Long) {
        sessionOf(dataSource.ds).use { session ->
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
