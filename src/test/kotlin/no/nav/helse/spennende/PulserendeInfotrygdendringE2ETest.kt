package no.nav.helse.spennende

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.contains
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.kafka.ConsumerProducerFactory
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.github.navikt.tbd_libs.test_support.TestDataSource
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import kotliquery.queryOf
import kotliquery.sessionOf
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit


internal class PulserendeInfotrygdendringE2ETest {

    private val rapid = TestRapid()

    private companion object {
        private const val fødselsnummer = "12345678911"
        private const val aktør = "aktør"
    }

    private lateinit var dataSource: TestDataSource
    private val speedClient = mockk<SpeedClient>()
    private val sendteMeldinger = mutableListOf<ProducerRecord<String, String>>()
    @BeforeEach
    fun setup() {
        rapid.reset()
        sendteMeldinger.clear()
        dataSource = databaseContainer.nyTilkobling()
        val repository = PostgresRepository { dataSource.ds }
        InfotrygdhendelseRiver(rapid, repository, speedClient)

        val fabrikk = mockk<ConsumerProducerFactory>()
        every { fabrikk.createProducer(any(), any()) } returns lagMockProducer()
        Puls(rapid, repository, "tulletopic", fabrikk)
    }

    private fun lagMockProducer(): KafkaProducer<String, String> {
        val mockProducer = mockk<KafkaProducer<String, String>>()

        every { mockProducer.flush() } just Runs
        every { mockProducer.close() } just Runs
        every { mockProducer.send(capture(sendteMeldinger)) } returns object : Future<RecordMetadata> {
            override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
                TODO("Not yet implemented")
            }

            override fun isCancelled(): Boolean {
                TODO("Not yet implemented")
            }

            override fun isDone(): Boolean {
                TODO("Not yet implemented")
            }

            override fun get(): RecordMetadata? {
                TODO("Not yet implemented")
            }

            override fun get(timeout: Long, unit: TimeUnit): RecordMetadata? {
                TODO("Not yet implemented")
            }
        }
        return mockProducer
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
        )
        rapid.sendTestMessage(createTestMessage(hendelseId))
        puls()
        assertEquals(0, sendteMeldinger.size)

        setEndringsmeldingTilForfall(hendelseId)
        puls()
        assertEquals(1, sendteMeldinger.size)
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
        )
        rapid.sendTestMessage(createTestMessage(hendelseId1))
        rapid.sendTestMessage(createTestMessage(hendelseId2))
        rapid.sendTestMessage(createTestMessage(hendelseId3))
        puls()
        assertEquals(0, sendteMeldinger.size)
        setEndringsmeldingTilForfall(hendelseId1)
        setEndringsmeldingTilForfall(hendelseId2)
        puls()
        assertEquals(0, sendteMeldinger.size)
        setEndringsmeldingTilForfall(hendelseId3)
        puls()
        assertEquals(1, sendteMeldinger.size)
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
        )
        every { speedClient.hentFødselsnummerOgAktørId(ident = "2", any()) } returns IdentResponse(
            fødselsnummer = "2",
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        )
        every { speedClient.hentFødselsnummerOgAktørId(ident = "3", any()) } returns IdentResponse(
            fødselsnummer = "3",
            aktørId = aktør,
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        )

        rapid.sendTestMessage(createTestMessage(hendelseId1, fnr = "1"))
        rapid.sendTestMessage(createTestMessage(hendelseId2, fnr = "2"))
        rapid.sendTestMessage(createTestMessage(hendelseId3, fnr = "3"))
        puls()
        assertEquals(0, sendteMeldinger.size)

        setEndringsmeldingTilForfall(hendelseId1)
        puls()
        assertEquals(1, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning(fnr = "1")

        setEndringsmeldingTilForfall(hendelseId2)
        puls()
        assertEquals(2, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning(fnr = "2")

        setEndringsmeldingTilForfall(hendelseId3)
        puls()
        assertEquals(3, sendteMeldinger.size)
        assertSendInfotrygdendringVedLøsning(fnr = "3")
    }

    private fun puls() {
        rapid.sendTestMessage("""{"@event_name": "minutt"}""")
    }

    private fun assertSendInfotrygdendringVedLøsning(fnr: String = fødselsnummer, aktørId: String = aktør) {
        val sisteMelding = jacksonObjectMapper().readTree(sendteMeldinger.last().value())
        assertSendtInfotrygdendring(sisteMelding, fnr, aktørId)
    }

    private fun assertSendtInfotrygdendring(utgående: JsonNode, fnr: String, aktørId: String) {
        Assertions.assertTrue(utgående.contains("@id"))
        assertEquals("infotrygdendring", utgående.path("@event_name").asText())
        assertEquals(fnr, utgående.path("fødselsnummer").asText())
        assertEquals(aktørId, utgående.path("aktørId").asText())
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