package org.mkuthan.streamprocessing.infrastructure

import java.util.UUID

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime

import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde

trait IntegrationTestFixtures {
  import IntegrationTestFixtures._

  val SampleClassBigQueryType: BigQueryType[SampleClass] = BigQueryType[SampleClass]
  val SampleClassBigQuerySchema = SampleClassBigQueryType.schema

  val SampleObject1: SampleClass = SampleClass(
    id = UUID.randomUUID().toString,
    stringField = "complex 1",
    optionalStringField = Some("complex 1"),
    intField = 1,
    longField = 1L,
    floatField = 1.0f,
    doubleField = 1.0,
    bigDecimalField = BigDecimal(1),
    instantField = Instant.parse("2014-09-10T12:03:01Z"),
    localDateTimeField = LocalDateTime.parse("2014-09-10T12:03:01"),
    localDateField = LocalDate.parse("2014-09-10"),
    localTimeField = LocalTime.parse("12:03:01")
  )

  val SampleJson1: Array[Byte] = JsonSerde.writeJsonAsBytes(SampleObject1)

  val SampleObject2: SampleClass = SampleClass(
    id = UUID.randomUUID().toString,
    stringField = "complex 2",
    optionalStringField = None,
    intField = 2,
    longField = 2L,
    floatField = 2.0f,
    doubleField = 2.0,
    bigDecimalField = BigDecimal(2),
    instantField = Instant.parse("2014-09-10T12:03:02Z"),
    localDateTimeField = LocalDateTime.parse("2014-09-10T12:03:02"),
    localDateField = LocalDate.parse("2014-09-10"),
    localTimeField = LocalTime.parse("12:03:02")
  )

  val SampleJson2: Array[Byte] = JsonSerde.writeJsonAsBytes(SampleObject2)

  val SampleMap1: Map[String, String] = Map("key1" -> "value1")
  val SampleMap2: Map[String, String] = Map("key2" -> "value2")
}

object IntegrationTestFixtures {
  @BigQueryType.toTable
  final case class SampleClass(
      id: String,
      stringField: String,
      optionalStringField: Option[String],
      intField: Int,
      longField: Long,
      floatField: Float,
      doubleField: Double,
      bigDecimalField: BigDecimal,
      instantField: Instant,
      localDateTimeField: LocalDateTime,
      localDateField: LocalDate,
      localTimeField: LocalTime
  )
}
