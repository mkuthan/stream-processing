package org.mkuthan.streamprocessing.infrastructure

import java.nio.charset.StandardCharsets

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant
import org.joda.time.LocalDate
import org.scalacheck._

import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.test.common.JodaTimeArbitrary

trait IntegrationTestFixtures extends JodaTimeArbitrary {
  import IntegrationTestFixtures._

  implicit val sampleClassArbitrary: Arbitrary[SampleClass] = Arbitrary[SampleClass] {
    for {
      string <- Gen.alphaNumStr
      optionString <- Gen.option(Gen.alphaNumStr)
      int <- Arbitrary.arbitrary[Int]
      bigDecimal <- Arbitrary.arbitrary[BigDecimal]
      instant <- Arbitrary.arbitrary[Instant]
      localDate <- Arbitrary.arbitrary[LocalDate]
    } yield SampleClass(
      string,
      optionString,
      int,
      bigDecimal,
      instant,
      localDate
    )
  }

  val SampleClassBigQueryType: BigQueryType[SampleClass] = BigQueryType[SampleClass]
  val SampleClassBigQuerySchema = SampleClassBigQueryType.schema

  val SampleObject1: SampleClass = SampleClass(
    stringField = "complex 1",
    optionalStringField = Some("complex 1"),
    intField = 1,
    bigDecimalField = BigDecimal(1),
    instantField = Instant.parse("2014-09-10T12:03:01Z"),
    localDateField = LocalDate.parse("2014-09-10")
  )

  val SampleJson1: Array[Byte] = JsonSerde.writeJsonAsBytes(SampleObject1)

  val SampleObject2: SampleClass = SampleClass(
    stringField = "complex 2",
    optionalStringField = None,
    intField = 2,
    bigDecimalField = BigDecimal(2),
    instantField = Instant.parse("2014-09-10T12:03:02Z"),
    localDateField = LocalDate.parse("2014-09-10")
  )

  val SampleJson2: Array[Byte] = JsonSerde.writeJsonAsBytes(SampleObject2)

  val InvalidJson: Array[Byte] = "invalid json".getBytes(StandardCharsets.UTF_8)

  val SampleMap1: Map[String, String] = Map("key1" -> "value1")
  val SampleMap2: Map[String, String] = Map("key2" -> "value2")
}

object IntegrationTestFixtures {
  @BigQueryType.toTable
  final case class SampleClass(
      stringField: String,
      optionalStringField: Option[String],
      intField: Int,
      bigDecimalField: BigDecimal,
      instantField: Instant,
      localDateField: LocalDate
  )
}
