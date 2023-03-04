package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.nio.charset.StandardCharsets

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant
import org.joda.time.LocalDate

object IntegrationTestFixtures {
  @BigQueryType.toTable
  final case class SimpleClass(
      stringField: String,
      intField: Int
  )

  val simpleClassBigQueryType = BigQueryType[SimpleClass]
  val simpleClassBigQuerySchema = simpleClassBigQueryType.schema

  val simpleObject1 = SimpleClass(
    stringField = "simple 1",
    intField = 1
  )

  val simpleObject2 = SimpleClass(
    stringField = "simple 2",
    intField = 2
  )

  @BigQueryType.toTable
  final case class ComplexClass(
      stringField: String,
      optionalStringField: Option[String],
      intField: Int,
      bigDecimalField: BigDecimal,
      instantField: Instant,
      localDateField: LocalDate
  )

  val complexClassBigQueryType = BigQueryType[ComplexClass]
  val complexClassBigQuerySchema = complexClassBigQueryType.schema

  val complexObject1 = ComplexClass(
    stringField = "complex 1",
    optionalStringField = Some("complex 1"),
    intField = 1,
    bigDecimalField = BigDecimal(1),
    instantField = Instant.parse("2014-09-10T12:03:01Z"),
    localDateField = LocalDate.parse("2014-09-10")
  )

  val complexObject2 = ComplexClass(
    stringField = "complex 2",
    optionalStringField = None,
    intField = 2,
    bigDecimalField = BigDecimal(2),
    instantField = Instant.parse("2014-09-10T12:03:02Z"),
    localDateField = LocalDate.parse("2014-09-10")
  )

  val invalidJson = "invalid json".getBytes(StandardCharsets.UTF_8)
}
