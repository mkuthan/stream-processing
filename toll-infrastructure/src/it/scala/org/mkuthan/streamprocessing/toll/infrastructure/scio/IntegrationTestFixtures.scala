package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant

object IntegrationTestFixtures {
  @BigQueryType.toTable
  final case class SimpleClass(
      stringField: String,
      intField: Int
  )

  val simpleClassBigQueryType = BigQueryType[SimpleClass]
  val simpleClassBigQuerySchema = simpleClassBigQueryType.schema

  val simpleObject1 = SimpleClass("simple 1", 1)
  val simpleObject2 = SimpleClass("simple 2", 2)

  @BigQueryType.toTable
  final case class ComplexClass(
      stringField: String,
      optionalStringField: Option[String],
      intField: Int,
      bigDecimalField: BigDecimal,
      instantField: Instant
  )

  val complexClassBigQueryType = BigQueryType[ComplexClass]
  val complexClassBigQuerySchema = complexClassBigQueryType.schema

  val complexObject1 = ComplexClass(
    "complex 1",
    Some("complex 1"),
    1,
    BigDecimal(1),
    Instant.parse("2014-09-10T12:03:01Z")
  )

  val complexObject2 = ComplexClass(
    "complex 2",
    None,
    2,
    BigDecimal(2),
    Instant.parse("2014-09-10T12:03:02Z")
  )
}
