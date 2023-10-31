package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import java.util.UUID

import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass

trait BigQueryTypesArbitrary {
  private final val BigQueryMinTimestamp = Instant.parse("0001-01-01T00:00:00Z")
  private final val BigQueryMaxTimestamp = Instant.parse("9999-12-31T23:59:59.999999Z")
  private final val BigQueryNumericPrecision = 38
  private final val BigQueryNumericScale = 9
  private final val BigQueryMaxNumeric = BigInt(10).pow(BigQueryNumericPrecision) - 1
  private final val BigQueryMinNumeric = -BigQueryMaxNumeric

  private final val sampleClassArbitrary = Arbitrary[SampleClass] {
    for {
      string <- Gen.alphaNumStr
      optionString <- Gen.option(Gen.alphaNumStr)
      int <- Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE)
      long <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      float <- Gen.chooseNum(Float.MinValue, Float.MaxValue)
      double <- Gen.chooseNum(Double.MinValue, Double.MaxValue)
      bigDecimal <- Gen.chooseNum(BigQueryMinNumeric, BigQueryMaxNumeric).map(BigDecimal(_, BigQueryNumericScale))
      instant <- Gen
        .chooseNum(BigQueryMinTimestamp.getMillis, BigQueryMaxTimestamp.getMillis)
        .map(new Instant(_))
      localDateTime <- Gen
        .chooseNum(BigQueryMinTimestamp.getMillis, BigQueryMaxTimestamp.getMillis)
        .map(new LocalDateTime(_, DateTimeZone.UTC))
      localDate <- Gen
        .chooseNum(BigQueryMinTimestamp.getMillis, BigQueryMaxTimestamp.getMillis)
        .map(new LocalDate(_, DateTimeZone.UTC))
      localTime <- Gen
        .chooseNum(0, 24 * 3600 * 1000)
        .map(LocalTime.fromMillisOfDay(_))
    } yield SampleClass(
      id = UUID.randomUUID().toString,
      stringField = string,
      optionalStringField = optionString,
      intField = int,
      longField = long,
      floatField = float,
      doubleField = double,
      bigDecimalField = bigDecimal,
      instantField = instant,
      localDateTimeField = localDateTime,
      localDateField = localDate,
      localTimeField = localTime
    )
  }

  def sampleObjects(n: Int = 100): Seq[SampleClass] =
    Gen.listOfN(n, sampleClassArbitrary.arbitrary).sample.get

}
