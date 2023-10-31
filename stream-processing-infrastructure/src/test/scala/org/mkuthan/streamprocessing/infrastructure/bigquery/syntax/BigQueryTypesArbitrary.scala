package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import java.util.UUID

import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass

trait BigQueryTypesArbitrary {
  private val BigQueryMinTimestamp = Instant.parse("0001-01-01T00:00:00Z")
  private val BigQueryMaxTimestamp = Instant.parse("9999-12-31T23:59:59.999999Z")
  private val BigQueryNumericPrecision = 38
  private val BigQueryNumericScale = 9
  private val BigQueryMaxNumeric = BigInt(10).pow(BigQueryNumericPrecision) - 1
  private val BigQueryMinNumeric = -BigQueryMaxNumeric

  private val sampleClassArbitrary = Arbitrary[SampleClass] {
    for {
      string <- Gen.alphaNumStr
      optionString <- Gen.option(Gen.alphaNumStr)
      int <- Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE)
      bigDecimal <- Gen.chooseNum(BigQueryMinNumeric, BigQueryMaxNumeric).map(BigDecimal(_, BigQueryNumericScale))
      instant <- Gen
        .chooseNum(BigQueryMinTimestamp.getMillis, BigQueryMaxTimestamp.getMillis)
        .map(new Instant(_))
      localDate <- Gen
        .chooseNum(BigQueryMinTimestamp.getMillis, BigQueryMaxTimestamp.getMillis)
        .map(new LocalDate(_, DateTimeZone.UTC))
    } yield SampleClass(
      id = UUID.randomUUID().toString,
      stringField = string,
      optionalStringField = optionString,
      intField = int,
      bigDecimalField = bigDecimal,
      instantField = instant,
      localDateField = localDate
    )
  }

  def sampleObjects(n: Int = 100): Seq[SampleClass] =
    Gen.listOfN(n, sampleClassArbitrary.arbitrary).sample.get

}
