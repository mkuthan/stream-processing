package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import java.util.UUID

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffShouldMatcher._
import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.InMemorySink
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class BigQueryPropertyTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  import BigQueryPropertyTest._

  behavior of "BigQuery syntax"

  it should "write and read" in {
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val samples = sampleObjects()

        val tableId = s"$projectId:$datasetName.$tableName"

        withScioContext { sc =>
          val input = boundedTestCollectionOf[SampleClass]
            .addElementsAtMinimumTime(samples: _*)
            .advanceWatermarkToInfinity()

          sc
            .testBounded(input)
            .writeBoundedToBigQuery(
              IoIdentifier[SampleClass]("any-id"),
              BigQueryPartition.notPartitioned[SampleClass](tableId)
            )

          sc.run().waitUntilDone()
        }

        withScioContext { sc =>
          val results = sc.readFromBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryTable[SampleClass](tableId)
          )

          val resultsSink = InMemorySink(results)

          sc.run().waitUntilDone()

          eventually {
            resultsSink.toSeq shouldMatchTo samples
          }
        }
      }
    }
  }
}

object BigQueryPropertyTest {

  import com.softwaremill.diffx.ObjectMatcher
  import com.softwaremill.diffx.SeqMatcher
  import org.scalacheck.Arbitrary
  import org.scalacheck.Gen

  private final val MinTimestamp = Instant.parse("0001-01-01T00:00:00Z")
  private final val MaxTimestamp = Instant.parse("9999-12-31T23:59:59.999999Z")
  private final val NumericPrecision = 38
  private final val NumericScale = 9
  private final val MaxNumeric = BigInt(10).pow(NumericPrecision) - 1
  private final val MinNumeric = -MaxNumeric

  private final val sampleClassArbitrary = Arbitrary[SampleClass] {
    for {
      string <- Gen.alphaNumStr
      optionString <- Gen.option(Gen.alphaNumStr)
      int <- Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE)
      long <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      float <- Gen.chooseNum(Float.MinValue, Float.MaxValue)
      double <- Gen.chooseNum(Double.MinValue, Double.MaxValue)
      bigDecimal <- Gen.chooseNum(MinNumeric, MaxNumeric).map(BigDecimal(_, NumericScale))
      instant <- Gen
        .chooseNum(MinTimestamp.getMillis, MaxTimestamp.getMillis)
        .map(new Instant(_))
      localDateTime <- Gen
        .chooseNum(MinTimestamp.getMillis, MaxTimestamp.getMillis)
        .map(new LocalDateTime(_, DateTimeZone.UTC))
      localDate <- Gen
        .chooseNum(MinTimestamp.getMillis, MaxTimestamp.getMillis)
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

  implicit val sampleClassMatcher: SeqMatcher[SampleClass] = ObjectMatcher.seq[SampleClass].byValue(_.id)

  def sampleObjects(n: Int = 100): Seq[SampleClass] =
    Gen.listOfN(n, sampleClassArbitrary.arbitrary).sample.get
}
