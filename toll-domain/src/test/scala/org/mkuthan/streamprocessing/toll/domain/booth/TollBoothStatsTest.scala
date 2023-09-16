package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio._

class TollBoothStatsTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothEntryFixture
    with TollBoothStatsFixture {

  import TollBoothStats._

  private val FiveMinutes = Duration.standardMinutes(5)

  behavior of "TollBoothStats"

  it should "calculate statistics in fixed window" in runWithScioContext { sc =>
    val tollBoothId1 = TollBoothId("1")
    val tollBoothId2 = TollBoothId("2")

    val tollBoothEntry1Time = Instant.parse("2014-09-10T12:01:00.000Z")
    val tollBoothEntry1 = anyTollBoothEntry.copy(
      id = tollBoothId1,
      entryTime = tollBoothEntry1Time,
      toll = BigDecimal(2)
    )

    val tollBoothEntry2Time = Instant.parse("2014-09-10T12:01:30.000Z")
    val tollBoothEntry2 = anyTollBoothEntry.copy(
      id = tollBoothId1,
      entryTime = tollBoothEntry2Time,
      toll = BigDecimal(1)
    )

    val tollBoothEntry3Time = Instant.parse("2014-09-10T12:04:00.000Z")
    val tollBoothEntry3 = anyTollBoothEntry.copy(
      id = tollBoothId2,
      entryTime = tollBoothEntry3Time,
      toll = BigDecimal(4)
    )

    val inputs = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(tollBoothEntry1.entryTime, tollBoothEntry1)
      .addElementsAtTime(tollBoothEntry2.entryTime, tollBoothEntry2)
      .addElementsAtTime(tollBoothEntry3.entryTime, tollBoothEntry3)
      .build()

    val results = calculateInFixedWindow(sc.testBounded(inputs), FiveMinutes)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containInAnyOrderAtTime(
        "2014-09-10T12:04:59.999Z",
        Seq(
          anyTollBoothStats.copy(
            id = tollBoothId1,
            count = 2,
            totalToll = BigDecimal(2 + 1),
            firstEntryTime = tollBoothEntry1Time,
            lastEntryTime = tollBoothEntry2Time
          ),
          anyTollBoothStats.copy(
            id = tollBoothId2,
            count = 1,
            totalToll = BigDecimal(4),
            firstEntryTime = tollBoothEntry3Time,
            lastEntryTime = tollBoothEntry3Time
          )
        )
      )
    }
  }

  it should "encode into record" in runWithScioContext { sc =>
    val recordTimestamp = Instant.parse("2014-09-10T12:04:59.999Z")
    val inputs = unboundedTestCollectionOf[TollBoothStats]
      .addElementsAtTime(recordTimestamp, anyTollBoothStats)
      .advanceWatermarkToInfinity()

    val results = encode(sc.testUnbounded(inputs))
    results should containSingleValue(anyTollBoothStatsRecord.copy(record_timestamp = recordTimestamp))
  }
}
