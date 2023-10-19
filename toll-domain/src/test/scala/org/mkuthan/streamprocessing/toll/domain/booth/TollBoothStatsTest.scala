package org.mkuthan.streamprocessing.toll.domain.booth

import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class TollBoothStatsTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothEntryFixture
    with TollBoothStatsFixture {

  import TollBoothStats._

  private val FiveMinutes = Duration.standardMinutes(5)

  private val DefaultWindowOptions = WindowOptions()

  behavior of "TollBoothStats"

  it should "calculate statistics in fixed window" in runWithScioContext { sc =>
    val tollBoothId1 = TollBoothId("1")
    val tollBoothId2 = TollBoothId("2")

    val entry1 = anyTollBoothEntry.copy(
      id = tollBoothId1,
      entryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
      toll = BigDecimal(2)
    )

    val entry2 = anyTollBoothEntry.copy(
      id = tollBoothId1,
      entryTime = Instant.parse("2014-09-10T12:01:30.000Z"),
      toll = BigDecimal(1)
    )

    val entry3 = anyTollBoothEntry.copy(
      id = tollBoothId2,
      entryTime = Instant.parse("2014-09-10T12:04:00.000Z"),
      toll = BigDecimal(4)
    )

    val inputs = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(entry1.entryTime, entry1)
      .addElementsAtTime(entry2.entryTime, entry2)
      .addElementsAtTime(entry3.entryTime, entry3)
      .advanceWatermarkToInfinity()

    val results = calculateInFixedWindow(sc.testBounded(inputs), FiveMinutes, DefaultWindowOptions)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containElementsAtTime(
        "2014-09-10T12:04:59.999Z",
        anyTollBoothStats.copy(
          id = tollBoothId1,
          count = 2,
          totalToll = BigDecimal(2 + 1),
          firstEntryTime = entry1.entryTime,
          lastEntryTime = entry2.entryTime
        ),
        anyTollBoothStats.copy(
          id = tollBoothId2,
          count = 1,
          totalToll = BigDecimal(4),
          firstEntryTime = entry3.entryTime,
          lastEntryTime = entry3.entryTime
        )
      )
    }
  }

  it should "calculate statistics in fixed window for late entries" in runWithScioContext { sc =>
    val onTimeEntry1 = anyTollBoothEntry.copy(
      entryTime = Instant.parse("2014-09-10T12:01:00Z"),
      toll = BigDecimal(2)
    )

    val onTimeEntry2 = anyTollBoothEntry.copy(
      entryTime = Instant.parse("2014-09-10T12:02:00Z"),
      toll = BigDecimal(3)
    )

    val lateEntry = anyTollBoothEntry.copy(
      entryTime = Instant.parse("2014-09-10T12:03:00Z"),
      toll = BigDecimal(1)
    )

    val inputs = unboundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(onTimeEntry1.entryTime, onTimeEntry1)
      .addElementsAtTime(onTimeEntry2.entryTime, onTimeEntry2)
      .advanceWatermarkTo("2014-09-10T12:05:00Z")
      .addElementsAtTime(lateEntry.entryTime, lateEntry)
      .advanceWatermarkToInfinity()

    val windowOptions = WindowOptions(
      allowedLateness = Duration.standardMinutes(2),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    )

    val results = calculateInFixedWindow(sc.testUnbounded(inputs), FiveMinutes, windowOptions)

    val windowStart = "2014-09-10T12:00:00Z"
    val windowEnd = "2014-09-10T12:05:00Z"
    val recordTimestamp = Instant.parse("2014-09-10T12:04:59.999Z")

    results.withTimestamp should inOnTimePane(windowStart, windowEnd) {
      containElementsAtTime(
        recordTimestamp,
        anyTollBoothStats.copy(
          count = 2,
          totalToll = BigDecimal(2 + 3),
          firstEntryTime = onTimeEntry1.entryTime,
          lastEntryTime = onTimeEntry2.entryTime
        )
      )
    }

    results.withTimestamp should inLatePane(windowStart, windowEnd) {
      containElementsAtTime(
        recordTimestamp,
        anyTollBoothStats.copy(
          count = 1,
          totalToll = BigDecimal(1),
          firstEntryTime = lateEntry.entryTime,
          lastEntryTime = lateEntry.entryTime
        )
      )
    }
  }

  it should "encode into record" in runWithScioContext { sc =>
    val recordTimestamp = Instant.parse("2014-09-10T12:04:59.999Z")
    val inputs = boundedTestCollectionOf[TollBoothStats]
      .addElementsAtTime(recordTimestamp, anyTollBoothStats)
      .advanceWatermarkToInfinity()

    val results = encodeRecord(sc.testBounded(inputs))
    results should containElements(anyTollBoothStatsRecord.copy(created_at = recordTimestamp))
  }
}
