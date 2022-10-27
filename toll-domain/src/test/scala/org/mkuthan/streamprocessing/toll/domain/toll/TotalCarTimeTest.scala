package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.test._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

class TotalCarTimeTest extends PipelineSpec
    with TimestampedMatchers
    with TollBoothEntryFixture
    with TollBoothExitFixture {

  import TotalCarTime._

  private val FiveMinutes = Duration.standardMinutes(5)

  "TotalCarTime" should "be calculated" in runWithContext { sc =>
    val tollBoothId = TollBoothId("1")
    val licensePlate = LicensePlate("AB 123")
    val entryTime = Instant.parse("2014-09-10T12:03:01Z")
    val exitTime = Instant.parse("2014-09-10T12:04:03Z")

    val tollBoothEntry = anyTollBoothEntry.copy(id = tollBoothId, licensePlate = licensePlate, entryTime = entryTime)
    val tollBoothExit = anyTollBoothExit.copy(id = tollBoothId, licensePlate = licensePlate, exitTime = exitTime)

    val boothEntries = testStreamOf[TollBoothEntry]
      .addElementsAtTime("2014-09-10T12:03:01Z", tollBoothEntry)
      .advanceWatermarkToInfinity()

    val boothExits = testStreamOf[TollBoothExit]
      .addElementsAtTime("2014-09-10T12:04:03Z", tollBoothExit)
      .advanceWatermarkToInfinity()

    val (results, diagnostic) = calculate(sc.testStream(boothEntries), sc.testStream(boothExits), FiveMinutes)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        TotalCarTime(
          tollBoothId = tollBoothId,
          licencePlate = licensePlate,
          entryTime = entryTime,
          exitTime = exitTime,
          duration = Duration.standardSeconds(62)
        )
      )
    }

    diagnostic should beEmpty
  }
}
