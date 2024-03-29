package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnosticFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

class TotalVehicleTimesTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothDiagnosticFixture
    with TollBoothEntryFixture
    with TollBoothExitFixture
    with TotalVehicleTimesFixture {

  import TotalVehicleTimes._

  private val FiveMinutes = Duration.standardMinutes(5)
  private val DefaultWindowOptions = WindowOptions()

  behavior of "TotalVehicleTimes"

  it should "calculate TotalVehicleTimes using session window" in runWithScioContext { sc =>
    val tollBoothId = TollBoothId("1")
    val licensePlate = LicensePlate("AB 123")
    val entryTime = Instant.parse("2014-09-10T12:03:01Z")
    val exitTime = Instant.parse("2014-09-10T12:04:03Z")

    val tollBoothEntry = anyTollBoothEntry.copy(id = tollBoothId, licensePlate = licensePlate, entryTime = entryTime)
    val tollBoothExit = anyTollBoothExit.copy(id = tollBoothId, licensePlate = licensePlate, exitTime = exitTime)

    val boothEntries = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(tollBoothEntry.entryTime, tollBoothEntry)
      .advanceWatermarkToInfinity()

    val boothExits = boundedTestCollectionOf[TollBoothExit]
      .addElementsAtTime(tollBoothExit.exitTime, tollBoothExit)
      .advanceWatermarkToInfinity()

    val (results, diagnostic) =
      calculateInSessionWindow(
        sc.testBounded(boothEntries),
        sc.testBounded(boothExits),
        FiveMinutes,
        DefaultWindowOptions
      )

    results.withTimestamp should inOnTimePane("2014-09-10T12:03:01Z", "2014-09-10T12:09:03Z") {
      containElementsAtTime(
        "2014-09-10T12:09:02.999Z",
        anyTotalVehicleTimes.copy(
          tollBoothId = tollBoothId,
          licensePlate = licensePlate,
          entryTime = entryTime,
          exitTime = exitTime,
          duration = Duration.standardSeconds(62)
        )
      )
    }

    diagnostic should beEmpty
  }

  it should "emit diagnostic if TollBoothExit is after session window gap" in runWithScioContext { sc =>
    val tollBoothId = TollBoothId("1")
    val licensePlate = LicensePlate("AB 123")
    val entryTime = Instant.parse("2014-09-10T12:03:01Z")
    val exitTime = Instant.parse("2014-09-10T12:08:03Z")

    val tollBoothEntry = anyTollBoothEntry.copy(id = tollBoothId, licensePlate = licensePlate, entryTime = entryTime)
    val tollBoothExit = anyTollBoothExit.copy(id = tollBoothId, licensePlate = licensePlate, exitTime = exitTime)

    val boothEntries = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(tollBoothEntry.entryTime, tollBoothEntry)
      .advanceWatermarkToInfinity()

    val boothExits = boundedTestCollectionOf[TollBoothExit]
      .addElementsAtTime(tollBoothExit.exitTime, tollBoothExit)
      .advanceWatermarkToInfinity()

    val (results, diagnostic) =
      calculateInSessionWindow(
        sc.testBounded(boothEntries),
        sc.testBounded(boothExits),
        FiveMinutes,
        DefaultWindowOptions
      )

    results should beEmpty

    diagnostic.withTimestamp should inOnTimePane("2014-09-10T12:03:01Z", "2014-09-10T12:08:01Z") {
      containElementsAtTime(
        "2014-09-10T12:08:00.999Z",
        anyTollBoothDiagnostic.copy(reason = TollBoothDiagnostic.MissingTollBoothExit)
      )
    }
  }

  it should "encode into record" in runWithScioContext { sc =>
    val createdAt = Instant.parse("2014-09-10T12:08:00.999Z")
    val inputs = boundedTestCollectionOf[TotalVehicleTimes]
      .addElementsAtTime(createdAt, anyTotalVehicleTimes)
      .advanceWatermarkToInfinity()

    val results = encodeRecord(sc.testBounded(inputs))
    results should containElements(anyTotalVehicleTimesRecord.copy(created_at = createdAt))
  }
}
