package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

class VehiclesWithExpiredRegistrationDiagnosticTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with VehiclesWithExpiredRegistrationDiagnosticFixture {

  import VehiclesWithExpiredRegistrationDiagnostic._

  private val FiveMinutes = Duration.standardMinutes(5)
  private val DefaultWindowOptions = WindowOptions()

  behavior of "VehiclesWithExpiredRegistrationDiagnostic"

  it should "aggregate and encode into record" in runWithScioContext { sc =>
    val tollBooth1 = TollBoothId("1")
    val tollBooth2 = TollBoothId("2")
    val reason1 = "reason 1"
    val reason2 = "reason 2"

    val diagnostic1 = anyVehiclesWithExpiredRegistrationDiagnostic
      .copy(tollBoothId = tollBooth1, reason = reason1, count = 1)
    val diagnostic2 = anyVehiclesWithExpiredRegistrationDiagnostic
      .copy(tollBoothId = tollBooth2, reason = reason2, count = 2)

    val input = boundedTestCollectionOf[VehiclesWithExpiredRegistrationDiagnostic]
      .addElementsAtTime("2014-09-10T12:00:00Z", diagnostic1, diagnostic2)
      .addElementsAtTime("2014-09-10T12:01:00Z", diagnostic1, diagnostic2)
      .advanceWatermarkToInfinity()

    val results =
      aggregateAndEncodeRecord(sc.testBounded(input), FiveMinutes, DefaultWindowOptions)

    val endOfWindow = "2014-09-10T12:04:59.999Z"
    val diagnosticRecord = anyVehiclesWithExpiredRegistrationDiagnosticRecord
      .copy(created_at = Instant.parse(endOfWindow))

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containInAnyOrderAtTime(
        endOfWindow,
        Seq(
          diagnosticRecord.copy(toll_booth_id = tollBooth1.id, reason = reason1, count = 1 + 1),
          diagnosticRecord.copy(toll_booth_id = tollBooth2.id, reason = reason2, count = 2 + 2)
        )
      )
    }
  }
}
