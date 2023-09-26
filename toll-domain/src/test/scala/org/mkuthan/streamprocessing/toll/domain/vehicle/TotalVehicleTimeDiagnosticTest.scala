package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio._

class TotalVehicleTimeDiagnosticTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TotalVehicleTimeDiagnosticFixture {

  import TotalVehicleTimeDiagnostic._

  private val FiveMinutes = Duration.standardMinutes(5)
  private val DefaultWindowOptions = WindowOptions()

  behavior of "TotalVehicleTimeDiagnostic"

  it should "aggregate and encode TotalVehicleTimeDiagnostic into record" in runWithScioContext { sc =>
    val diagnostic1 = anyDiagnostic.copy(reason = "1", count = 1)
    val diagnostic2 = anyDiagnostic.copy(reason = "2", count = 2)

    val input = boundedTestCollectionOf[TotalVehicleTimeDiagnostic]
      .addElementsAtTime("2014-09-10T12:00:00Z", diagnostic1)
      .addElementsAtTime("2014-09-10T12:01:00Z", diagnostic2)
      .addElementsAtTime("2014-09-10T12:02:00Z", diagnostic1)
      .addElementsAtTime("2014-09-10T12:03:00Z", diagnostic2)
      .advanceWatermarkToInfinity()

    val results =
      aggregateAndEncode(sc.testBounded(input), FiveMinutes, DefaultWindowOptions)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containInAnyOrderAtTime(
        "2014-09-10T12:04:59.999Z",
        Seq(
          anyDiagnosticRecord.copy(
            created_at = Instant.parse("2014-09-10T12:04:59.999Z"),
            reason = "1",
            count = 2
          ),
          anyDiagnosticRecord.copy(
            created_at = Instant.parse("2014-09-10T12:04:59.999Z"),
            reason = "2",
            count = 4
          )
        )
      )
    }
  }
}
