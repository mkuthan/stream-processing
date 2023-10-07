package org.mkuthan.streamprocessing.shared.common

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio._

class DiagnosticTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with DiagnosticFixture {

  import Diagnostic._

  val FiveMinutes: Duration = Duration.standardMinutes(5)
  val DefaultWindowOptions: WindowOptions = WindowOptions()

  behavior of "Diagnostic"

  it should "aggregate and encode into record" in runWithScioContext { sc =>
    val id1 = "id1"
    val id2 = "id2"
    val reason1 = "reason1"
    val reason2 = "reason2"

    val diagnostic1 = anyDiagnostic.copy(id = id1, reason = reason1, count = 1)
    val diagnostic2 = anyDiagnostic.copy(id = id2, reason = reason2, count = 2)

    val input = boundedTestCollectionOf[Diagnostic]
      .addElementsAtTime("2014-09-10T12:00:00Z", diagnostic1, diagnostic2)
      .addElementsAtTime("2014-09-10T12:01:00Z", diagnostic1, diagnostic2)
      .advanceWatermarkToInfinity()

    val results =
      aggregateAndEncodeRecord(sc.testBounded(input), FiveMinutes, DefaultWindowOptions)

    val endOfWindow = "2014-09-10T12:04:59.999Z"
    val diagnosticRecord = anyDiagnosticRecord
      .copy(created_at = Instant.parse(endOfWindow))

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containInAnyOrderAtTime(
        endOfWindow,
        Seq(
          diagnosticRecord.copy(id = id1, reason = reason1, count = 1 + 1),
          diagnosticRecord.copy(id = id2, reason = reason2, count = 2 + 2)
        )
      )
    }
  }
}
