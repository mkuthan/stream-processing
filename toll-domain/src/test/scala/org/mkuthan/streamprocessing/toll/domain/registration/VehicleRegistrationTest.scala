package org.mkuthan.streamprocessing.toll.domain.registration

import org.joda.time.LocalDate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class VehicleRegistrationTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with VehicleRegistrationFixture {

  import VehicleRegistration._

  behavior of "VehicleRegistration"

  it should "decode valid message into VehicleRegistration" in runWithScioContext { sc =>
    val input = unboundedTestCollectionOf[Message[VehicleRegistration.Payload]]
      .addElementsAtTime(
        anyVehicleRegistrationMessage.attributes(VehicleRegistration.TimestampAttribute),
        anyVehicleRegistrationMessage
      )
      .advanceWatermarkToInfinity()

    val (results, dlq) = decodeMessage(sc.testUnbounded(input))

    results should containElements(anyVehicleRegistrationUpdate)
    dlq should beEmpty
  }

  it should "put invalid message into DLQ" in {
    val run = runWithScioContext { sc =>
      val input = unboundedTestCollectionOf[Message[VehicleRegistration.Payload]]
        .addElementsAtTime(
          invalidVehicleRegistrationMessage.attributes(VehicleRegistration.TimestampAttribute),
          invalidVehicleRegistrationMessage
        )
        .advanceWatermarkToInfinity()

      val (results, dlq) = decodeMessage(sc.testUnbounded(input))

      results should beEmpty
      dlq should containElements(vehicleRegistrationDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(VehicleRegistration.DlqCounter).attempted shouldBe 1
  }

  it should "decode historical record into VehicleRegistration" in runWithScioContext { sc =>
    val partitionDate = LocalDate.parse("2014-09-09")

    val history = boundedTestCollectionOf[VehicleRegistration.Record]
      .addElementsAtMinimumTime(anyVehicleRegistrationRecord)
      .advanceWatermarkToInfinity()

    val results = decodeRecord(sc.testBounded(history), partitionDate)

    results should containElements(anyVehicleRegistrationHistory)
  }

  it should "union history with updates" in runWithScioContext { sc =>
    val history = boundedTestCollectionOf[VehicleRegistration]
      .addElementsAtTime(anyVehicleRegistrationHistory.registrationTime, anyVehicleRegistrationHistory)
      .advanceWatermarkToInfinity()

    val updates = unboundedTestCollectionOf[VehicleRegistration]
      .addElementsAtTime(anyVehicleRegistrationUpdate.registrationTime, anyVehicleRegistrationUpdate)
      .advanceWatermarkToInfinity()

    val results = VehicleRegistration.unionHistoryWithUpdates(sc.testBounded(history), sc.testUnbounded(updates))

    results should containElements(anyVehicleRegistrationHistory, anyVehicleRegistrationUpdate)
  }

}
