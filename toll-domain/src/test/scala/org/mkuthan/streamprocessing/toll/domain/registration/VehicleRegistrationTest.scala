package org.mkuthan.streamprocessing.toll.domain.registration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio._

class VehicleRegistrationTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with VehicleRegistrationFixture {

  import VehicleRegistration._

  behavior of "VehicleRegistration"

  it should "decode valid record into VehicleRegistration" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[VehicleRegistration.Record]
      .addElementsAtWatermarkTime(anyVehicleRegistrationRecord)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testUnbounded(inputs))

    results should containSingleValue(anyVehicleRegistration)
    dlq should beEmpty
  }

  it should "put invalid record into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[VehicleRegistration.Record]
        .addElementsAtWatermarkTime(vehicleRegistrationRecordInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testUnbounded(inputs))

      results should beEmpty
      dlq should containSingleValue(vehicleRegistrationDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(VehicleRegistration.DlqCounter).attempted shouldBe 1
  }

  it should "union history with updates" in runWithScioContext { sc =>
    val vehicleRegistrationHistory = anyVehicleRegistrationRecord.copy(id = "history")
    val history = boundedTestCollectionOf[VehicleRegistration.Record]
      .addElementsAtMinimumTime(vehicleRegistrationHistory)
      .build()

    val vehicleRegistrationUpdate = anyVehicleRegistrationRecord.copy(id = "update")
    val updates = unboundedTestCollectionOf[Message[VehicleRegistration.Record]]
      .addElementsAtWatermarkTime(Message(vehicleRegistrationUpdate))
      .advanceWatermarkToInfinity()

    val result = unionHistoryWithUpdates(sc.testBounded(history), sc.testUnbounded(updates))

    result should containInAnyOrder(Seq(vehicleRegistrationHistory, vehicleRegistrationUpdate))
  }
}
