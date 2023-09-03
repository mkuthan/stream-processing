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

  it should "decode valid VehicleRegistration into raw" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[VehicleRegistration.Raw]
      .addElementsAtMinimumTime(anyVehicleRegistrationRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testUnbounded(inputs))

    results should containSingleValue(anyVehicleRegistration)
    dlq should beEmpty
  }

  it should "put invalid VehicleRegistration into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[VehicleRegistration.Raw]
        .addElementsAtMinimumTime(vehicleRegistrationRawInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testUnbounded(inputs))

      results should beEmpty
      dlq should containSingleValue(vehicleRegistrationDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(VehicleRegistration.DlqCounter).attempted shouldBe 1
  }

  it should "union history with updates" in runWithScioContext { sc =>
    val vehicleRegistrationHistory = anyVehicleRegistrationRaw.copy(id = "history")
    val history = boundedTestCollectionOf[VehicleRegistration.Raw]
      .addElementsAtMinimumTime(vehicleRegistrationHistory)
      .build()

    val vehicleRegistrationUpdate = anyVehicleRegistrationRaw.copy(id = "update")
    val updates = unboundedTestCollectionOf[Message[VehicleRegistration.Raw]]
      .addElementsAtMinimumTime(Message(vehicleRegistrationUpdate))
      .advanceWatermarkToInfinity()

    val result = unionHistoryWithUpdates(sc.testBounded(history), sc.testUnbounded(updates))

    result should containInAnyOrder(Seq(vehicleRegistrationHistory, vehicleRegistrationUpdate))
  }
}
