package org.mkuthan.streamprocessing.toll.domain.registration

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.TestStreamScioContext

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class VehicleRegistrationTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with VehicleRegistrationFixture {

  import VehicleRegistration._

  behavior of "VehicleRegistration"

  it should "decode valid VehicleRegistration into raw" in runWithScioContext { sc =>
    val inputs = testStreamOf[VehicleRegistration.Raw]
      .addElements(anyVehicleRegistrationRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyVehicleRegistration)
    dlq should beEmpty
  }

  it should "put invalid VehicleRegistration into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = testStreamOf[VehicleRegistration.Raw]
        .addElements(vehicleRegistrationRawInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(vehicleRegistrationDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(VehicleRegistration.DlqCounter).attempted shouldBe 1
  }

  it should "union history with updates" in runWithScioContext { sc =>
    val vehicleRegistrationHistory = anyVehicleRegistrationRaw.copy(id = "history")
    val history = sc.parallelize(Seq(vehicleRegistrationHistory))

    val vehicleRegistrationUpdate = anyVehicleRegistrationRaw.copy(id = "update")
    val updates = testStreamOf[Message[VehicleRegistration.Raw]]
      .addElements(Message(vehicleRegistrationUpdate))
      .advanceWatermarkToInfinity()

    val result = unionHistoryWithUpdates(history, sc.testStream(updates))

    result should containInAnyOrder(Seq(vehicleRegistrationHistory, vehicleRegistrationUpdate))
  }
}
