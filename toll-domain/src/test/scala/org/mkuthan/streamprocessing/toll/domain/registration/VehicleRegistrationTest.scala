package org.mkuthan.streamprocessing.toll.domain.registration

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.TestStreamScioContext
import org.mkuthan.streamprocessing.test.scio.TestScioContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      dlq should containSingleValue(vehicleRegistrationRawInvalid)
    }

    val result = run.waitUntilDone()
    result.counter(VehicleRegistration.DlqCounter).attempted shouldBe 1
  }

}
