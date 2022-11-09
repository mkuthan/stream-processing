package org.mkuthan.streamprocessing.toll.domain.registration

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext

class VehicleRegistrationTest extends PipelineSpec with VehicleRegistrationFixture {

  import VehicleRegistration._

  behavior of "VehicleRegistration"

  it should "decode valid VehicleRegistration into raw" in runWithContext { sc =>
    val inputs = testStreamOf[VehicleRegistration.Raw]
      .addElements(anyVehicleRegistrationRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyVehicleRegistration)
    dlq should beEmpty
  }

  it should "put invalid VehicleRegistration into DLQ" in {
    val run = runWithContext { sc =>
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
