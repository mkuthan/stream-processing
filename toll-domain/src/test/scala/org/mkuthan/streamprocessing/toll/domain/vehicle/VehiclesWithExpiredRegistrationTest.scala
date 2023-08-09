package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.testing._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio._

class VehiclesWithExpiredRegistrationTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with VehiclesWithExpiredRegistrationFixture {

  import VehiclesWithExpiredRegistration._

  behavior of "VehiclesWithExpiredRegistration"

  ignore should "calculate ..." in runWithScioContext { sc =>
    // TODO: implement test
  }

  it should "encode VehiclesWithExpiredRegistration into Raw" in runWithScioContext { sc =>
    val inputs = testStreamOf[VehiclesWithExpiredRegistration]
      .addElementsAtTime(anyVehicleWithExpiredRegistrationRaw.created_at, anyVehicleWithExpiredRegistration)
      .advanceWatermarkToInfinity()

    val results = encode(sc.testStream(inputs))
    results should containSingleValue(anyVehicleWithExpiredRegistrationRaw)
  }
}
