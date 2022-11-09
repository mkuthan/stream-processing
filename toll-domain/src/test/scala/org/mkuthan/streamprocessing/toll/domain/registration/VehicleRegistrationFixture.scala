package org.mkuthan.streamprocessing.toll.domain.registration

import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait VehicleRegistrationFixture {

  val anyVehicleRegistrationRaw = VehicleRegistration.Raw(
    id = "1",
    license_plate = "JNB 7001",
    expired = 0
  )

  val vehicleRegistrationRawInvalid = anyVehicleRegistrationRaw.copy(expired = -1)

  val anyVehicleRegistration = VehicleRegistration(
    id = VehicleRegistrationId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    expired = false
  )
}
