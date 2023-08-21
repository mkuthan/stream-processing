package org.mkuthan.streamprocessing.toll.domain.registration

import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait VehicleRegistrationFixture {

  final val anyVehicleRegistrationRaw = VehicleRegistration.Raw(
    id = "1",
    license_plate = "JNB 7001",
    expired = 1
  )

  final val vehicleRegistrationRawInvalid = anyVehicleRegistrationRaw.copy(expired = -1)

  final val anyVehicleRegistration = VehicleRegistration(
    id = VehicleRegistrationId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    expired = true
  )
}
