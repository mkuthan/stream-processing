package org.mkuthan.streamprocessing.toll.domain.registration

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait VehicleRegistrationFixture {

  final val anyVehicleRegistrationRecord = VehicleRegistration.Record(
    id = "1",
    license_plate = "JNB 7001",
    expired = 1
  )

  final val vehicleRegistrationRecordInvalid = anyVehicleRegistrationRecord.copy(expired = -1)

  final val vehicleRegistrationDecodingError = DeadLetter[VehicleRegistration.Record](
    data = vehicleRegistrationRecordInvalid,
    error = "requirement failed: Field 'expired' must be positive but was '-1'"
  )

  final val anyVehicleRegistration = VehicleRegistration(
    id = VehicleRegistrationId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    expired = true
  )
}
