package org.mkuthan.streamprocessing.toll.domain.registration

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait VehicleRegistrationFixture {

  final val anyVehicleRegistrationPayload = VehicleRegistration.Payload(
    id = "1",
    registration_time = "2014-09-10T11:59:00Z", // before toll booth entry
    license_plate = "JNB 7001",
    expired = "1"
  )

  final val vehicleRegistrationPayloadInvalid = anyVehicleRegistrationPayload.copy(license_plate = "")

  final val vehicleRegistrationDecodingError = DeadLetter[VehicleRegistration.Payload](
    data = vehicleRegistrationPayloadInvalid,
    error = "requirement failed: Licence plate number is empty"
  )

  final val anyVehicleRegistrationRecord = VehicleRegistration.Record(
    id = "2",
    license_plate = anyVehicleRegistrationPayload.license_plate,
    expired = anyVehicleRegistrationPayload.expired.toInt
  )

  final val anyVehicleRegistrationUpdate = VehicleRegistration(
    id = VehicleRegistrationId(anyVehicleRegistrationPayload.id),
    registrationTime = Instant.parse(anyVehicleRegistrationPayload.registration_time),
    licensePlate = LicensePlate(anyVehicleRegistrationPayload.license_plate),
    expired = anyVehicleRegistrationPayload.expired == "1"
  )

  final val anyVehicleRegistrationHistory = VehicleRegistration(
    id = VehicleRegistrationId(anyVehicleRegistrationRecord.id),
    registrationTime = Instant.parse("2014-09-09T00:00:00.000Z"), // the previous day
    licensePlate = LicensePlate(anyVehicleRegistrationRecord.license_plate),
    expired = anyVehicleRegistrationRecord.expired == 1
  )
}
