package org.mkuthan.streamprocessing.toll.domain.registration

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait VehicleRegistrationFixture {

  private final val messageTimestamp = "2014-09-10T11:59:00Z"
  private final val defaultLicensePlate = "JNB 7001"
  private final val defaultExpired = "1"

  final val anyVehicleRegistrationMessage = Message(
    VehicleRegistration.Payload(
      id = "1",
      license_plate = defaultLicensePlate,
      expired = defaultExpired
    ),
    Map(VehicleRegistration.TimestampAttribute -> messageTimestamp)
  )

  final val vehicleRegistrationMessageInvalid = Message(
    anyVehicleRegistrationMessage.payload.copy(license_plate = ""),
    anyVehicleRegistrationMessage.attributes
  )

  final val vehicleRegistrationDecodingError = DeadLetter[VehicleRegistration.Payload](
    data = vehicleRegistrationMessageInvalid.payload,
    error = "requirement failed: Licence plate number is empty"
  )

  final val anyVehicleRegistrationRecord = VehicleRegistration.Record(
    id = "2",
    license_plate = defaultLicensePlate,
    expired = defaultExpired.toInt
  )

  final val anyVehicleRegistrationUpdate = VehicleRegistration(
    id = VehicleRegistrationId(anyVehicleRegistrationMessage.payload.id),
    registrationTime = Instant.parse("2014-09-10T11:59:00Z"), // before toll booth entry
    licensePlate = LicensePlate(defaultLicensePlate),
    expired = defaultExpired == "1"
  )

  final val anyVehicleRegistrationHistory = VehicleRegistration(
    id = VehicleRegistrationId(anyVehicleRegistrationRecord.id),
    registrationTime = Instant.parse("2014-09-09T00:00:00.000Z"), // the previous day
    licensePlate = LicensePlate(defaultLicensePlate),
    expired = defaultExpired == "1"
  )
}
