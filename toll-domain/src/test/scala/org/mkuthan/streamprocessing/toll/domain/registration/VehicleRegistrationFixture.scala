package org.mkuthan.streamprocessing.toll.domain.registration

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait VehicleRegistrationFixture {

  private final val messageTimestamp = "2014-09-10T11:59:00Z"
  private final val defaultLicensePlate = "JNB 7001"
  private final val defaultExpired = "1"

  private final val anyVehicleRegistrationPayload = VehicleRegistration.Payload(
    id = "1",
    license_plate = defaultLicensePlate,
    expired = defaultExpired
  )

  final val anyVehicleRegistrationMessage: Message[VehicleRegistration.Payload] = Message(
    anyVehicleRegistrationPayload,
    Map(VehicleRegistration.TimestampAttribute -> messageTimestamp)
  )

  final val invalidVehicleRegistrationMessage: Message[VehicleRegistration.Payload] = Message(
    anyVehicleRegistrationPayload.copy(license_plate = ""),
    anyVehicleRegistrationMessage.attributes
  )

  final val vehicleRegistrationDecodingError: DeadLetter[VehicleRegistration.Payload] =
    DeadLetter[VehicleRegistration.Payload](
      data = invalidVehicleRegistrationMessage.payload,
      error = "requirement failed: License plate number is empty"
    )

  final val anyVehicleRegistrationRecord: VehicleRegistration.Record = VehicleRegistration.Record(
    id = "2",
    license_plate = defaultLicensePlate,
    expired = defaultExpired.toInt
  )

  final val anyVehicleRegistrationUpdate: VehicleRegistration = VehicleRegistration(
    id = VehicleRegistrationId(anyVehicleRegistrationMessage.payload.id),
    registrationTime = Instant.parse("2014-09-10T11:59:00Z"), // before toll booth entry
    licensePlate = LicensePlate(defaultLicensePlate),
    expired = defaultExpired == "1"
  )

  final val anyVehicleRegistrationHistory: VehicleRegistration = VehicleRegistration(
    id = VehicleRegistrationId(anyVehicleRegistrationRecord.id),
    registrationTime = Instant.parse("2014-09-09T00:00:00.000Z"), // the previous day
    licensePlate = LicensePlate(defaultLicensePlate),
    expired = defaultExpired == "1"
  )
}

object VehicleRegistrationFixture extends VehicleRegistrationFixture
