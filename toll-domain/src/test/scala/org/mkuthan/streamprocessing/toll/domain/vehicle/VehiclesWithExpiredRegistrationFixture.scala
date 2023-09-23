package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

trait VehiclesWithExpiredRegistrationFixture {
  private val defaultRegistrationId = "1"
  private val defaultLicencePlate = "JNB 7001"
  private val defaultTollBoothId = "1"
  private val defaultEntryTime = "2014-09-10T12:01:00.000Z"

  final def anyVehicleWithExpiredRegistration(id: VehicleRegistrationId =
    VehicleRegistrationId(defaultRegistrationId)): VehiclesWithExpiredRegistration = VehiclesWithExpiredRegistration(
    vehicleRegistrationId = id,
    licensePlate = LicensePlate(defaultLicencePlate),
    tollBoothId = TollBoothId(defaultTollBoothId),
    entryTime = Instant.parse(defaultEntryTime)
  )

  final def anyVehicleWithExpiredRegistrationRecord(
      createdAt: Instant,
      id: String = defaultRegistrationId
  ): VehiclesWithExpiredRegistration.Record =
    VehiclesWithExpiredRegistration.Record(
      created_at = createdAt,
      vehicle_registration_id = id,
      license_plate = defaultLicencePlate,
      toll_booth_id = defaultTollBoothId,
      entry_time = Instant.parse(defaultEntryTime)
    )

  final def anyVehicleWithExpiredRegistrationMessage(
      createdAt: Instant,
      id: String = defaultRegistrationId
  ): Message[VehiclesWithExpiredRegistration.Payload] =
    Message(
      VehiclesWithExpiredRegistration.Payload(
        created_at = createdAt.toString,
        vehicle_registration_id = id,
        license_plate = defaultLicencePlate,
        toll_booth_id = defaultTollBoothId,
        entry_time = defaultEntryTime
      ),
      Map(VehiclesWithExpiredRegistration.TimestampAttribute -> createdAt.toString)
    )

  final val vehicleWithNotExpiredRegistrationDiagnostic: VehiclesWithExpiredRegistrationDiagnostic =
    VehiclesWithExpiredRegistrationDiagnostic(
      tollBothId = TollBoothId("1"),
      reason = VehiclesWithExpiredRegistrationDiagnostic.NotExpired
    )

  final val vehicleWithMissingRegistrationDiagnostic: VehiclesWithExpiredRegistrationDiagnostic =
    VehiclesWithExpiredRegistrationDiagnostic(
      tollBothId = TollBoothId("1"),
      reason = VehiclesWithExpiredRegistrationDiagnostic.MissingRegistration
    )
}
