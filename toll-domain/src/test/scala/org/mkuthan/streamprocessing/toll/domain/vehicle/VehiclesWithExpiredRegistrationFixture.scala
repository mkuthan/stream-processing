package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

trait VehiclesWithExpiredRegistrationFixture {
  private val defaultRegistrationId = "1"

  final val anyVehicleWithExpiredRegistration = VehiclesWithExpiredRegistration(
    vehicleRegistrationId = VehicleRegistrationId(defaultRegistrationId),
    licensePlate = LicensePlate("JNB 7001"),
    tollBoothId = TollBoothId("1"),
    entryTime = Instant.parse("2014-09-10T12:01:00.000Z")
  )

  final def anyVehicleWithExpiredRegistrationRecord(createdAt: Instant, id: String = defaultRegistrationId) =
    VehiclesWithExpiredRegistration.Record(
      created_at = createdAt,
      vehicle_registration_id = id,
      license_plate = anyVehicleWithExpiredRegistration.licensePlate.number,
      toll_booth_id = anyVehicleWithExpiredRegistration.tollBoothId.id,
      entry_time = anyVehicleWithExpiredRegistration.entryTime
    )

  final def anyVehicleWithExpiredRegistrationMessage(createdAt: Instant, id: String = defaultRegistrationId) =
    Message(
      VehiclesWithExpiredRegistration.Payload(
        created_at = createdAt.toString,
        vehicle_registration_id = id,
        license_plate = anyVehicleWithExpiredRegistration.licensePlate.number,
        toll_booth_id = anyVehicleWithExpiredRegistration.tollBoothId.id,
        entry_time = anyVehicleWithExpiredRegistration.entryTime.toString
      ),
      Map(VehiclesWithExpiredRegistration.TimestampAttribute -> createdAt.toString)
    )

  final val vehicleWithNotExpiredRegistrationDiagnostic = VehiclesWithExpiredRegistrationDiagnostic(
    tollBothId = TollBoothId("1"),
    reason = "Vehicle registration is not expired"
  )

  final val vehicleWithMissingRegistrationDiagnostic = VehiclesWithExpiredRegistrationDiagnostic(
    tollBothId = TollBoothId("1"),
    reason = "Missing vehicle registration"
  )
}
