package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

trait VehiclesWithExpiredRegistrationFixture {
  final val anyVehicleWithExpiredRegistration = VehiclesWithExpiredRegistration(
    licensePlate = LicensePlate("JNB 7001"),
    tollBoothId = TollBoothId("1"),
    vehicleRegistrationId = VehicleRegistrationId("1"),
    entryTime = Instant.parse("2014-09-10T12:01:00.000Z")
  )

  final val anyVehicleWithExpiredRegistrationRaw = VehiclesWithExpiredRegistration.Raw(
    created_at = Instant.parse("2014-09-10T12:09:59.999Z"),
    license_plate = "JNB 7001",
    toll_booth_id = "1",
    vehicle_registration_id = "1",
    entry_time = Instant.parse("2014-09-10T12:01:00.000Z")
  )

  final val vehicleWithNotExpiredRegistrationDiagnostic = VehiclesWithExpiredRegistration.Diagnostic(
    created_at = Instant.parse("2014-09-10T12:09:59.999Z"),
    toll_booth_id = "1",
    reason = "Vehicle registration is not expired",
    count = 1
  )

  final val vehicleWithMissingRegistrationDiagnostic = VehiclesWithExpiredRegistration.Diagnostic(
    created_at = Instant.parse("2014-09-10T12:09:59.999Z"),
    toll_booth_id = "1",
    reason = "Missing vehicle registration",
    count = 1
  )
}
