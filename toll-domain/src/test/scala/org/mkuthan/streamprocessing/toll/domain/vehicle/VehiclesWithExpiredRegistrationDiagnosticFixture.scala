package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

trait VehiclesWithExpiredRegistrationDiagnosticFixture {

  final val anyVehiclesWithExpiredRegistrationDiagnostic: VehiclesWithExpiredRegistrationDiagnostic =
    VehiclesWithExpiredRegistrationDiagnostic(
      tollBoothId = TollBoothId("1"),
      reason = "any reason",
      count = 1
    )

  final val anyVehiclesWithExpiredRegistrationDiagnosticRecord: VehiclesWithExpiredRegistrationDiagnostic.Record =
    VehiclesWithExpiredRegistrationDiagnostic.Record(
      created_at = Instant.EPOCH,
      toll_booth_id = anyVehiclesWithExpiredRegistrationDiagnostic.tollBoothId.id,
      reason = anyVehiclesWithExpiredRegistrationDiagnostic.reason,
      count = anyVehiclesWithExpiredRegistrationDiagnostic.count
    )

  final val vehicleWithNotExpiredRegistrationDiagnostic: VehiclesWithExpiredRegistrationDiagnostic =
    anyVehiclesWithExpiredRegistrationDiagnostic.copy(
      reason = VehiclesWithExpiredRegistrationDiagnostic.NotExpired
    )

  final val vehicleWithMissingRegistrationDiagnostic: VehiclesWithExpiredRegistrationDiagnostic =
    anyVehiclesWithExpiredRegistrationDiagnostic.copy(
      reason = VehiclesWithExpiredRegistrationDiagnostic.MissingRegistration
    )

}
