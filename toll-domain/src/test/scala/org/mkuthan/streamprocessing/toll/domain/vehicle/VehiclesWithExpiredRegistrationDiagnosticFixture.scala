package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

trait VehiclesWithExpiredRegistrationDiagnosticFixture {

  final val anyDiagnostic: VehiclesWithExpiredRegistrationDiagnostic =
    VehiclesWithExpiredRegistrationDiagnostic(
      tollBothId = TollBoothId("1"),
      reason = "any reason",
      count = 1
    )

  final val anyDiagnosticRecord: VehiclesWithExpiredRegistrationDiagnostic.Record =
    VehiclesWithExpiredRegistrationDiagnostic.Record(
      created_at = Instant.EPOCH,
      toll_both_id = anyDiagnostic.tollBothId.id,
      reason = anyDiagnostic.reason,
      count = anyDiagnostic.count
    )
}
