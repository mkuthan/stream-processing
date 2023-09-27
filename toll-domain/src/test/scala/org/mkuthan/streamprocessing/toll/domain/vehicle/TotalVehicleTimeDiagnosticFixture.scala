package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

trait TotalVehicleTimeDiagnosticFixture {

  final val anyDiagnostic: TotalVehicleTimeDiagnostic = TotalVehicleTimeDiagnostic(
    tollBoothId = TollBoothId("1"),
    reason = "any reason",
    count = 1
  )

  final val anyDiagnosticRecord: TotalVehicleTimeDiagnostic.Record = TotalVehicleTimeDiagnostic.Record(
    created_at = Instant.EPOCH,
    toll_booth_id = anyDiagnostic.tollBoothId.id,
    reason = anyDiagnostic.reason,
    count = anyDiagnostic.count
  )
}
