package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

trait TotalVehicleTimeDiagnosticFixture {

  final val anyTotalVehicleTimeDiagnostic: TotalVehicleTimeDiagnostic = TotalVehicleTimeDiagnostic(
    tollBoothId = TollBoothId("1"),
    reason = "any reason",
    count = 1
  )

  final val anyTotalVehicleTimeDiagnosticRecord: TotalVehicleTimeDiagnostic.Record = TotalVehicleTimeDiagnostic.Record(
    created_at = Instant.EPOCH,
    toll_booth_id = anyTotalVehicleTimeDiagnostic.tollBoothId.id,
    reason = anyTotalVehicleTimeDiagnostic.reason,
    count = anyTotalVehicleTimeDiagnostic.count
  )

  final val totalVehicleTimeWithMissingTollBoothExitDiagnostic: TotalVehicleTimeDiagnostic =
    anyTotalVehicleTimeDiagnostic.copy(reason = TotalVehicleTimeDiagnostic.MissingTollBoothExit)
}
