package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

trait TotalVehicleTimesDiagnosticFixture {

  final val anyTotalVehicleTimesDiagnostic: TotalVehicleTimesDiagnostic = TotalVehicleTimesDiagnostic(
    tollBoothId = TollBoothId("1"),
    reason = "any reason",
    count = 1
  )

  final val anyTotalVehicleTimesDiagnosticRecord: TotalVehicleTimesDiagnostic.Record =
    TotalVehicleTimesDiagnostic.Record(
      created_at = Instant.EPOCH,
      toll_booth_id = anyTotalVehicleTimesDiagnostic.tollBoothId.id,
      reason = anyTotalVehicleTimesDiagnostic.reason,
      count = anyTotalVehicleTimesDiagnostic.count
    )

  final val totalVehicleTimesWithMissingTollBoothExitDiagnostic: TotalVehicleTimesDiagnostic =
    anyTotalVehicleTimesDiagnostic.copy(reason = TotalVehicleTimesDiagnostic.MissingTollBoothExit)
}
