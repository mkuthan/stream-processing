package org.mkuthan.streamprocessing.toll.application.batch

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic

trait TollBatchJobIo extends TollBoothIo with VehicleIo

trait TollBoothIo {
  val EntryTableIoId: IoIdentifier[TollBoothEntry.Record] =
    IoIdentifier[TollBoothEntry.Record]("entry-table-id")

  val ExitTableIoId: IoIdentifier[TollBoothExit.Record] =
    IoIdentifier[TollBoothExit.Record]("exit-table-id")

  val EntryStatsTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier[TollBoothStats.Record]("entry-stats-table-id")
}

trait VehicleIo {
  val TotalVehicleTimeTableIoId: IoIdentifier[TotalVehicleTime.Record] =
    IoIdentifier[TotalVehicleTime.Record]("total-vehicle-time-table-id")

  val TotalVehicleTimeDiagnosticTableIoId: IoIdentifier[TotalVehicleTimeDiagnostic.Raw] =
    IoIdentifier[TotalVehicleTimeDiagnostic.Raw]("total-vehicle-time-diagnostic-table-id")
}
