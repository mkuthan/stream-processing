package org.mkuthan.streamprocessing.toll.application.batch

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime

trait TollBatchJobIo extends TollBoothIo with VehicleIo

trait TollBoothIo {
  val EntryTableIoId: IoIdentifier[TollBoothEntry.Record] =
    IoIdentifier[TollBoothEntry.Record]("entry-table-id")

  val ExitTableIoId: IoIdentifier[TollBoothExit.Record] =
    IoIdentifier[TollBoothExit.Record]("exit-table-id")

  val EntryStatsHourlyTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier[TollBoothStats.Record]("entry-stats-hourly-table-id")

  val EntryStatsDailyTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier[TollBoothStats.Record]("entry-stats-daily-table-id")
}

trait VehicleIo {
  val TotalVehicleTimeOneHourGapTableIoId: IoIdentifier[TotalVehicleTime.Record] =
    IoIdentifier[TotalVehicleTime.Record]("total-vehicle-time-one-hour-gap-table-id")
}
