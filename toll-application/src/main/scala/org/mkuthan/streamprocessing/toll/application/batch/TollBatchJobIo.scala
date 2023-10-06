package org.mkuthan.streamprocessing.toll.application.batch

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

trait TollBatchJobIo extends TollBoothIo with RegistrationIo with VehicleIo

trait TollBoothIo {
  val EntryTableIoId: IoIdentifier[TollBoothEntry.Record] =
    IoIdentifier("entry-table-id")

  val ExitTableIoId: IoIdentifier[TollBoothExit.Record] =
    IoIdentifier("exit-table-id")

  val EntryStatsHourlyTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier("entry-stats-hourly-table-id")

  val EntryStatsDailyTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier("entry-stats-daily-table-id")
}

trait RegistrationIo {
  val VehicleRegistrationTableIoId: IoIdentifier[VehicleRegistration.Record] =
    IoIdentifier("toll.vehicle_registration")
}

trait VehicleIo {
  val VehiclesWithExpiredRegistrationDailyTableIoId: IoIdentifier[VehiclesWithExpiredRegistration.Record] =
    IoIdentifier("vehicles-with-expired-registration-daily-table-id")

  val VehiclesWithExpiredRegistrationDiagnosticDailyTableIoId: IoIdentifier[TollBoothDiagnostic.Record] =
    IoIdentifier("vehicles-with-expired-registration-diagnostic-daily-table-id")

  val TotalVehicleTimesOneHourGapTableIoId: IoIdentifier[TotalVehicleTimes.Record] =
    IoIdentifier("total-vehicle-times-one-hour-gap-table-id")

  val TotalVehicleTimesDiagnosticOneHourGapTableIoId: IoIdentifier[TollBoothDiagnostic.Record] =
    IoIdentifier("total-vehicle-times-diagnostic-one-hour-gap-table-id")
}
