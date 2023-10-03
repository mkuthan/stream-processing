package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.Args

import org.joda.time.LocalDate

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimesDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

final case class TollBatchJobConfig(
    effectiveDate: LocalDate,
    entryTable: BigQueryTable[TollBoothEntry.Record],
    exitTable: BigQueryTable[TollBoothExit.Record],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Record],
    entryStatsHourlyPartition: BigQueryPartition[TollBoothStats.Record],
    entryStatsDailyPartition: BigQueryPartition[TollBoothStats.Record],
    vehiclesWithExpiredRegistrationDailyPartition: BigQueryPartition[VehiclesWithExpiredRegistration.Record],
    vehiclesWithExpiredRegistrationDiagnosticDailyPartition: BigQueryPartition[
      VehiclesWithExpiredRegistrationDiagnostic.Record
    ],
    totalVehicleTimesOneHourGapPartition: BigQueryPartition[TotalVehicleTimes.Record],
    totalVehicleTimesDiagnosticOneHourGapTable: BigQueryPartition[TotalVehicleTimesDiagnostic.Record]
)

object TollBatchJobConfig {
  def parse(args: Args): TollBatchJobConfig = {
    val effectiveDate = LocalDate.parse(args.required("effectiveDate"))
    TollBatchJobConfig(
      effectiveDate = effectiveDate,
      entryTable = BigQueryTable(args.required("entryTable")),
      exitTable = BigQueryTable(args.required("exitTable")),
      vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
      entryStatsHourlyPartition = BigQueryPartition.daily(args.required("entryStatsHourlyTable"), effectiveDate),
      entryStatsDailyPartition = BigQueryPartition.daily(args.required("entryStatsDailyTable"), effectiveDate),
      vehiclesWithExpiredRegistrationDailyPartition =
        BigQueryPartition.daily(args.required("vehiclesWithExpiredRegistrationDailyTable"), effectiveDate),
      vehiclesWithExpiredRegistrationDiagnosticDailyPartition =
        BigQueryPartition.daily(args.required("vehiclesWithExpiredRegistrationDiagnosticDailyTable"), effectiveDate),
      totalVehicleTimesOneHourGapPartition =
        BigQueryPartition.daily(args.required("totalVehicleTimesOneHourGapTable"), effectiveDate),
      totalVehicleTimesDiagnosticOneHourGapTable =
        BigQueryPartition.daily(args.required("totalVehicleTimesDiagnosticOneHourGapTable"), effectiveDate)
    )
  }
}
