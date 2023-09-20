package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.Args

import org.joda.time.LocalDate

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

case class TollBatchJobConfig(
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
    totalVehicleTimeOneHourGapPartition: BigQueryPartition[TotalVehicleTime.Record],
    totalVehicleTimeDiagnosticOneHourGapTable: BigQueryPartition[TotalVehicleTimeDiagnostic.Record]
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
      totalVehicleTimeOneHourGapPartition =
        BigQueryPartition.daily(args.required("totalVehicleTimeOneHourGapTable"), effectiveDate),
      totalVehicleTimeDiagnosticOneHourGapTable =
        BigQueryPartition.daily(args.required("totalVehicleTimeDiagnosticOneHourGapTable"), effectiveDate)
    )
  }
}