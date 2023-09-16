package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.Args

import org.joda.time.LocalDate

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic

case class TollBatchJobConfig(
    effectiveDate: LocalDate,
    entryTable: BigQueryTable[TollBoothEntry.Record],
    exitTable: BigQueryTable[TollBoothExit.Record],
    entryStatsHourlyPartition: BigQueryPartition[TollBoothStats.Record],
    entryStatsDailyPartition: BigQueryPartition[TollBoothStats.Record],
    totalVehicleTimeOneHourGapPartition: BigQueryPartition[TotalVehicleTime.Record]
)

object TollBatchJobConfig {
  def parse(args: Args): TollBatchJobConfig = {
    val effectiveDate = LocalDate.parse(args.required("effectiveDate"))
    TollBatchJobConfig(
      effectiveDate = effectiveDate,
      entryTable = BigQueryTable(args.required("entryTable")),
      exitTable = BigQueryTable(args.required("exitTable")),
      entryStatsHourlyPartition = BigQueryPartition.daily(args.required("entryStatsHourlyTable"), effectiveDate),
      entryStatsDailyPartition = BigQueryPartition.daily(args.required("entryStatsDailyTable"), effectiveDate),
      totalVehicleTimeOneHourGapPartition =
        BigQueryPartition.daily(args.required("totalVehicleTimeOneHourGapTable"), effectiveDate)
    )
  }
}
