package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction.PartitionDateRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

object TollBatchJob extends TollBatchJobIo {

  private val OneHour = Duration.standardHours(1)

  private val OneDay = Duration.standardDays(1)

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollBatchJobConfig.parse(args)

    // read toll booth entries and toll booth exists
    val boothEntryRecords = sc.readFromBigQuery(
      EntryTableIoId,
      config.entryTable,
      StorageReadConfiguration().withRowRestriction(
        RowRestriction.DateColumnRestriction(TollBoothEntry.PartitioningColumnName, config.effectiveDate)
      )
    )
    val boothEntries = TollBoothEntry.decodeRecord(boothEntryRecords)

    val boothExitRecords = sc.readFromBigQuery(
      ExitTableIoId,
      config.exitTable,
      StorageReadConfiguration().withRowRestriction(
        RowRestriction.DateColumnRestriction(TollBoothExit.PartitioningColumnName, config.effectiveDate)
      )
    )
    val boothExits = TollBoothExit.decodeRecord(boothExitRecords)

    // read vehicle registrations
    val vehicleRegistrationRecords =
      sc.readFromBigQuery(
        VehicleRegistrationTableIoId,
        config.vehicleRegistrationTable,
        StorageReadConfiguration().withRowRestriction(
          PartitionDateRestriction(config.effectiveDate)
        )
      )

    val vehicleRegistrations = VehicleRegistration.decodeRecord(vehicleRegistrationRecords, config.effectiveDate)

    // calculate tool booth stats
    val boothStatsHourly = TollBoothStats.calculateInFixedWindow(boothEntries, OneHour)
    TollBoothStats
      .encode(boothStatsHourly)
      .writeBoundedToBigQuery(EntryStatsHourlyTableIoId, config.entryStatsHourlyPartition)

    val boothStatsDaily = TollBoothStats.calculateInFixedWindow(boothEntries, OneDay)
    TollBoothStats
      .encode(boothStatsDaily)
      .writeBoundedToBigQuery(EntryStatsDailyTableIoId, config.entryStatsDailyPartition)

    // calculate total vehicle times
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(boothEntries, boothExits, OneHour)
    TotalVehicleTime
      .encodeRecord(totalVehicleTimes)
      .writeBoundedToBigQuery(TotalVehicleTimeOneHourGapTableIoId, config.totalVehicleTimeOneHourGapPartition)

    totalVehicleTimesDiagnostic
      .sumByKeyInFixedWindow(windowDuration = OneDay)
      .mapWithTimestamp(TotalVehicleTimeDiagnostic.toRecord)
      .writeBoundedToBigQuery(
        TotalVehicleTimeDiagnosticOneHourGapTableIoId,
        config.totalVehicleTimeDiagnosticOneHourGapTable
      )

    // calculate vehicles with expired registrations
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculateInFixedWindow(boothEntries, vehicleRegistrations, OneDay)
    VehiclesWithExpiredRegistration
      .encodeRecord(vehiclesWithExpiredRegistration)
      .writeBoundedToBigQuery(
        VehiclesWithExpiredRegistrationDailyTableIoId,
        config.vehiclesWithExpiredRegistrationDailyPartition
      )

    vehiclesWithExpiredRegistrationDiagnostic
      .sumByKeyInFixedWindow(windowDuration = OneDay)
      .mapWithTimestamp(VehiclesWithExpiredRegistrationDiagnostic.toRecord)
      .writeBoundedToBigQuery(
        VehiclesWithExpiredRegistrationDiagnosticDailyTableIoId,
        config.vehiclesWithExpiredRegistrationDiagnosticDailyPartition
      )

    val _ = sc.run()
  }
}
