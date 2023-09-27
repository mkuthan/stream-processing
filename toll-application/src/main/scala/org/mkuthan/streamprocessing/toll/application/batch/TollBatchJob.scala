package org.mkuthan.streamprocessing.toll.application.batch

import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.WindowOptions
import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
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

  private val TwoDays = Duration.standardDays(1)

  private val DefaultWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
    allowedLateness = Duration.ZERO,
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollBatchJobConfig.parse(args)

    // read toll booth entries and toll booth exists
    val entryRecords = sc.readFromBigQuery(
      EntryTableIoId,
      config.entryTable,
      StorageReadConfiguration().withRowRestriction(
        RowRestriction.TimestampColumnRestriction(TollBoothEntry.PartitioningColumnName, config.effectiveDate)
      )
    )
    val entries = TollBoothEntry.decodeRecord(entryRecords)

    val exitRecords = sc.readFromBigQuery(
      ExitTableIoId,
      config.exitTable,
      StorageReadConfiguration().withRowRestriction(
        RowRestriction.TimestampColumnRestriction(TollBoothExit.PartitioningColumnName, config.effectiveDate)
      )
    )
    val exits = TollBoothExit.decodeRecord(exitRecords)

    // read vehicle registrations
    val vehicleRegistrationRecords =
      sc.readFromBigQuery(
        VehicleRegistrationTableIoId,
        config.vehicleRegistrationTable,
        StorageReadConfiguration().withRowRestriction(
          RowRestriction.PartitionDateRestriction(config.effectiveDate)
        )
      )

    val vehicleRegistrations = VehicleRegistration.decodeRecord(vehicleRegistrationRecords, config.effectiveDate)

    // calculate tool booth stats
    val tollBoothStatsHourly = TollBoothStats.calculateInFixedWindow(entries, OneHour, DefaultWindowOptions)
    TollBoothStats
      .encode(tollBoothStatsHourly)
      .writeBoundedToBigQuery(EntryStatsHourlyTableIoId, config.entryStatsHourlyPartition)

    val tollBoothStatsDaily = TollBoothStats.calculateInFixedWindow(entries, OneDay, DefaultWindowOptions)
    TollBoothStats
      .encode(tollBoothStatsDaily)
      .writeBoundedToBigQuery(EntryStatsDailyTableIoId, config.entryStatsDailyPartition)

    // calculate total vehicle times
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(entries, exits, OneHour, DefaultWindowOptions)
    TotalVehicleTime
      .encodeRecord(totalVehicleTimes)
      .writeBoundedToBigQuery(TotalVehicleTimeOneHourGapTableIoId, config.totalVehicleTimeOneHourGapPartition)

    TotalVehicleTimeDiagnostic
      .aggregateAndEncode(totalVehicleTimesDiagnostic, OneDay, DefaultWindowOptions)
      .writeBoundedToBigQuery(
        TotalVehicleTimeDiagnosticOneHourGapTableIoId,
        config.totalVehicleTimeDiagnosticOneHourGapTable
      )

    // calculate vehicles with expired registrations
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculateWithTemporalJoin(
        entries,
        vehicleRegistrations,
        leftWindowDuration = OneDay,
        rightWindowDuration = TwoDays,
        windowOptions = DefaultWindowOptions
      )
    VehiclesWithExpiredRegistration
      .encodeRecord(vehiclesWithExpiredRegistration)
      .writeBoundedToBigQuery(
        VehiclesWithExpiredRegistrationDailyTableIoId,
        config.vehiclesWithExpiredRegistrationDailyPartition
      )

    VehiclesWithExpiredRegistrationDiagnostic
      .aggregateAndEncode(vehiclesWithExpiredRegistrationDiagnostic, OneDay, DefaultWindowOptions)
      .writeBoundedToBigQuery(
        VehiclesWithExpiredRegistrationDiagnosticDailyTableIoId,
        config.vehiclesWithExpiredRegistrationDiagnosticDailyPartition
      )

    val _ = sc.run()
  }
}
