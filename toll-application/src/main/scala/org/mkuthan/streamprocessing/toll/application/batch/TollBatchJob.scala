package org.mkuthan.streamprocessing.toll.application.batch

import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.ScioContext

import org.joda.time.Duration

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

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

    val entries = getEntries(sc, config)
    val exits = getExits(sc, config)
    val vehicleRegistrations = getVehicleRegistrations(sc, config)

    calculateTollBoothStats(config, entries)
    calculateTotalVehicleTimes(config, entries, exits)
    calculateVehiclesWithExpiredRegistrations(config, entries, vehicleRegistrations)

    val _ = sc.run()
  }

  private def getEntries(sc: ScioContext, config: TollBatchJobConfig): SCollection[TollBoothEntry] = {
    val entryRecords = sc.readFromBigQuery(
      EntryTableIoId,
      config.entryTable,
      StorageReadConfiguration().withRowRestriction(
        RowRestriction.TimestampColumnRestriction(TollBoothEntry.PartitioningColumnName, config.effectiveDate)
      )
    )
    TollBoothEntry.decodeRecord(entryRecords)
  }

  private def getExits(sc: ScioContext, config: TollBatchJobConfig): SCollection[TollBoothExit] = {
    val exitRecords = sc.readFromBigQuery(
      ExitTableIoId,
      config.exitTable,
      StorageReadConfiguration().withRowRestriction(
        RowRestriction.TimestampColumnRestriction(TollBoothExit.PartitioningColumnName, config.effectiveDate)
      )
    )
    TollBoothExit.decodeRecord(exitRecords)
  }

  private def getVehicleRegistrations(
      sc: ScioContext,
      config: TollBatchJobConfig
  ): SCollection[VehicleRegistration] = {
    val vehicleRegistrationRecords =
      sc.readFromBigQuery(
        VehicleRegistrationTableIoId,
        config.vehicleRegistrationTable,
        StorageReadConfiguration().withRowRestriction(
          RowRestriction.PartitionDateRestriction(config.effectiveDate)
        )
      )

    VehicleRegistration.decodeRecord(vehicleRegistrationRecords, config.effectiveDate)
  }

  private def calculateTollBoothStats(
      config: TollBatchJobConfig,
      entries: SCollection[TollBoothEntry]
  ): Unit = {
    val tollBoothStatsHourly = TollBoothStats.calculateInFixedWindow(entries, OneHour, DefaultWindowOptions)
    TollBoothStats
      .encodeRecord(tollBoothStatsHourly)
      .writeBoundedToBigQuery(EntryStatsHourlyTableIoId, config.entryStatsHourlyPartition)

    val tollBoothStatsDaily = TollBoothStats.calculateInFixedWindow(entries, OneDay, DefaultWindowOptions)
    TollBoothStats
      .encodeRecord(tollBoothStatsDaily)
      .writeBoundedToBigQuery(EntryStatsDailyTableIoId, config.entryStatsDailyPartition)

  }

  private def calculateTotalVehicleTimes(
      config: TollBatchJobConfig,
      entries: SCollection[TollBoothEntry],
      exits: SCollection[TollBoothExit]
  ): Unit = {
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTimes.calculateInSessionWindow(entries, exits, OneHour, DefaultWindowOptions)

    TotalVehicleTimes
      .encodeRecord(totalVehicleTimes)
      .writeBoundedToBigQuery(TotalVehicleTimesOneHourGapTableIoId, config.totalVehicleTimesOneHourGapPartition)

    TollBoothDiagnostic
      .aggregateAndEncodeRecord(totalVehicleTimesDiagnostic, OneDay, DefaultWindowOptions)
      .writeBoundedToBigQuery(
        TotalVehicleTimesDiagnosticOneHourGapTableIoId,
        config.totalVehicleTimesOneHourGapDiagnosticTable
      )
  }

  private def calculateVehiclesWithExpiredRegistrations(
      config: TollBatchJobConfig,
      entries: SCollection[TollBoothEntry],
      vehicleRegistrations: SCollection[VehicleRegistration]
  ): Unit = {
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

    TollBoothDiagnostic
      .aggregateAndEncodeRecord(vehiclesWithExpiredRegistrationDiagnostic, OneDay, DefaultWindowOptions)
      .writeBoundedToBigQuery(
        VehiclesWithExpiredRegistrationDiagnosticDailyTableIoId,
        config.vehiclesWithExpiredRegistrationDiagnosticDailyPartition
      )
  }

}
