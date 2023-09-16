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
import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic

object TollBatchJob extends TollBatchJobIo {

  // TODO: share with streaming or not?
  private val TenMinutes = Duration.standardMinutes(10)

  // TODO: share with streaming or not?
  private val DiagnosticWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
    allowedLateness = Duration.ZERO,
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollBatchJobConfig.parse(args)

    // read toll booth entries and toll booth exists
    val boothEntryRecords = sc.readFromBigQuery(
      EntryTableIoId,
      config.entryTable,
      StorageReadConfiguration()
        .withRowRestriction(
          RowRestriction.DateColumnRestriction(TollBoothEntry.PartitioningColumnName, config.effectiveDate)
        )
    )
    val boothEntries = TollBoothEntry.decodeRecord(boothEntryRecords)

    val boothExitRecords = sc.readFromBigQuery(
      ExitTableIoId,
      config.exitTable,
      StorageReadConfiguration()
        .withRowRestriction(
          RowRestriction.DateColumnRestriction(TollBoothExit.PartitioningColumnName, config.effectiveDate)
        )
    )
    val boothExits = TollBoothExit.decodeRecord(boothExitRecords)

    // read vehicle registrations (TODO)

    // calculate tool booth stats
    val boothStats = TollBoothStats.calculateInFixedWindow(boothEntries, TenMinutes)
    TollBoothStats
      .encode(boothStats)
      .writeBoundedToBigQuery(EntryStatsTableIoId, config.entryStatsPartition)

    // calculate total vehicle times
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(boothEntries, boothExits, TenMinutes)
    TotalVehicleTime
      .encode(totalVehicleTimes)
      .writeBoundedToBigQuery(TotalVehicleTimeTableIoId, config.totalVehicleTimePartition)

    totalVehicleTimesDiagnostic
      .sumByKeyInFixedWindow(windowDuration = TenMinutes, windowOptions = DiagnosticWindowOptions)
      .mapWithTimestamp(TotalVehicleTimeDiagnostic.toRaw)
      .writeBoundedToBigQuery(TotalVehicleTimeDiagnosticTableIoId, config.totalVehicleTimeDiagnosticPartition)

    val _ = sc.run()
  }
}
