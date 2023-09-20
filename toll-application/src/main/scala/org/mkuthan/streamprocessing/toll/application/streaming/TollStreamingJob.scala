package org.mkuthan.streamprocessing.toll.application.streaming

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.WindowOptions
import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration
import org.joda.time.LocalDate

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction.PartitionDateRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.pubsub.JsonReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.NamedTimestampAttribute
import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

object TollStreamingJob extends TollStreamingJobIo {

  private val TenMinutes = Duration.standardMinutes(10)

  private val DeadLetterWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(
      AfterFirst.of(
        AfterWatermark.pastEndOfWindow(),
        AfterPane.elementCountAtLeast(1_000_000)
      )
    ),
    allowedLateness = Duration.ZERO,
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  private val DiagnosticWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
    allowedLateness = Duration.ZERO,
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollStreamingJobConfig.parse(args)

    // receive toll booth entries and toll booth exists
    val (boothEntryMessages, boothEntryMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        EntrySubscriptionIoId,
        config.entrySubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(TollBoothEntry.TimestampAttribute))
      ).unzip

    val (boothEntries, boothEntriesDlq) = TollBoothEntry.decodeMessage(boothEntryMessages)
    boothEntriesDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeUnboundedToStorageAsJson(EntryDlqBucketIoId, config.entryDlq)

    val (boothExitMessages, boothExitMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        ExitSubscriptionIoId,
        config.exitSubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(TollBoothExit.TimestampAttribute))
      ).unzip

    val (boothExits, boothExistsDlq) = TollBoothExit.decodeMessage(boothExitMessages)
    boothExistsDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeUnboundedToStorageAsJson(ExitDlqBucketIoId, config.exitDlq)

    // receive vehicle registrations
    val (vehicleRegistrationMessages, vehicleRegistrationMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        VehicleRegistrationSubscriptionIoId,
        config.vehicleRegistrationSubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(VehicleRegistration.TimestampAttribute))
      ).unzip

    val (vehicleRegistrationUpdates, vehicleRegistrationsDlq) =
      VehicleRegistration.decodeMessage(vehicleRegistrationMessages)

    val partitionDate = LocalDate.now().minusDays(1)
    val vehicleRegistrationRecords =
      sc.readFromBigQuery(
        VehicleRegistrationTableIoId,
        config.vehicleRegistrationTable,
        StorageReadConfiguration().withRowRestriction(
          PartitionDateRestriction(partitionDate)
        )
      )

    val vehicleRegistrationsHistory = VehicleRegistration.decodeRecord(vehicleRegistrationRecords, partitionDate)

    val vehicleRegistrations =
      VehicleRegistration.unionHistoryWithUpdates(vehicleRegistrationsHistory, vehicleRegistrationUpdates)

    vehicleRegistrationsDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeUnboundedToStorageAsJson(VehicleRegistrationDlqBucketIoId, config.vehicleRegistrationDlq)

    // calculate tool booth stats
    val boothStats = TollBoothStats.calculateInFixedWindow(boothEntries, TenMinutes)
    val tollBoothStatsDlq = TollBoothStats
      .encode(boothStats)
      .writeUnboundedToBigQuery(EntryStatsTableIoId, config.entryStatsTable)

    // calculate total vehicle times
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(boothEntries, boothExits, TenMinutes)
    val totalVehicleTimesDlq = TotalVehicleTime
      .encodeRecord(totalVehicleTimes)
      .writeUnboundedToBigQuery(TotalVehicleTimeTableIoId, config.totalVehicleTimeTable)

    totalVehicleTimesDiagnostic
      .sumByKeyInFixedWindow(windowDuration = TenMinutes, windowOptions = DiagnosticWindowOptions)
      .mapWithTimestamp(TotalVehicleTimeDiagnostic.toRecord)
      .writeUnboundedToBigQuery(TotalVehicleTimeDiagnosticTableIoId, config.totalVehicleTimeDiagnosticTable)

    // calculate vehicles with expired registrations
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculateInFixedWindow(boothEntries, vehicleRegistrations, TenMinutes)

    VehiclesWithExpiredRegistration
      .encodeMessage(vehiclesWithExpiredRegistration)
      .publishJsonToPubSub(VehiclesWithExpiredRegistrationTopicIoId, config.vehiclesWithExpiredRegistrationTopic)

    vehiclesWithExpiredRegistrationDiagnostic
      .sumByKeyInFixedWindow(windowDuration = TenMinutes, windowOptions = DiagnosticWindowOptions)
      .mapWithTimestamp(VehiclesWithExpiredRegistrationDiagnostic.toRecord)
      .writeUnboundedToBigQuery(
        VehiclesWithExpiredRegistrationDiagnosticTableIoId,
        config.vehiclesWithExpiredRegistrationDiagnosticTable
      )

    // dead letters diagnostic
    val ioDiagnostics = sc.unionInGlobalWindow(
      boothEntryMessagesDlq.toDiagnostic(EntrySubscriptionIoId),
      boothExitMessagesDlq.toDiagnostic(ExitSubscriptionIoId),
      vehicleRegistrationMessagesDlq.toDiagnostic(VehicleRegistrationSubscriptionIoId),
      tollBoothStatsDlq.toDiagnostic(EntryStatsTableIoId),
      totalVehicleTimesDlq.toDiagnostic(TotalVehicleTimeTableIoId)
    )

    ioDiagnostics
      .sumByKeyInFixedWindow(windowDuration = TenMinutes, windowOptions = DiagnosticWindowOptions)
      .mapWithTimestamp(IoDiagnostic.toRaw)
      .writeUnboundedToBigQuery(DiagnosticTableIoId, config.diagnosticTable)

    val _ = sc.run()
  }
}