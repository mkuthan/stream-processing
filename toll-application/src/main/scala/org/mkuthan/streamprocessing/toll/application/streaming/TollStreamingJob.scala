package org.mkuthan.streamprocessing.toll.application.streaming

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
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
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetter
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction.PartitionDateRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.pubsub.JsonReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.NamedTimestampAttribute
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
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

  private val TwoDays = Duration.standardDays(2)

  private val DefaultWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
    allowedLateness = Duration.ZERO,
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  private val TollBoothStatsWindowOptions = DefaultWindowOptions.copy(
    allowedLateness = Duration.standardMinutes(2)
  )

  private val DeadLetterWindowOptions = DefaultWindowOptions.copy(
    trigger = Repeatedly.forever(
      AfterFirst.of(
        AfterWatermark.pastEndOfWindow(),
        AfterPane.elementCountAtLeast(1_000_000)
      )
    )
  )

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollStreamingJobConfig.parse(args)

    // receive toll booth entries and toll booth exists
    val (entries, entryMessagesDlq) = subscribeEntries(sc, config)
    val (exits, exitMessagesDlq) = subscribeExits(sc, config)

    // receive vehicle registrations
    val (vehicleRegistrationUpdates, vehicleRegistrationMessagesDlq) = subscribeVehicleRegistrations(sc, config)
    val vehicleRegistrationsHistory = readVehicleRegistrationsHistory(sc, config)

    val vehicleRegistrations = VehicleRegistration
      .unionHistoryWithUpdates(vehicleRegistrationsHistory, vehicleRegistrationUpdates)

    // calculate tool booth stats
    val tollBoothStatsDlq = calculateTollBoothStats(config, entries)

    // calculate total vehicle times
    val (totalVehicleTimesDlq, totalVehicleTimesDiagnosticDlq) = calculateTotalVehicleTimes(config, entries, exits)

    // calculate vehicles with expired registrations
    val vehiclesWithExpiredRegistrationDiagnosticDlq =
      calculateVehiclesWithExpiredRegistrations(config, entries, vehicleRegistrations)

    // io diagnostic
    val ioDiagnostics = IoDiagnostic.union(
      entryMessagesDlq.toDiagnostic(EntrySubscriptionIoId),
      exitMessagesDlq.toDiagnostic(ExitSubscriptionIoId),
      vehicleRegistrationMessagesDlq.toDiagnostic(VehicleRegistrationSubscriptionIoId),
      tollBoothStatsDlq.toDiagnostic(EntryStatsTableIoId),
      totalVehicleTimesDlq.toDiagnostic(TotalVehicleTimeTableIoId),
      totalVehicleTimesDiagnosticDlq.toDiagnostic(TotalVehicleTimeDiagnosticTableIoId),
      vehiclesWithExpiredRegistrationDiagnosticDlq.toDiagnostic(VehiclesWithExpiredRegistrationDiagnosticTableIoId)
    )

    val _ = IoDiagnostic
      .aggregateAndEncode(ioDiagnostics, windowDuration = TenMinutes, windowOptions = DefaultWindowOptions)
      .writeUnboundedToBigQuery(DiagnosticTableIoId, config.diagnosticTable)

    val _ = sc.run()
  }

  private def subscribeEntries(sc: ScioContext, config: TollStreamingJobConfig): (
      SCollection[TollBoothEntry],
      SCollection[PubsubDeadLetter[TollBoothEntry.Payload]]
  ) = {
    val (entryMessages, entryMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        EntrySubscriptionIoId,
        config.entrySubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(TollBoothEntry.TimestampAttribute))
      ).unzip

    val (entries, entriesDlq) = TollBoothEntry.decodeMessage(entryMessages)
    entriesDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeUnboundedToStorageAsJson(EntryDlqBucketIoId, config.entryDlq)

    (entries, entryMessagesDlq)
  }

  private def subscribeExits(sc: ScioContext, config: TollStreamingJobConfig): (
      SCollection[TollBoothExit],
      SCollection[PubsubDeadLetter[TollBoothExit.Payload]]
  ) = {
    val (exitMessages, exitMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        ExitSubscriptionIoId,
        config.exitSubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(TollBoothExit.TimestampAttribute))
      ).unzip

    val (exits, existsDlq) = TollBoothExit.decodeMessage(exitMessages)
    existsDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeUnboundedToStorageAsJson(ExitDlqBucketIoId, config.exitDlq)

    (exits, exitMessagesDlq)
  }

  private def subscribeVehicleRegistrations(sc: ScioContext, config: TollStreamingJobConfig): (
      SCollection[VehicleRegistration],
      SCollection[PubsubDeadLetter[VehicleRegistration.Payload]]
  ) = {
    val (vehicleRegistrationMessages, vehicleRegistrationMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        VehicleRegistrationSubscriptionIoId,
        config.vehicleRegistrationSubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(VehicleRegistration.TimestampAttribute))
      ).unzip

    val (vehicleRegistrations, vehicleRegistrationsDlq) =
      VehicleRegistration.decodeMessage(vehicleRegistrationMessages)

    vehicleRegistrationsDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeUnboundedToStorageAsJson(VehicleRegistrationDlqBucketIoId, config.vehicleRegistrationDlq)

    (vehicleRegistrations, vehicleRegistrationMessagesDlq)
  }

  private def readVehicleRegistrationsHistory(
      sc: ScioContext,
      config: TollStreamingJobConfig
  ): SCollection[VehicleRegistration] = {
    val partitionDate = config.effectiveDate.minusDays(1)
    val vehicleRegistrationRecords =
      sc.readFromBigQuery(
        VehicleRegistrationTableIoId,
        config.vehicleRegistrationTable,
        StorageReadConfiguration().withRowRestriction(
          PartitionDateRestriction(partitionDate)
        )
      )

    VehicleRegistration.decodeRecord(vehicleRegistrationRecords, partitionDate)
  }

  private def calculateTollBoothStats(
      config: TollStreamingJobConfig,
      entries: SCollection[TollBoothEntry]
  ): SCollection[BigQueryDeadLetter[TollBoothStats.Record]] = {
    val tollBoothStats = TollBoothStats.calculateInFixedWindow(entries, TenMinutes, TollBoothStatsWindowOptions)
    TollBoothStats
      .encode(tollBoothStats)
      .writeUnboundedToBigQuery(EntryStatsTableIoId, config.entryStatsTable)
  }

  private def calculateTotalVehicleTimes(
      config: TollStreamingJobConfig,
      entries: SCollection[TollBoothEntry],
      exits: SCollection[TollBoothExit]
  ): (
      SCollection[BigQueryDeadLetter[TotalVehicleTime.Record]],
      SCollection[BigQueryDeadLetter[TotalVehicleTimeDiagnostic.Record]]
  ) = {
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(entries, exits, TenMinutes, DefaultWindowOptions)

    // TODO: publish to Pubsub

    val totalVehicleTimesDlq = TotalVehicleTime
      .encodeRecord(totalVehicleTimes)
      .writeUnboundedToBigQuery(TotalVehicleTimeTableIoId, config.totalVehicleTimeTable)

    val totalVehicleTimeDiagnosticDlq = TotalVehicleTimeDiagnostic
      .aggregateAndEncode(totalVehicleTimesDiagnostic, TenMinutes, DefaultWindowOptions)
      .writeUnboundedToBigQuery(TotalVehicleTimeDiagnosticTableIoId, config.totalVehicleTimeDiagnosticTable)

    (totalVehicleTimesDlq, totalVehicleTimeDiagnosticDlq)
  }

  private def calculateVehiclesWithExpiredRegistrations(
      config: TollStreamingJobConfig,
      entries: SCollection[TollBoothEntry],
      vehicleRegistrations: SCollection[VehicleRegistration]
  ): SCollection[BigQueryDeadLetter[VehiclesWithExpiredRegistrationDiagnostic.Record]] = {
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculateWithTemporalJoin(
        entries,
        vehicleRegistrations,
        leftWindowDuration = TenMinutes,
        rightWindowDuration = TwoDays,
        windowOptions = DefaultWindowOptions
      )

    VehiclesWithExpiredRegistration
      .encodeMessage(vehiclesWithExpiredRegistration)
      .publishJsonToPubsub(VehiclesWithExpiredRegistrationTopicIoId, config.vehiclesWithExpiredRegistrationTopic)

    // TODO: write to BigQuery

    VehiclesWithExpiredRegistrationDiagnostic
      .aggregateAndEncode(vehiclesWithExpiredRegistrationDiagnostic, TenMinutes, DefaultWindowOptions)
      .writeUnboundedToBigQuery(
        VehiclesWithExpiredRegistrationDiagnosticTableIoId,
        config.vehiclesWithExpiredRegistrationDiagnosticTable
      )
  }
}
