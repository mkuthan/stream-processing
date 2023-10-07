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
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction.PartitionDateRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.JsonReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.NamedTimestampAttribute
import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

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

    val (entries, entriesIoDiagnostic) = getEntries(sc, config)
    val (exits, exitsIoDiagnostic) = getExits(sc, config)
    val (vehicleRegistrations, vehicleRegistrationsIoDiagnostic) = getVehicleRegistrations(sc, config)

    val tollBoothStatsIoDiagnostic = calculateTollBoothStats(config, entries)
    val totalVehicleTimesIoDiagnostic = calculateTotalVehicleTimes(config, entries, exits)
    val vehiclesWithExpiredRegistrationsIoDiagnostic =
      calculateVehiclesWithExpiredRegistrations(config, entries, vehicleRegistrations)

    val ioDiagnostics = sc.unionInGlobalWindow(
      entriesIoDiagnostic,
      exitsIoDiagnostic,
      vehicleRegistrationsIoDiagnostic,
      tollBoothStatsIoDiagnostic,
      totalVehicleTimesIoDiagnostic,
      vehiclesWithExpiredRegistrationsIoDiagnostic
    )

    val _ = Diagnostic
      .aggregateAndEncodeRecord(ioDiagnostics, windowDuration = TenMinutes, windowOptions = DefaultWindowOptions)
      .writeUnboundedToBigQuery(IoDiagnosticTableIoId, config.ioDiagnosticTable)

    val _ = sc.run()
  }

  private def getEntries(
      sc: ScioContext,
      config: TollStreamingJobConfig
  ): (SCollection[TollBoothEntry], SCollection[Diagnostic]) = {
    val (entryMessages, entryMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        EntrySubscriptionIoId,
        config.entrySubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(TollBoothEntry.TimestampAttribute))
      ).unzip

    val (entries, entriesDlq) = TollBoothEntry.decodeMessage(entryMessages)
    entriesDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeToStorageAsJson(EntryDlqBucketIoId, config.entryDlq)

    val ioDiagnostic = entryMessagesDlq.toDiagnostic(EntrySubscriptionIoId)

    (entries, ioDiagnostic)
  }

  private def getExits(
      sc: ScioContext,
      config: TollStreamingJobConfig
  ): (SCollection[TollBoothExit], SCollection[Diagnostic]) = {
    val (exitMessages, exitMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        ExitSubscriptionIoId,
        config.exitSubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(TollBoothExit.TimestampAttribute))
      ).unzip

    val (exits, existsDlq) = TollBoothExit.decodeMessage(exitMessages)
    existsDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeToStorageAsJson(ExitDlqBucketIoId, config.exitDlq)

    val ioDiagnostic = exitMessagesDlq.toDiagnostic(ExitSubscriptionIoId)

    (exits, ioDiagnostic)
  }

  private def getVehicleRegistrations(
      sc: ScioContext,
      config: TollStreamingJobConfig
  ): (SCollection[VehicleRegistration], SCollection[Diagnostic]) = {
    val (vehicleRegistrationMessages, vehicleRegistrationMessagesDlq) =
      sc.subscribeJsonFromPubsub(
        VehicleRegistrationSubscriptionIoId,
        config.vehicleRegistrationSubscription,
        JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute(VehicleRegistration.TimestampAttribute))
      ).unzip

    val (vehicleRegistrationUpdates, vehicleRegistrationUpdatesDlq) =
      VehicleRegistration.decodeMessage(vehicleRegistrationMessages)

    vehicleRegistrationUpdatesDlq
      .withFixedWindows(duration = TenMinutes, options = DeadLetterWindowOptions)
      .writeToStorageAsJson(VehicleRegistrationDlqBucketIoId, config.vehicleRegistrationDlq)

    val partitionDate = config.effectiveDate.minusDays(1)
    val vehicleRegistrationRecords =
      sc.readFromBigQuery(
        VehicleRegistrationTableIoId,
        config.vehicleRegistrationTable,
        StorageReadConfiguration().withRowRestriction(
          PartitionDateRestriction(partitionDate)
        )
      )

    val vehicleRegistrationsHistory = VehicleRegistration
      .decodeRecord(vehicleRegistrationRecords, partitionDate)

    val vehicleRegistrations = VehicleRegistration
      .unionHistoryWithUpdates(vehicleRegistrationsHistory, vehicleRegistrationUpdates)

    val ioDiagnostic = vehicleRegistrationMessagesDlq.toDiagnostic(VehicleRegistrationSubscriptionIoId)

    (vehicleRegistrations, ioDiagnostic)
  }

  private def calculateTollBoothStats(
      config: TollStreamingJobConfig,
      entries: SCollection[TollBoothEntry]
  ): SCollection[Diagnostic] = {
    val tollBoothStats = TollBoothStats.calculateInFixedWindow(entries, TenMinutes, TollBoothStatsWindowOptions)
    val tollBoothStatsDlq = TollBoothStats
      .encodeRecord(tollBoothStats)
      .writeUnboundedToBigQuery(EntryStatsTableIoId, config.entryStatsTable)

    tollBoothStatsDlq.toDiagnostic(EntryStatsTableIoId)
  }

  private def calculateTotalVehicleTimes(
      config: TollStreamingJobConfig,
      entries: SCollection[TollBoothEntry],
      exits: SCollection[TollBoothExit]
  ): (SCollection[Diagnostic]) = {
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTimes.calculateInSessionWindow(entries, exits, TenMinutes, DefaultWindowOptions)

    val totalVehicleTimesDlq = TotalVehicleTimes
      .encodeRecord(totalVehicleTimes)
      .writeUnboundedToBigQuery(TotalVehicleTimesTableIoId, config.totalVehicleTimesTable)

    val totalVehicleTimesDiagnosticDlq = TollBoothDiagnostic
      .aggregateAndEncodeRecord(totalVehicleTimesDiagnostic, TenMinutes, DefaultWindowOptions)
      .writeUnboundedToBigQuery(TotalVehicleTimesDiagnosticTableIoId, config.totalVehicleTimesDiagnosticTable)

    totalVehicleTimesDlq
      .toDiagnostic(TotalVehicleTimesTableIoId)
      .unionInGlobalWindow(
        totalVehicleTimesDiagnosticDlq.toDiagnostic(TotalVehicleTimesDiagnosticTableIoId)
      )
  }

  private def calculateVehiclesWithExpiredRegistrations(
      config: TollStreamingJobConfig,
      entries: SCollection[TollBoothEntry],
      vehicleRegistrations: SCollection[VehicleRegistration]
  ): SCollection[Diagnostic] = {
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

    val vehiclesWithExpiredRegistrationsDiagnosticDlq = TollBoothDiagnostic
      .aggregateAndEncodeRecord(vehiclesWithExpiredRegistrationDiagnostic, TenMinutes, DefaultWindowOptions)
      .writeUnboundedToBigQuery(
        VehiclesWithExpiredRegistrationDiagnosticTableIoId,
        config.vehiclesWithExpiredRegistrationDiagnosticTable
      )

    vehiclesWithExpiredRegistrationsDiagnosticDlq.toDiagnostic(VehiclesWithExpiredRegistrationDiagnosticTableIoId)
  }
}
