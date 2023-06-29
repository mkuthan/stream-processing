package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration

/**
 * A toll station is a common phenomenon. You encounter them on many expressways, bridges, and tunnels across the world.
 * Each toll station has multiple toll booths. At manual booths, you stop to pay the toll to an attendant. At automated
 * booths, a sensor on top of each booth scans an RFID card that's affixed to the windshield of your vehicle as you pass
 * the toll booth. It is easy to visualize the passage of vehicles through these toll stations as an event stream over
 * which interesting operations can be performed.
 *
 * See:
 * https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-build-an-iot-solution-using-stream-analytics
 */
object TollApplication extends TollApplicationIo with TollApplicationMetrics {

  private val TenMinutes = Duration.standardMinutes(10)

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollApplicationConfig.parse(args)

    sc.initCounter(TollBoothEntryRawInvalidRows.counter, TollBoothExitRawInvalidRows.counter)
    sc.initCounter()

    val (boothEntriesRaw, boothEntriesRawDlq) =
      sc.subscribeJsonFromPubsub(EntrySubscriptionIoId, config.entrySubscription)
    boothEntriesRawDlq.metrics(TollBoothEntryRawInvalidRows)

    val (boothEntries, boothEntriesDlq) = TollBoothEntry.decode(boothEntriesRaw)
    boothEntriesDlq.saveToStorageAsJson(EntryDlqBucketIoId, config.entryDlq)

    val (boothExitsRaw, boothExitsRawDlq) = sc.subscribeJsonFromPubsub(ExitSubscriptionIoId, config.exitSubscription)
    boothExitsRawDlq.metrics(TollBoothExitRawInvalidRows)

    val (boothExits, boothExistsDlq) = TollBoothExit.decode(boothExitsRaw)
    boothExistsDlq.saveToStorageAsJson(ExitDlqBucketIoId, config.exitDlq)

    val (vehicleRegistrations, vehicleRegistrationsDlq) = VehicleRegistration
      .decode(sc.readFromBigQuery(VehicleRegistrationTableIoId, config.vehicleRegistrationTable))
    vehicleRegistrationsDlq.saveToStorageAsJson(VehicleRegistrationDlqBucketIoId, config.vehicleRegistrationDlq)

    val boothEntryStats = TollBoothEntryStats.calculateInFixedWindow(boothEntries, TenMinutes)
    TollBoothEntryStats
      .encode(boothEntryStats)
      .writeToBigQuery(EntryStatsTableIoId, config.entryStatsTable)

    val (tollTotalCarTimes, totalCarTimesDiagnostic) =
      TotalCarTime.calculateInSessionWindow(boothEntries, boothExits, TenMinutes)
    TotalCarTime
      .encode(tollTotalCarTimes)
      .writeToBigQuery(CarTotalTimeTableIoId, config.carTotalTimeTable)

    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculate(boothEntries, vehicleRegistrations)
    VehiclesWithExpiredRegistration
      .encode(vehiclesWithExpiredRegistration)
      .publishJsonToPubSub(VehiclesWithExpiredRegistrationTopicIoId, config.vehiclesWithExpiredRegistrationTopic)

    val diagnostics = Diagnostic.unionInGlobalWindow(totalCarTimesDiagnostic, vehiclesWithExpiredRegistrationDiagnostic)
    val diagnosticsAggregated = Diagnostic.aggregateInFixedWindow(diagnostics, TenMinutes)
    Diagnostic
      .encode(diagnosticsAggregated)
      .writeToBigQuery(DiagnosticTableIoId, config.diagnosticTable)

    val _ = sc.run()
  }
}
