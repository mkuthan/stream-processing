package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

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

    sc.initCounter(
      TollBoothEntryRawInvalidRows.counter,
      TollBoothExitRawInvalidRows.counter,
      VehicleRegistrationRawInvalidRows.counter
    )

    // receive toll booth entries and toll booth exists
    val (boothEntriesRaw, boothEntriesRawDlq) =
      sc.subscribeJsonFromPubsub(EntrySubscriptionIoId, config.entrySubscription)
    boothEntriesRawDlq.metrics(TollBoothEntryRawInvalidRows)

    val (boothEntries, boothEntriesDlq) = TollBoothEntry.decode(boothEntriesRaw)
    boothEntriesDlq.writeDeadLetterToStorageAsJson(EntryDlqBucketIoId, config.entryDlq)

    val (boothExitsRaw, boothExitsRawDlq) =
      sc.subscribeJsonFromPubsub(ExitSubscriptionIoId, config.exitSubscription)
    boothExitsRawDlq.metrics(TollBoothExitRawInvalidRows)

    val (boothExits, boothExistsDlq) = TollBoothExit.decode(boothExitsRaw)
    boothExistsDlq.writeDeadLetterToStorageAsJson(ExitDlqBucketIoId, config.exitDlq)

    // receive vehicle registrations
    val (vehicleRegistrationsRawUpdates, vehicleRegistrationsRawUpdatesDlq) =
      sc.subscribeJsonFromPubsub(VehicleRegistrationSubscriptionIoId, config.vehicleRegistrationSubscription)
    vehicleRegistrationsRawUpdatesDlq.metrics(VehicleRegistrationRawInvalidRows)

    val vehicleRegistrationsRawHistory =
      sc.readFromBigQuery(VehicleRegistrationTableIoId, config.vehicleRegistrationTable)

    val vehicleRegistrationsRaw =
      VehicleRegistration.unionHistoryWithUpdates(vehicleRegistrationsRawHistory, vehicleRegistrationsRawUpdates)

    val (vehicleRegistrations, vehicleRegistrationsDlq) = VehicleRegistration.decode(vehicleRegistrationsRaw)
    vehicleRegistrationsDlq.writeDeadLetterToStorageAsJson(
      VehicleRegistrationDlqBucketIoId,
      config.vehicleRegistrationDlq
    )

    // calculate tool booth stats
    val boothStats = TollBoothStats.calculateInFixedWindow(boothEntries, TenMinutes)
    TollBoothStats
      .encode(boothStats)
      .writeUnboundedToBigQuery(EntryStatsTableIoId, config.entryStatsTable)

    // calculate total vehicle times
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(boothEntries, boothExits, TenMinutes)
    TotalVehicleTime
      .encode(totalVehicleTimes)
      .writeUnboundedToBigQuery(TotalVehicleTimeTableIoId, config.carTotalTimeTable)
    totalVehicleTimesDiagnostic.writeDiagnosticToBigQuery(
      TotalVehicleTimeDiagnosticTableIoId,
      config.carTotalTimeDiagnosticTable,
      TotalVehicleTime.DiagnosticSemigroup
    )

    // calculate vehicles with expired registrations
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculate(boothEntries, vehicleRegistrations)
    VehiclesWithExpiredRegistration
      .encode(vehiclesWithExpiredRegistration)
      .publishJsonToPubSub(VehiclesWithExpiredRegistrationTopicIoId, config.vehiclesWithExpiredRegistrationTopic)

    vehiclesWithExpiredRegistrationDiagnostic.writeDiagnosticToBigQuery(
      VehiclesWithExpiredRegistrationDiagnosticTableIoId,
      config.vehiclesWithExpiredRegistrationDiagnosticTable,
      VehiclesWithExpiredRegistration.DiagnosticSemigroup
    )

    val _ = sc.run()
  }
}
