package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.values.SCollection
import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.pubsub.PubsubMessage
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.common.IoDiagnostic
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
object TollApplication extends TollApplicationIo {

  private val TenMinutes = Duration.standardMinutes(10)

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollApplicationConfig.parse(args)

    // receive toll booth entries and toll booth exists
    val (boothEntriesRaw, boothEntriesRawDlq) =
      sc.subscribeJsonFromPubsub(EntrySubscriptionIoId, config.entrySubscription)

    val (boothEntries, boothEntriesDlq) = TollBoothEntry.decode(boothEntriesRaw)
    boothEntriesDlq.writeDeadLetterToStorageAsJson(EntryDlqBucketIoId, config.entryDlq)

    val (boothExitsRaw, boothExitsRawDlq) =
      sc.subscribeJsonFromPubsub(ExitSubscriptionIoId, config.exitSubscription)

    val (boothExits, boothExistsDlq) = TollBoothExit.decode(boothExitsRaw)
    boothExistsDlq.writeDeadLetterToStorageAsJson(ExitDlqBucketIoId, config.exitDlq)

    // receive vehicle registrations
    val (vehicleRegistrationsRawUpdates, vehicleRegistrationsRawUpdatesDlq) =
      sc.subscribeJsonFromPubsub(VehicleRegistrationSubscriptionIoId, config.vehicleRegistrationSubscription)

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
      .writeUnboundedToBigQuery(TotalVehicleTimeTableIoId, config.totalVehicleTimeTable)
    totalVehicleTimesDiagnostic.writeDiagnosticToBigQuery(
      TotalVehicleTimeDiagnosticTableIoId,
      config.totalVehicleTimeDiagnosticTable
    )

    // calculate vehicles with expired registrations
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculateInFixedWindow(boothEntries, vehicleRegistrations, TenMinutes)
    VehiclesWithExpiredRegistration
      .encode(vehiclesWithExpiredRegistration)
      .map(p => PubsubMessage(p)) // TODO: move to encode
      .publishJsonToPubSub(VehiclesWithExpiredRegistrationTopicIoId, config.vehiclesWithExpiredRegistrationTopic)

    vehiclesWithExpiredRegistrationDiagnostic.writeDiagnosticToBigQuery(
      VehiclesWithExpiredRegistrationDiagnosticTableIoId,
      config.vehiclesWithExpiredRegistrationDiagnosticTable
    )

    // io diagnostic, TODO: encapsulate and remove code duplication
    SCollection
      .unionAll(
        Seq(
          boothEntriesRawDlq.map(x => IoDiagnostic(x.id, x.error)),
          boothExitsRawDlq.map(x => IoDiagnostic(x.id, x.error)),
          vehicleRegistrationsRawUpdatesDlq.map(x => IoDiagnostic(x.id, x.error))
        )
      )
      .keyBy(_.key)
      .writeDiagnosticToBigQuery(IoDiagnosticTableIoId, config.ioDiagnosticTable)

    val _ = sc.run()
  }
}
