package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.infrastructure.scio._
import org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub.PubSubMessage

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
object TollApplication {

  private val TenMinutes = Duration.standardMinutes(10)

  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollApplicationConfig.parse(args)

    // TODO: handle deserialization errors
    val (boothEntriesRaw, _) = sc.subscribeJsonFromPubSub(config.entrySubscription)
    val (boothEntries, boothEntriesDlq) = TollBoothEntry.decode(boothEntriesRaw.extractPayload)
    boothEntriesDlq.saveToStorageAsJson(config.entryDlq)

    // TODO: handle deserialization errors
    val (boothExitsRaw, _) = sc.subscribeJsonFromPubSub(config.exitSubscription)
    val (boothExits, boothExistsDlq) = TollBoothExit.decode(boothExitsRaw.extractPayload)
    boothExistsDlq.saveToStorageAsJson(config.exitDlq)

    val (vehicleRegistrations, vehicleRegistrationsDlq) = VehicleRegistration
      .decode(sc.loadFromBigQuery(config.vehicleRegistrationTable))
    vehicleRegistrationsDlq.saveToStorageAsJson(config.vehicleRegistrationDlq)

    val boothEntryStats = TollBoothEntryStats.calculateInFixedWindow(boothEntries, TenMinutes)
    TollBoothEntryStats
      .encode(boothEntryStats)
      .saveToBigQuery(config.entryStatsTable)

    val (tollTotalCarTimes, totalCarTimesDiagnostic) =
      TotalCarTime.calculateInSessionWindow(boothEntries, boothExits, TenMinutes)
    TotalCarTime
      .encode(tollTotalCarTimes)
      .saveToBigQuery(config.carTotalTimeTable)

    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculate(boothEntries, vehicleRegistrations)
    VehiclesWithExpiredRegistration
      .encode(vehiclesWithExpiredRegistration)
      .map(PubSubMessage(_, Map.empty)) // TODO: encapsulate somewhere
      .publishJsonToPubSub(config.vehiclesWithExpiredRegistrationTopic)

    val diagnostics = Diagnostic.unionInGlobalWindow(totalCarTimesDiagnostic, vehiclesWithExpiredRegistrationDiagnostic)
    val diagnosticsAggregated = Diagnostic.aggregateInFixedWindow(diagnostics, TenMinutes)
    Diagnostic
      .encode(diagnosticsAggregated)
      .saveToBigQuery(config.diagnosticTable)

    val _ = sc.run()
  }
}
