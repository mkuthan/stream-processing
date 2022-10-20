package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.Args
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.ScioContext
import org.joda.time.Duration
import org.mkuthan.streamprocessing.toll.configuration.TollApplicationConfiguration
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.dlq.DeadLetterQueue
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.infrastructure.BigQueryRepository
import org.mkuthan.streamprocessing.toll.infrastructure.PubSubRepository
import org.mkuthan.streamprocessing.toll.infrastructure.StorageRepository

/** A toll station is a common phenomenon. You encounter them on many expressways, bridges, and tunnels across the world. Each toll station has
  * multiple toll booths. At manual booths, you stop to pay the toll to an attendant. At automated booths, a sensor on top of each booth scans an RFID
  * card that's affixed to the windshield of your vehicle as you pass the toll booth. It is easy to visualize the passage of vehicles through these
  * toll stations as an event stream over which interesting operations can be performed.
  *
  * See: https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-build-an-iot-solution-using-stream-analytics
  */
object TollApplication {

  private val TenMinutes = Duration.standardMinutes(10)

  def main(mainArgs: Array[String]): Unit = {
    implicit val (sc: ScioContext, args: Args) = ContextAndArgs(mainArgs)

    val configuration = TollApplicationConfiguration.parse(args)

    val boothEntriesRaw = PubSubRepository.subscribe(configuration.entrySubscription)
    val boothExitsRaw = PubSubRepository.subscribe(configuration.exitSubscription)
    val vehicleRegistrationsRaw = BigQueryRepository.load(configuration.vehicleRegistrationTable)

    val (boothEntries, boothEntriesDlq) = TollBoothEntry.decode(boothEntriesRaw)
    val (boothExits, boothExistsDlq) = TollBoothExit.decode(boothExitsRaw)
    val (vehicleRegistrations, vehicleRegistrationsDlq) = VehicleRegistration.decode(vehicleRegistrationsRaw)

    val boothEntryStats = TollBoothEntryStats.calculateInFixedWindow(boothEntries, TenMinutes)
    BigQueryRepository.save(configuration.entryStatsTable, TollBoothEntryStats.encode(boothEntryStats))

    val (tollTotalCarTimes, totalCarTimesDiagnostic) = TotalCarTime.calculate(boothEntries, boothExits)
    BigQueryRepository.save(configuration.carTotalTimeTable, TotalCarTime.encode(tollTotalCarTimes))

    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculate(boothEntries, vehicleRegistrations)
    PubSubRepository.publish(
      configuration.vehiclesWithExpiredRegistrationTopic,
      VehiclesWithExpiredRegistration.encode(vehiclesWithExpiredRegistration)
    )

    val dlqs = sc.unionAll(Seq(
      DeadLetterQueue.encode(boothEntriesDlq),
      DeadLetterQueue.encode(boothExistsDlq),
      DeadLetterQueue.encode(vehicleRegistrationsDlq)
    ))
    StorageRepository.save(configuration.dlqBucket, DeadLetterQueue.encode(dlqs))

    val diagnostics = Diagnostic.aggregateInFixedWindow(Seq(totalCarTimesDiagnostic, vehiclesWithExpiredRegistrationDiagnostic), TenMinutes)
    BigQueryRepository.save(configuration.diagnosticTable, Diagnostic.encode(diagnostics))
  }
}
