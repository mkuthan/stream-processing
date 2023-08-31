package org.mkuthan.streamprocessing.toll.application.streaming

import com.spotify.scio.ContextAndArgs

import org.joda.time.Duration

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic
import org.mkuthan.streamprocessing.toll.application.config.TollApplicationConfig
import org.mkuthan.streamprocessing.toll.application.io._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

object TollApplication {

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
    val tollBoothStatsDlq = TollBoothStats
      .encode(boothStats)
      .writeUnboundedToBigQuery(EntryStatsTableIoId, config.entryStatsTable)

    // calculate total vehicle times
    val (totalVehicleTimes, totalVehicleTimesDiagnostic) =
      TotalVehicleTime.calculateInSessionWindow(boothEntries, boothExits, TenMinutes)
    val totalVehicleTimesDlq = TotalVehicleTime
      .encode(totalVehicleTimes)
      .writeUnboundedToBigQuery(TotalVehicleTimeTableIoId, config.totalVehicleTimeTable)

    totalVehicleTimesDiagnostic.writeUnboundedDiagnosticToBigQuery(
      TotalVehicleTimeDiagnosticTableIoId,
      config.totalVehicleTimeDiagnosticTable,
      TotalVehicleTimeDiagnostic.toRaw
    )

    // calculate vehicles with expired registrations
    val (vehiclesWithExpiredRegistration, vehiclesWithExpiredRegistrationDiagnostic) =
      VehiclesWithExpiredRegistration.calculateInFixedWindow(boothEntries, vehicleRegistrations, TenMinutes)
    VehiclesWithExpiredRegistration
      .encode(vehiclesWithExpiredRegistration)
      .publishJsonToPubSub(VehiclesWithExpiredRegistrationTopicIoId, config.vehiclesWithExpiredRegistrationTopic)

    vehiclesWithExpiredRegistrationDiagnostic.writeUnboundedDiagnosticToBigQuery(
      VehiclesWithExpiredRegistrationDiagnosticTableIoId,
      config.vehiclesWithExpiredRegistrationDiagnosticTable,
      VehiclesWithExpiredRegistrationDiagnostic.toRaw
    )

    // dead letters diagnostic
    val ioDiagnostics = sc.unionAll(Seq(
      boothEntriesRawDlq.toDiagnostic(),
      boothExitsRawDlq.toDiagnostic(),
      vehicleRegistrationsRawUpdatesDlq.toDiagnostic(),
      tollBoothStatsDlq.toDiagnostic(),
      totalVehicleTimesDlq.toDiagnostic()
    ))

    ioDiagnostics.writeUnboundedDiagnosticToBigQuery(
      DiagnosticTableIoId,
      config.diagnosticTable,
      IoDiagnostic.toRaw
    )

    val _ = sc.run()
  }
}
