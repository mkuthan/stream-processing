package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.Args

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.PubsubSubscription
import org.mkuthan.streamprocessing.shared.scio.common.PubsubTopic
import org.mkuthan.streamprocessing.shared.scio.common.StorageBucket
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.common.IoDiagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

case class TollApplicationConfig(
    entrySubscription: PubsubSubscription[TollBoothEntry.Raw],
    entryDlq: StorageBucket[TollBoothEntry.DeadLetterRaw],
    exitSubscription: PubsubSubscription[TollBoothExit.Raw],
    exitDlq: StorageBucket[TollBoothExit.DeadLetterRaw],
    vehicleRegistrationSubscription: PubsubSubscription[VehicleRegistration.Raw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    vehicleRegistrationDlq: StorageBucket[VehicleRegistration.Raw],
    entryStatsTable: BigQueryTable[TollBoothStats.Raw],
    totalVehicleTimeTable: BigQueryTable[TotalVehicleTime.Raw],
    totalVehicleTimeDiagnosticTable: BigQueryTable[TotalVehicleTime.Diagnostic],
    vehiclesWithExpiredRegistrationTopic: PubsubTopic[VehiclesWithExpiredRegistration.Raw],
    vehiclesWithExpiredRegistrationDiagnosticTable: BigQueryTable[VehiclesWithExpiredRegistration.Diagnostic],
    ioDiagnosticTable: BigQueryTable[IoDiagnostic.Diagnostic]
)

object TollApplicationConfig {
  def parse(args: Args): TollApplicationConfig = TollApplicationConfig(
    entrySubscription = PubsubSubscription(args.required("entrySubscription")),
    entryDlq = StorageBucket(args.required("entryDlq")),
    exitSubscription = PubsubSubscription(args.required("exitSubscription")),
    exitDlq = StorageBucket(args.required("exitDlq")),
    vehicleRegistrationSubscription = PubsubSubscription(args.required("vehicleRegistrationSubscription")),
    vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
    vehicleRegistrationDlq = StorageBucket(args.required("vehicleRegistrationDlq")),
    entryStatsTable = BigQueryTable(args.required("entryStatsTable")),
    totalVehicleTimeTable = BigQueryTable(args.required("totalVehicleTimeTable")),
    totalVehicleTimeDiagnosticTable = BigQueryTable(args.required("totalVehicleTimeDiagnosticTable")),
    vehiclesWithExpiredRegistrationTopic = PubsubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
    vehiclesWithExpiredRegistrationDiagnosticTable =
      BigQueryTable(args.required("vehiclesWithExpiredRegistrationDiagnosticTable")),
    ioDiagnosticTable = BigQueryTable(args.required("ioDiagnosticTable"))
  )
}
