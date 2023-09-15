package org.mkuthan.streamprocessing.toll.application.streaming

import com.spotify.scio.Args

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubSubscription
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubTopic
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

case class TollStreamingJobConfig(
    entrySubscription: PubsubSubscription[TollBoothEntry.Payload],
    entryDlq: StorageBucket[TollBoothEntry.DeadLetterPayload],
    exitSubscription: PubsubSubscription[TollBoothExit.Payload],
    exitDlq: StorageBucket[TollBoothExit.DeadLetterPayload],
    vehicleRegistrationSubscription: PubsubSubscription[VehicleRegistration.Raw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    vehicleRegistrationDlq: StorageBucket[VehicleRegistration.DeadLetterRaw],
    entryStatsTable: BigQueryTable[TollBoothStats.Raw],
    totalVehicleTimeTable: BigQueryTable[TotalVehicleTime.Raw],
    totalVehicleTimeDiagnosticTable: BigQueryTable[TotalVehicleTimeDiagnostic.Raw],
    vehiclesWithExpiredRegistrationTopic: PubsubTopic[VehiclesWithExpiredRegistration.Raw],
    vehiclesWithExpiredRegistrationDiagnosticTable: BigQueryTable[VehiclesWithExpiredRegistrationDiagnostic.Raw],
    diagnosticTable: BigQueryTable[IoDiagnostic.Raw]
)

object TollStreamingJobConfig {
  def parse(args: Args): TollStreamingJobConfig = TollStreamingJobConfig(
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
    diagnosticTable = BigQueryTable(args.required("diagnosticTable"))
  )
}
