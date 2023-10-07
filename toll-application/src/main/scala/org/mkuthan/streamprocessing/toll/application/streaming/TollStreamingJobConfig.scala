package org.mkuthan.streamprocessing.toll.application.streaming

import com.spotify.scio.Args

import org.joda.time.LocalDate

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubSubscription
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubTopic
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

final case class TollStreamingJobConfig(
    effectiveDate: LocalDate,
    entrySubscription: PubsubSubscription[TollBoothEntry.Payload],
    entryDlq: StorageBucket[TollBoothEntry.DeadLetterPayload],
    exitSubscription: PubsubSubscription[TollBoothExit.Payload],
    exitDlq: StorageBucket[TollBoothExit.DeadLetterPayload],
    vehicleRegistrationSubscription: PubsubSubscription[VehicleRegistration.Payload],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Record],
    vehicleRegistrationDlq: StorageBucket[VehicleRegistration.DeadLetterPayload],
    entryStatsTable: BigQueryTable[TollBoothStats.Record],
    totalVehicleTimesTable: BigQueryTable[TotalVehicleTimes.Record],
    totalVehicleTimesDiagnosticTable: BigQueryTable[TollBoothDiagnostic.Record],
    vehiclesWithExpiredRegistrationTopic: PubsubTopic[VehiclesWithExpiredRegistration.Payload],
    vehiclesWithExpiredRegistrationDiagnosticTable: BigQueryTable[TollBoothDiagnostic.Record],
    ioDiagnosticTable: BigQueryTable[Diagnostic.Record]
)

object TollStreamingJobConfig {
  def parse(args: Args): TollStreamingJobConfig = {
    val effectiveDate = args.optional("effectiveDate").map(LocalDate.parse).getOrElse(LocalDate.now())

    TollStreamingJobConfig(
      effectiveDate = effectiveDate,
      entrySubscription = PubsubSubscription(args.required("entrySubscription")),
      entryDlq = StorageBucket(args.required("entryDlq")),
      exitSubscription = PubsubSubscription(args.required("exitSubscription")),
      exitDlq = StorageBucket(args.required("exitDlq")),
      vehicleRegistrationSubscription = PubsubSubscription(args.required("vehicleRegistrationSubscription")),
      vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
      vehicleRegistrationDlq = StorageBucket(args.required("vehicleRegistrationDlq")),
      entryStatsTable = BigQueryTable(args.required("entryStatsTable")),
      totalVehicleTimesTable = BigQueryTable(args.required("totalVehicleTimesTable")),
      totalVehicleTimesDiagnosticTable = BigQueryTable(args.required("totalVehicleTimesDiagnosticTable")),
      vehiclesWithExpiredRegistrationTopic = PubsubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
      vehiclesWithExpiredRegistrationDiagnosticTable =
        BigQueryTable(args.required("vehiclesWithExpiredRegistrationDiagnosticTable")),
      ioDiagnosticTable = BigQueryTable(args.required("ioDiagnosticTable"))
    )
  }
}
