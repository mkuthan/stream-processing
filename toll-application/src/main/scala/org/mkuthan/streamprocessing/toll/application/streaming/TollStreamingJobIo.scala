package org.mkuthan.streamprocessing.toll.application.streaming

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

trait TollStreamingJobIo extends DiagnosticIo with RegistrationIo with TollBoothIo with VehicleIo

trait DiagnosticIo {
  val DiagnosticTableIoId: IoIdentifier[IoDiagnostic.Record] =
    IoIdentifier("diagnostic-table-id")
}

trait RegistrationIo {
  val VehicleRegistrationTableIoId: IoIdentifier[VehicleRegistration.Record] =
    IoIdentifier("toll.vehicle_registration")
  val VehicleRegistrationSubscriptionIoId: IoIdentifier[VehicleRegistration.Payload] =
    IoIdentifier("vehicle-registration-subscription-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier[VehicleRegistration.DeadLetterPayload] =
    IoIdentifier("vehicle-registration-dlq-bucket-id")
}

trait TollBoothIo {
  val EntrySubscriptionIoId: IoIdentifier[TollBoothEntry.Payload] =
    IoIdentifier("entry-subscription-id")
  val EntryDlqBucketIoId: IoIdentifier[TollBoothEntry.DeadLetterPayload] =
    IoIdentifier("entry-dlq-bucket-id")

  val ExitSubscriptionIoId: IoIdentifier[TollBoothExit.Payload] =
    IoIdentifier("exit-subscription-id")
  val ExitDlqBucketIoId: IoIdentifier[TollBoothExit.DeadLetterPayload] =
    IoIdentifier("exit-dlq-bucket-id")

  val EntryStatsTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier("entry-stats-table-id")
}

trait VehicleIo {
  val VehiclesWithExpiredRegistrationTopicIoId: IoIdentifier[VehiclesWithExpiredRegistration.Payload] =
    IoIdentifier("vehicles-with-expired-registration-topic-id")

  val VehiclesWithExpiredRegistrationDiagnosticTableIoId
      : IoIdentifier[VehiclesWithExpiredRegistrationDiagnostic.Record] =
    IoIdentifier("vehicles-with-expired-registration-diagnostic-table-id")

  val TotalVehicleTimeTableIoId: IoIdentifier[TotalVehicleTime.Record] =
    IoIdentifier("total-vehicle-time-table-id")

  val TotalVehicleTimeDiagnosticTableIoId: IoIdentifier[TotalVehicleTimeDiagnostic.Record] =
    IoIdentifier("total-vehicle-time-diagnostic-table-id")
}
