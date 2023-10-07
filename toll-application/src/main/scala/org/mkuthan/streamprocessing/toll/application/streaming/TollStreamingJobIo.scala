package org.mkuthan.streamprocessing.toll.application.streaming

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

trait TollStreamingJobIo extends TollBoothIo with RegistrationIo with VehicleIo with DiagnosticIo

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

trait RegistrationIo {
  val VehicleRegistrationSubscriptionIoId: IoIdentifier[VehicleRegistration.Payload] =
    IoIdentifier("vehicle-registration-subscription-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier[VehicleRegistration.DeadLetterPayload] =
    IoIdentifier("vehicle-registration-dlq-bucket-id")

  val VehicleRegistrationTableIoId: IoIdentifier[VehicleRegistration.Record] =
    IoIdentifier("toll.vehicle_registration")
}

trait VehicleIo {
  val VehiclesWithExpiredRegistrationTopicIoId: IoIdentifier[VehiclesWithExpiredRegistration.Payload] =
    IoIdentifier("vehicles-with-expired-registration-topic-id")

  val VehiclesWithExpiredRegistrationDiagnosticTableIoId: IoIdentifier[TollBoothDiagnostic.Record] =
    IoIdentifier("vehicles-with-expired-registration-diagnostic-table-id")

  val TotalVehicleTimesTableIoId: IoIdentifier[TotalVehicleTimes.Record] =
    IoIdentifier("total-vehicle-times-table-id")

  val TotalVehicleTimesDiagnosticTableIoId: IoIdentifier[TollBoothDiagnostic.Record] =
    IoIdentifier("total-vehicle-times-diagnostic-table-id")
}

trait DiagnosticIo {
  val IoDiagnosticTableIoId: IoIdentifier[Diagnostic.Record] =
    IoIdentifier("diagnostic-table-id")
}
