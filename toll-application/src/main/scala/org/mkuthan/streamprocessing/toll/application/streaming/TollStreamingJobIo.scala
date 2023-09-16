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
  val DiagnosticTableIoId: IoIdentifier[IoDiagnostic.Raw] =
    IoIdentifier[IoDiagnostic.Raw]("diagnostic-table-id")
}

trait RegistrationIo {
  val VehicleRegistrationTableIoId: IoIdentifier[VehicleRegistration.Record] =
    IoIdentifier[VehicleRegistration.Record]("toll.vehicle_registration")
  val VehicleRegistrationSubscriptionIoId: IoIdentifier[VehicleRegistration.Record] =
    IoIdentifier[VehicleRegistration.Record]("vehicle-registration-subscription-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier[VehicleRegistration.DeadLetterRecord] =
    IoIdentifier[VehicleRegistration.DeadLetterRecord]("vehicle-registration-dlq-bucket-id")
}

trait TollBoothIo {
  val EntrySubscriptionIoId: IoIdentifier[TollBoothEntry.Payload] =
    IoIdentifier[TollBoothEntry.Payload]("entry-subscription-id")
  val EntryDlqBucketIoId: IoIdentifier[TollBoothEntry.DeadLetterPayload] =
    IoIdentifier[TollBoothEntry.DeadLetterPayload]("entry-dlq-bucket-id")

  val ExitSubscriptionIoId: IoIdentifier[TollBoothExit.Payload] =
    IoIdentifier[TollBoothExit.Payload]("exit-subscription-id")
  val ExitDlqBucketIoId: IoIdentifier[TollBoothExit.DeadLetterPayload] =
    IoIdentifier[TollBoothExit.DeadLetterPayload]("exit-dlq-bucket-id")

  val EntryStatsTableIoId: IoIdentifier[TollBoothStats.Record] =
    IoIdentifier[TollBoothStats.Record]("entry-stats-table-id")
}

trait VehicleIo {
  val VehiclesWithExpiredRegistrationTopicIoId: IoIdentifier[VehiclesWithExpiredRegistration.Record] =
    IoIdentifier[VehiclesWithExpiredRegistration.Record]("vehicles-with-expired-registration-topic-id")

  val VehiclesWithExpiredRegistrationDiagnosticTableIoId
      : IoIdentifier[VehiclesWithExpiredRegistrationDiagnostic.Record] =
    IoIdentifier[VehiclesWithExpiredRegistrationDiagnostic.Record](
      "vehicles-with-expired-registration-diagnostic-table-id"
    )

  val TotalVehicleTimeTableIoId: IoIdentifier[TotalVehicleTime.Record] =
    IoIdentifier[TotalVehicleTime.Record]("total-vehicle-time-table-id")

  val TotalVehicleTimeDiagnosticTableIoId: IoIdentifier[TotalVehicleTimeDiagnostic.Record] =
    IoIdentifier[TotalVehicleTimeDiagnostic.Record]("total-vehicle-time-diagnostic-table-id")
}
