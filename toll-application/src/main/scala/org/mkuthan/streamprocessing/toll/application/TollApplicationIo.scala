package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

trait TollApplicationIo {
  val EntrySubscriptionIoId: IoIdentifier[TollBoothEntry.Raw] =
    IoIdentifier[TollBoothEntry.Raw]("entry-subscription-id")
  val EntryDlqBucketIoId: IoIdentifier[TollBoothEntry.DeadLetterRaw] =
    IoIdentifier[TollBoothEntry.DeadLetterRaw]("entry-dlq-bucket-id")

  val ExitSubscriptionIoId: IoIdentifier[TollBoothExit.Raw] =
    IoIdentifier[TollBoothExit.Raw]("exit-subscription-id")
  val ExitDlqBucketIoId: IoIdentifier[TollBoothExit.DeadLetterRaw] =
    IoIdentifier[TollBoothExit.DeadLetterRaw]("exit-dlq-bucket-id")

  val VehicleRegistrationTableIoId: IoIdentifier[VehicleRegistration.Raw] =
    IoIdentifier[VehicleRegistration.Raw]("toll.vehicle_registration")
  val VehicleRegistrationSubscriptionIoId: IoIdentifier[VehicleRegistration.Raw] =
    IoIdentifier[VehicleRegistration.Raw]("vehicle-registration-subscription-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier[VehicleRegistration.Raw] =
    IoIdentifier[VehicleRegistration.Raw]("vehicle-registration-dlq-bucket-id")

  val VehiclesWithExpiredRegistrationTopicIoId: IoIdentifier[VehiclesWithExpiredRegistration.Raw] =
    IoIdentifier[VehiclesWithExpiredRegistration.Raw]("vehicles-with-expired-registration-topic-id")

  val VehiclesWithExpiredRegistrationDiagnosticTableIoId: IoIdentifier[VehiclesWithExpiredRegistration.Diagnostic] =
    IoIdentifier[VehiclesWithExpiredRegistration.Diagnostic](
      "toll.vehicles-with-expired-registration-diagnostic-table-id"
    )

  val EntryStatsTableIoId: IoIdentifier[TollBoothStats.Raw] =
    IoIdentifier[TollBoothStats.Raw]("entry-stats-table-id")

  val TotalVehicleTimeTableIoId: IoIdentifier[TotalVehicleTime.Raw] =
    IoIdentifier[TotalVehicleTime.Raw]("total-vehicle-time-table-id")

  val TotalVehicleTimeDiagnosticTableIoId: IoIdentifier[TotalVehicleTime.Diagnostic] =
    IoIdentifier[TotalVehicleTime.Diagnostic]("total-vehicle-time-diagnostic-table-id")

  val IoDiagnosticTableIoId: IoIdentifier[IoDiagnostic.Diagnostic] =
    IoIdentifier[IoDiagnostic.Diagnostic]("io-diagnostic-table-id")
}
