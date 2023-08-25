package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

trait VehicleIo {
  val VehiclesWithExpiredRegistrationTopicIoId: IoIdentifier[VehiclesWithExpiredRegistration.Raw] =
    IoIdentifier[VehiclesWithExpiredRegistration.Raw]("vehicles-with-expired-registration-topic-id")

  val VehiclesWithExpiredRegistrationDiagnosticTableIoId: IoIdentifier[VehiclesWithExpiredRegistration.Diagnostic] =
    IoIdentifier[VehiclesWithExpiredRegistration.Diagnostic](
      "toll.vehicles-with-expired-registration-diagnostic-table-id"
    )

  val TotalVehicleTimeTableIoId: IoIdentifier[TotalVehicleTime.Raw] =
    IoIdentifier[TotalVehicleTime.Raw]("total-vehicle-time-table-id")

  val TotalVehicleTimeDiagnosticTableIoId: IoIdentifier[TotalVehicleTime.Diagnostic] =
    IoIdentifier[TotalVehicleTime.Diagnostic]("total-vehicle-time-diagnostic-table-id")
}