package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

trait VehicleIo {
  val VehiclesWithExpiredRegistrationTopicIoId: IoIdentifier[VehiclesWithExpiredRegistration.Raw] =
    IoIdentifier[VehiclesWithExpiredRegistration.Raw]("vehicles-with-expired-registration-topic-id")

  val VehiclesWithExpiredRegistrationDiagnosticTableIoId: IoIdentifier[VehiclesWithExpiredRegistrationDiagnostic.Raw] =
    IoIdentifier[VehiclesWithExpiredRegistrationDiagnostic.Raw](
      "toll.vehicles-with-expired-registration-diagnostic-table-id"
    )

  val TotalVehicleTimeTableIoId: IoIdentifier[TotalVehicleTime.Raw] =
    IoIdentifier[TotalVehicleTime.Raw]("total-vehicle-time-table-id")

  val TotalVehicleTimeDiagnosticTableIoId: IoIdentifier[TotalVehicleTimeDiagnostic.Raw] =
    IoIdentifier[TotalVehicleTimeDiagnostic.Raw]("total-vehicle-time-diagnostic-table-id")
}
