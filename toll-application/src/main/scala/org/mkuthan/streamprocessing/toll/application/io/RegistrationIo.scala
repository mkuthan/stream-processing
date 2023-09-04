package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration

trait RegistrationIo {
  val VehicleRegistrationTableIoId: IoIdentifier[VehicleRegistration.Raw] =
    IoIdentifier[VehicleRegistration.Raw]("toll.vehicle_registration")
  val VehicleRegistrationSubscriptionIoId: IoIdentifier[VehicleRegistration.Raw] =
    IoIdentifier[VehicleRegistration.Raw]("vehicle-registration-subscription-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier[VehicleRegistration.DeadLetterRaw] =
    IoIdentifier[VehicleRegistration.DeadLetterRaw]("vehicle-registration-dlq-bucket-id")

}
