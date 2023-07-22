package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

trait TollApplicationIo {
  val EntrySubscriptionIoId: IoIdentifier = IoIdentifier("entry-subscription-id")
  val EntryDlqBucketIoId: IoIdentifier = IoIdentifier("entry-dlq-bucket-id")

  val ExitSubscriptionIoId: IoIdentifier = IoIdentifier("exit-subscription-id")
  val ExitDlqBucketIoId: IoIdentifier = IoIdentifier("exit-dlq-bucket-id")

  val VehicleRegistrationTableIoId = IoIdentifier("toll.vehicle_registration")
  val VehicleRegistrationSubscriptionIoId = IoIdentifier("vehicle-registration-subscription-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier = IoIdentifier("vehicle-registration-dlq-bucket-id")

  val VehiclesWithExpiredRegistrationTopicIoId = IoIdentifier("vehicles-with-expired-registration-topic-id")

  val EntryStatsTableIoId = IoIdentifier("toll.entry_stats")

  val TotalVehicleTimeTableIoId = IoIdentifier("toll.total_vehicle_time")

  val DiagnosticTableIoId = IoIdentifier("toll.diagnostic")
}
