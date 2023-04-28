package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

trait TollApplicationIo {
  val EntrySubscriptionIoId: IoIdentifier = IoIdentifier("entry-subscription-id")
  val EntryDlqBucketIoId: IoIdentifier = IoIdentifier("entry-dlq-bucket-id")

  val ExitSubscriptionIoId: IoIdentifier = IoIdentifier("exit-subscription-id")
  val ExitDlqBucketIoId: IoIdentifier = IoIdentifier("exit-dlq-bucket-id")

  val VehicleRegistrationDlqBucketIoId: IoIdentifier = IoIdentifier("vehicle-registration-dlq-bucket-id")

  val VehiclesWithExpiredRegistrationTopicIoId = IoIdentifier("vehicles-with-expired-registration-topic-id")
}
