package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

trait TollApplicationIo {
  val EntrySubscriptionIoId = IoIdentifier("entry-subscription-id")
  val EntryDlqBucketIoId = IoIdentifier("entry-dlq-bucket-id")

  val ExitSubscriptionIoId = IoIdentifier("exit-subscription-id")
  val ExitDlqBucketIoId = IoIdentifier("exit-dlq-bucket-id")

  val VehicleRegistrationDlqBucketIoId = IoIdentifier("vehicle-registration-dlq-bucket-id")

  val VehiclesWithExpiredRegistrationTopicIoId = IoIdentifier("vehicles-with-expired-registration-topic-id")
}
