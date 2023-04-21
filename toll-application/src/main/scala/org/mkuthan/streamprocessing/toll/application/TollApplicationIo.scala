package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.toll.infrastructure.scio.common.IoIdentifier

trait TollApplicationIo {
  val EntrySubscriptionIoId = IoIdentifier("entry-subscription-id")
  val ExitSubscriptionIoId = IoIdentifier("exit-subscription-id")
  val VehiclesWithExpiredRegistrationTopicIoId = IoIdentifier("vehicles-with-expired-registration-topic-id")
}
