package org.mkuthan.streamprocessing.toll.shared.configuration

final case class PubSubSubscription[T](
    subscription: String,
    idAttribute: Option[String],
    tsAttribute: Option[String]
)
