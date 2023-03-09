package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

final case class PubSubMessage[T](
    payload: T,
    attributes: Map[String, String]
)
