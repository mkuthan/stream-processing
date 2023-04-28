package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

final case class PubsubMessage[T](
    payload: T,
    attributes: Map[String, String]
)
