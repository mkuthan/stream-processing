package org.mkuthan.streamprocessing.shared.scio.pubsub

final case class PubsubMessage[T](
    payload: T,
    attributes: Map[String, String]
)
