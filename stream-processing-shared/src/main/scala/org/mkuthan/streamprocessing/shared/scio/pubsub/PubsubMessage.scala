package org.mkuthan.streamprocessing.shared.scio.pubsub

case class PubsubMessage[T](
    payload: T,
    attributes: Map[String, String] = Map.empty
)
