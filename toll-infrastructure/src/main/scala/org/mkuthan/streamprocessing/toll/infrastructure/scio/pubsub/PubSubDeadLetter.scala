package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

final case class PubSubDeadLetter[T](
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
