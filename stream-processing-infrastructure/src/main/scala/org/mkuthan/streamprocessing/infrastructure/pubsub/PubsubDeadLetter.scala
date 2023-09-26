package org.mkuthan.streamprocessing.infrastructure.pubsub

final case class PubsubDeadLetter[T](
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
