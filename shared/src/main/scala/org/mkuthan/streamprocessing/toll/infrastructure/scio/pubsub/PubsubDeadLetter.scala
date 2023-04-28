package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

final case class PubsubDeadLetter[T](
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
