package org.mkuthan.streamprocessing.shared.scio.pubsub

final case class PubsubDeadLetter[T](
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
