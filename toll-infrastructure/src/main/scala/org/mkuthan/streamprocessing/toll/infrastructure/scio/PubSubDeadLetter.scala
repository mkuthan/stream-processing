package org.mkuthan.streamprocessing.toll.infrastructure.scio

final case class PubSubDeadLetter[T](
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
