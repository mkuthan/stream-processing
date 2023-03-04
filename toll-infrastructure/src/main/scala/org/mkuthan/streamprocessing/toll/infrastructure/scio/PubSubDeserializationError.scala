package org.mkuthan.streamprocessing.toll.infrastructure.scio

final case class PubSubDeserializationError[T](
    payload: Array[Byte],
    attributes: Map[String, String],
    exception: Throwable
)
