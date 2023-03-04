package org.mkuthan.streamprocessing.toll.infrastructure.scio

final case class PubSubMessage[T](
    payload: T,
    attributes: Map[String, String] = Map.empty
)
