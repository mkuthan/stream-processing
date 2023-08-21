package org.mkuthan.streamprocessing.infrastructure.pubsub

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

case class PubsubDeadLetter[T](
    id: IoIdentifier[T],
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
