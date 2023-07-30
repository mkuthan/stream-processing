package org.mkuthan.streamprocessing.shared.scio.pubsub

import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

case class PubsubDeadLetter[T](
    id: IoIdentifier[T],
    payload: Array[Byte],
    attributes: Map[String, String],
    error: String
)
