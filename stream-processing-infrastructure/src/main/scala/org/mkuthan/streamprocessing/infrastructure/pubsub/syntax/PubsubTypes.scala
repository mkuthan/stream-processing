package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.shared.common.Message

private[syntax] trait PubsubTypes {
  type PubsubResult[T] = Either[PubsubDeadLetter[T], Message[T]]
}
