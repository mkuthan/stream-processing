package org.mkuthan.streamprocessing.toll.domain.dlq

import com.spotify.scio.values.SCollection

final case class DeadLetterQueue[T](
    payload: T
)

object DeadLetterQueue {

  // implicit val CoderCache: Coder[DeadLetterQueue] = Coder.gen

  final case class Raw()
}
