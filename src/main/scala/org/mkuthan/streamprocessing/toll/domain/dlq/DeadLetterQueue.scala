package org.mkuthan.streamprocessing.toll.domain.dlq

import com.spotify.scio.values.SCollection

case class DeadLetterQueue()

object DeadLetterQueue {
  def encode[T](input: SCollection[T]): SCollection[DeadLetterQueue] = ???
}
