package org.mkuthan.streamprocessing.toll.domain.dlq

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

final case class DeadLetterQueue()

object DeadLetterQueue {
  @BigQueryType.toTable
  final case class Raw()

  def encode[T](input: SCollection[T]): SCollection[Raw] = ???
}
