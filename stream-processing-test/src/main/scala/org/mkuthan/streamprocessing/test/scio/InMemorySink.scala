package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.test.common.RandomString

/**
 * Sink for the materialized content of SCollection.
 */
final case class InMemorySink[T: Coder](private val input: SCollection[T]) {
  private val id = RandomString.randomString()
  InMemoryCache.put(id, input)

  def toSeq: Seq[T] = InMemoryCache.get(id)

  def toElement: T = InMemoryCache.get(id)(0)
}
