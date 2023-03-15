package org.mkuthan.streamprocessing.shared.it.sink

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.shared.it.sink.InMemoryCache

/**
 * Sink for the materialized content of SCollection.
 */
case class InMemorySink[T](private val input: SCollection[T])(implicit c: Coder[T]) {
  private val id = RandomString.randomString()
  InMemoryCache.put(id, input)

  def toSeq: Seq[T] = InMemoryCache.get(id)

  def toElement: T = InMemoryCache.get(id)(0)
}
