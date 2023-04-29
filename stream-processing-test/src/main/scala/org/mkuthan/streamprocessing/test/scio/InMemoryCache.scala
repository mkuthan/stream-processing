package org.mkuthan.streamprocessing.test.scio

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

/**
 * In memory cache for the elements materialized from SCollection. Must be static - if not, Cache will be serialized
 * with the `input.map` closure.
 */
private[scio] case object InMemoryCache {
  // TODO: thread-safety / atomicity
  // Current implementation isn't fully thread-safe (ArrayBuffer)
  // Does it really matter? How many threads computes `input.map`?
  private val cache = TrieMap.empty[String, ArrayBuffer[Any]]

  def put[T: Coder](id: String, input: SCollection[T]): Unit = {
    cache += id -> ArrayBuffer.empty
    val _ = input.tap { element =>
      cache(id) += element
    }
  }

  // TODO: verify performance, toSeq makes a copy of ArrayBuffer
  def get[T](id: String): Seq[T] =
    cache(id).toSeq.asInstanceOf[Seq[T]]
}
