package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.TextIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

final class StorageSCollectionOps[T <: AnyRef](private val self: SCollection[T]) extends AnyVal {
  def saveToStorage(
      location: StorageBucket[T]
  )(implicit c: Coder[T]): Unit = {
    val io = TextIO.write()
      .to(location.id)
      .withNumShards(1)
      .withSuffix(".json")
      .withWindowedWrites()

    self
      .map(JsonSerde.write)
      .saveAsCustomOutput(location.id, io)
    ()
  }
}

trait SCollectionStorageSyntax {
  implicit def storageSCollectionOps[T <: AnyRef](sc: SCollection[T]): StorageSCollectionOps[T] =
    new StorageSCollectionOps(sc)
}
