package org.mkuthan.streamprocessing.toll.infrastructure.scio.storage

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.TextIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsString
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

private[storage] final class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {
  def saveToStorageAsJson(
      location: StorageBucket[T],
      numShards: Int = 1
  ): Unit = {
    val io = TextIO.write()
      .to(location.id)
      .withNumShards(numShards)
      .withSuffix(".json")
      .withWindowedWrites()

    val _ = self
      .map(writeJsonAsString)
      .saveAsCustomOutput(location.id, io)
  }
}

trait SCollectionSyntax {
  implicit def storageSCollectionOps[T <: AnyRef: Coder](sColl: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sColl)
}
