package org.mkuthan.streamprocessing.shared.scio.storage

import scala.language.implicitConversions
import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.TextIO

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.common.StorageBucket

private[storage] class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {
  def saveToStorageAsJson(
      id: IoIdentifier[T],
      bucket: StorageBucket[T],
      configuration: JsonWriteConfiguration = JsonWriteConfiguration()
  ): Unit = {
    val io = TextIO.write()
      .pipe(write => configuration.configure(write))
      .to(bucket.id)

    val _ = self
      .withName(s"$id/Serialize")
      .map(JsonSerde.writeJsonAsString)
      .saveAsCustomOutput(id.id, io)
  }
}

trait SCollectionSyntax {
  implicit def storageSCollectionOps[T <: AnyRef: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)
}
