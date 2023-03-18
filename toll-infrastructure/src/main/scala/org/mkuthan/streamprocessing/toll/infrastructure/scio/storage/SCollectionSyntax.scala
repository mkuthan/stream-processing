package org.mkuthan.streamprocessing.toll.infrastructure.scio.storage

import scala.language.implicitConversions
import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.TextIO

import org.mkuthan.streamprocessing.shared.configuration.StorageBucket
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsString

private[storage] final class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {
  def saveToStorageAsJson(
      location: StorageBucket[T],
      writeConfiguration: JsonWriteConfiguration = JsonWriteConfiguration()
  ): Unit = {
    val io = TextIO.write()
      .to(location.id)
      .pipe(write => writeConfiguration.configure(write))

    val _ = self
      .map(writeJsonAsString)
      .saveAsCustomOutput(location.id, io)
  }
}

trait SCollectionSyntax {
  implicit def storageSCollectionOps[T <: AnyRef: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)
}
