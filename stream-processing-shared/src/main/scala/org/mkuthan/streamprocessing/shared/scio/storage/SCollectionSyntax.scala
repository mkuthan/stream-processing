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
      ioIdentifier: IoIdentifier,
      location: StorageBucket[T],
      writeConfiguration: JsonWriteConfiguration = JsonWriteConfiguration()
  ): Unit = {
    val io = TextIO.write()
      .to(location.id)
      .pipe(write => writeConfiguration.configure(write))

    val _ = self
      .withName(s"$ioIdentifier/Serialize")
      .map(JsonSerde.writeJsonAsString)
      .saveAsCustomOutput(ioIdentifier.id, io)
  }
}

trait SCollectionSyntax {
  implicit def storageSCollectionOps[T <: AnyRef: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)
}
