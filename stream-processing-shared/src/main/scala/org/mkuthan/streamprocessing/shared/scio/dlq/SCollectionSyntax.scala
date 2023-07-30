package org.mkuthan.streamprocessing.shared.scio.dlq

import scala.language.implicitConversions
import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.TextIO

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.common.StorageBucket

private[dlq] class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {
  def writeDeadLetterToStorageAsJson(
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
      .withName(s"$id/ApplyFixedWindow")
      .withFixedWindows(configuration.duration)
      .saveAsCustomOutput(id.id, io)
  }
}

trait SCollectionSyntax {
  implicit def dlqSCollectionOps[T <: AnyRef: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)
}
