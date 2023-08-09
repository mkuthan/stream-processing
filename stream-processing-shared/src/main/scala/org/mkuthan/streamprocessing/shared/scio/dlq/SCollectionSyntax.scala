package org.mkuthan.streamprocessing.shared.scio.dlq

import scala.language.implicitConversions
import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.common.StorageBucket
import org.mkuthan.streamprocessing.shared.scio.dlq.DlqConfiguration

private[dlq] class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {
  def writeDeadLetterToStorageAsJson(
      id: IoIdentifier[T],
      bucket: StorageBucket[T],
      configuration: DlqConfiguration = DlqConfiguration()
  ): Unit = {
    val io = FileIO.write[String]()
      .via(TextIO.sink())
      .pipe(write => configuration.configure(write))
      .to(bucket.id)

    val _ = self
      .withName(s"$id/Serialize")
      .map(JsonSerde.writeJsonAsString)
      .withName(s"$id/Window")
      .withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
      .saveAsCustomOutput(id.id, io)
  }
}

trait SCollectionSyntax {
  implicit def dlqSCollectionOps[T <: AnyRef: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)
}
