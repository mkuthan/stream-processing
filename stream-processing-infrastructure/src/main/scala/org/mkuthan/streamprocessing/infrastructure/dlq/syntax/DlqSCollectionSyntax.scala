package org.mkuthan.streamprocessing.infrastructure.dlq.syntax

import scala.util.chaining._

import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.dlq.DlqConfiguration
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.infrastructure.storage.StorageConfiguration
import org.mkuthan.streamprocessing.infrastructure.storage.Suffix
import org.mkuthan.streamprocessing.shared.json.JsonSerde

private[syntax] trait DlqSCollectionSyntax {

  implicit class DlqSCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {
    def writeDeadLetterToStorageAsJson(
        id: IoIdentifier[T],
        bucket: StorageBucket[T],
        configuration: DlqConfiguration = DlqConfiguration()
    ): Unit = {
      val storageConfiguration = StorageConfiguration()
        .withPrefix(configuration.prefix)
        .withSuffix(Suffix.Json)
        .withNumShards(configuration.numShards)

      val io = FileIO.write[String]()
        .via(TextIO.sink())
        .pipe(write => storageConfiguration.configure(write))
        .to(bucket.url)

      val _ = self
        .withName(s"$id/Serialize")
        .map(JsonSerde.writeJsonAsString)
        .withName(s"$id/Window")
        .withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
        .saveAsCustomOutput(id.id, io)
    }
  }
}