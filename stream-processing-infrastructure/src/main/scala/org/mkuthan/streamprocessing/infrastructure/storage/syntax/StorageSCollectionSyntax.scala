package org.mkuthan.streamprocessing.infrastructure.storage.syntax

import scala.util.chaining.scalaUtilChainingOps

import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.infrastructure.storage.StorageConfiguration

private[syntax] trait StorageSCollectionSyntax {
  implicit class StorageSCollectionOps[T <: AnyRef: Coder](private val self: SCollection[T]) {

    import com.spotify.scio.values.BetterSCollection._

    def writeToStorageAsJson(
        id: IoIdentifier[T],
        bucket: StorageBucket[T],
        storageConfiguration: StorageConfiguration = StorageConfiguration()
    ): Unit = {
      val io = FileIO.write[String]()
        .via(TextIO.sink())
        .pipe(write => storageConfiguration.configure(write))
        .to(bucket.url)

      val _ = self.betterSaveAsCustomOutput(id.id) { in =>
        in.withName("Serialize")
          .map(JsonSerde.writeJsonAsString)
          .internal.apply("Write", io)
      }
    }
  }
}
