package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

import scala.language.implicitConversions

final class StorageSCollectionOps[T <: AnyRef](private val self: SCollection[T]) extends AnyVal {
  def saveToStorage(
      bucket: StorageBucket[T]
  )(implicit c: Coder[T]): Unit = {
    self
      .map(JsonSerde.write[T])
      .saveAsTextFile(bucket.id)
  }

}

@SuppressWarnings(Array("org.wartremover.warts.ImplicitConversion"))
trait SCollectionStorageSyntax {
  implicit def storageSCollectionOps[T <: AnyRef](sc: SCollection[T]): StorageSCollectionOps[T] =
    new StorageSCollectionOps(sc)
}
