package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

final class StorageSCollectionOps[T <: AnyRef](private val self: SCollection[T]) extends AnyVal {
  def saveToStorage(
      location: StorageLocation[T]
  )(implicit c: Coder[T]): Unit = {
    self
      .map(JsonSerde.write[T])
      .saveAsTextFile(location.path)
  }

}

@SuppressWarnings(Array("org.wartremover.warts.ImplicitConversion"))
trait SCollectionStorageSyntax {
  implicit def storageSCollectionOps[T <: AnyRef](sc: SCollection[T]): StorageSCollectionOps[T] =
    new StorageSCollectionOps(sc)
}
