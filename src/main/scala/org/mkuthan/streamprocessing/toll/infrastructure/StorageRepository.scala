package org.mkuthan.streamprocessing.toll.infrastructure

import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.toll.configuration.StorageBucket

object StorageRepository {
  def save[T](bucket: StorageBucket[T], data: SCollection[T]): Unit = ???
}
