package org.mkuthan.streamprocessing.toll.shared.configuration

final case class StorageBucket[T](id: String, numShards: Int = 1) {
  val name = id.stripPrefix("gs://")
}
