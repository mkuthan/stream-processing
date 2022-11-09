package org.mkuthan.streamprocessing.toll.shared.configuration

final case class StorageBucket[T](bucket: String, numShards: Int) {
  val name = bucket.stripPrefix("gs://")
}
