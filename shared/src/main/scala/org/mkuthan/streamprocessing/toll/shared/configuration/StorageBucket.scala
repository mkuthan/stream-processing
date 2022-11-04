package org.mkuthan.streamprocessing.toll.shared.configuration

final case class StorageBucket[T](id: String) {
  val name = id.stripPrefix("gs://")
}
