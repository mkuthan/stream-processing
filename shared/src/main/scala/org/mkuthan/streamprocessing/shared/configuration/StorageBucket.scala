package org.mkuthan.streamprocessing.shared.configuration

final case class StorageBucket[T](id: String) {
  lazy val name: String = id.stripPrefix("gs://")
}
