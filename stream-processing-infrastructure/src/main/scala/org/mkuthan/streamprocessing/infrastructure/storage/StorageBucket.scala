package org.mkuthan.streamprocessing.infrastructure.storage

case class StorageBucket[T](id: String) {
  lazy val name: String = id.stripPrefix("gs://")
}
