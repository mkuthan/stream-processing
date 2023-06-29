package org.mkuthan.streamprocessing.shared.scio.common

case class StorageBucket[T](id: String) {
  lazy val name: String = id.stripPrefix("gs://")
}
