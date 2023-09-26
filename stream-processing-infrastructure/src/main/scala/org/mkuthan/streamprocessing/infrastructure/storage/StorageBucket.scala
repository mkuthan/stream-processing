package org.mkuthan.streamprocessing.infrastructure.storage

final case class StorageBucket[T](id: String) {
  lazy val url: String = s"gs://$id"
}
