package org.mkuthan.streamprocessing.shared.configuration

final case class StorageBucket[T](id: String) extends AnyVal {
  def name = id.stripPrefix("gs://")
}