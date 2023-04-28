package org.mkuthan.streamprocessing.shared.configuration

case class StorageBucket[T](id: String) extends AnyVal {
  def name: String = id.stripPrefix("gs://")
}
