package org.mkuthan.streamprocessing.shared.scio.common

case class StorageBucket[T](id: String) extends AnyVal {
  def name: String = id.stripPrefix("gs://")
}
