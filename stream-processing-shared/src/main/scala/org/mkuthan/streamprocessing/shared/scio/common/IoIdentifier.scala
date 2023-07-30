package org.mkuthan.streamprocessing.shared.scio.common

case class IoIdentifier[T](id: String) {
  override def toString: String = id
}
