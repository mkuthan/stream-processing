package org.mkuthan.streamprocessing.infrastructure.common

final case class IoIdentifier[T](id: String) {
  override def toString: String = id
}
