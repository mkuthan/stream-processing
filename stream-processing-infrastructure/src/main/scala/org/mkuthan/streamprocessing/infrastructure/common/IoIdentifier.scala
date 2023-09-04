package org.mkuthan.streamprocessing.infrastructure.common

case class IoIdentifier[T](id: String) {
  override def toString: String = id
}
