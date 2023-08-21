package org.mkuthan.streamprocessing.infrastructure.common

// TODO: move to infrastructure
case class IoIdentifier[T](id: String) {
  override def toString: String = id
}
