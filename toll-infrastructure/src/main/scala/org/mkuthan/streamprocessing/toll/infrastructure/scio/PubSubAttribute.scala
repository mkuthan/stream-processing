package org.mkuthan.streamprocessing.toll.infrastructure.scio

sealed trait PubSubAttribute

object PubSubAttribute {
  val DefaultId = Id("id")
  val DefaultTimestamp = Timestamp("timestamp")

  case class Id(name: String) extends AnyVal
  case class Timestamp(name: String) extends AnyVal
}
