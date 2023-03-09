package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read

sealed trait PubSubAttribute

object PubSubAttribute {
  val DefaultId = Id("id")
  val DefaultTimestamp = Timestamp("timestamp")

  case class Id(name: String) extends AnyVal {
    def configure[T](read: Read[T]): Read[T] = read.withIdAttribute(name)
  }
  case class Timestamp(name: String) extends AnyVal {
    def configure[T](read: Read[T]): Read[T] = read.withTimestampAttribute(name)
  }
}
