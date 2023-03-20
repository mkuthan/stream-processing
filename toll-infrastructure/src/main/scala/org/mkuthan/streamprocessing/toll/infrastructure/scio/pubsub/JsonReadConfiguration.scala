package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read

case class JsonReadConfiguration(
    idAttribute: IdAttribute = NoIdAttribute,
    tsAttribute: TimestampAttribute = NoTimestampAttribute
) {
  def withIdAttribute(idAttribute: IdAttribute): JsonReadConfiguration =
    copy(idAttribute = idAttribute)

  def withTimestampAttribute(tsAttribute: TimestampAttribute): JsonReadConfiguration =
    copy(tsAttribute = tsAttribute)

  def configure[T](read: Read[T]): Read[T] =
    ioParams.foldLeft(read)((read, param) => param.configure(read))

  private lazy val ioParams: Set[PubsubReadParam] = Set(
    idAttribute,
    tsAttribute
  )
}
