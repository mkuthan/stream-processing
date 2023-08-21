package org.mkuthan.streamprocessing.shared.common

case class Message[T](
    payload: T,
    attributes: Map[String, String] = Map.empty
)
