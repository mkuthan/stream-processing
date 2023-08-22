package org.mkuthan.streamprocessing.shared.common

final case class DeadLetter[T](data: T, error: String)
