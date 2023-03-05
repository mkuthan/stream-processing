package org.mkuthan.streamprocessing.toll.shared

final case class DeadLetter[T](data: T, error: String)
