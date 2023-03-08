package org.mkuthan.streamprocessing.toll.shared.core

final case class DeadLetter[T](data: T, error: String)
