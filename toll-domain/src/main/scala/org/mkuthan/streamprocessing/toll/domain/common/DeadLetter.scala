package org.mkuthan.streamprocessing.toll.domain.common

final case class DeadLetter[T](data: T, error: String)
