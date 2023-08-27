package org.mkuthan.streamprocessing.shared.common

import com.twitter.algebird.Semigroup

trait SumByKey[T] extends Serializable {
  def key(input: T): String

  def semigroup: Semigroup[T]
}

object SumByKey {
  def apply[T](implicit d: SumByKey[T]): SumByKey[T] = d

  def create[T](keyFn: T => String, plusFn: (T, T) => T): SumByKey[T] =
    new SumByKey[T] {
      override def key(input: T): String = keyFn(input)

      override def semigroup: Semigroup[T] = (x: T, y: T) => {
        require(key(x) == key(y))
        plusFn(x, y)
      }
    }
}
