package org.mkuthan.streamprocessing.toll.infrastructure.scio.core

import com.spotify.scio.ScioMetrics

trait Counter[T] {
  def inc(): Unit
  def counter: org.apache.beam.sdk.metrics.Counter
}

object Counter {
  def apply[T](namespace: String, name: String): Counter[T] = new Counter[T] with Serializable {
    private val _counter = ScioMetrics.counter(namespace, name)

    override def counter = _counter
    override def inc(): Unit = counter.inc()
  }
}
