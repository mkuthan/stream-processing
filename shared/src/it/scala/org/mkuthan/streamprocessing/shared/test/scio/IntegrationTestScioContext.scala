package org.mkuthan.streamprocessing.shared.test.scio

import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptions

trait IntegrationTestScioContext {
  def withScioContext[T](fn: ScioContext => T): T = {
    val sc = ScioContext()
    fn(sc)
  }

  def withScioContext[T](options: PipelineOptions)(fn: ScioContext => T): T = {
    val sc = ScioContext(options)
    fn(sc)
  }
}
