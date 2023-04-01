package org.mkuthan.streamprocessing.shared.test.scio

import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.ScioContext
import com.spotify.scio.ScioExecutionContext

trait TestScioContext extends SCollectionMatchers with TimestampedMatchers {
  def runWithScioContext(fn: ScioContext => Any): ScioExecutionContext = {
    val sc = ScioContext.forTest()
    fn(sc)
    sc.run()
  }
}
