package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.ScioContext
import com.spotify.scio.ScioExecutionContext

trait TestScioContext extends SCollectionMatchers with TimestampedMatchers with TestCollectionSyntax {
  def runWithScioContext(fn: ScioContext => Any): ScioExecutionContext = {
    val sc = ScioContext.forTest()
    fn(sc)
    sc.run()
  }
}
