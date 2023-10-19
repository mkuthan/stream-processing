package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.ScioContext
import com.spotify.scio.ScioExecutionContext

trait TestScioContext extends ScioMatchers {
  def runWithScioContext(fn: ScioContext => Any): ScioExecutionContext = {
    val sc = ScioContext.forTest()
    fn(sc)
    sc.run()
  }
}
