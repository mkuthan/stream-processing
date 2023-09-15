package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.testing.JobTest
import org.mkuthan.streamprocessing.test.scio.JobTestScioContext
import org.mkuthan.streamprocessing.toll.application.TollApplicationFixtures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TollBatchApplicationTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollApplicationFixtures {

  "Toll application" should "run" in {
    JobTest[TollBatchApplication.type]
      .args("--foo=bar")
      .run()
  }
}
