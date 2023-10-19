package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.testing.JobTest.BeamOptions

trait JobTestScioContext extends ScioMatchers {
  // required by com.spotify.scio.testing.JobTest
  implicit val beamOptions: BeamOptions = BeamOptions(List.empty)
}
