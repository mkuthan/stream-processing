package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.testing.JobTest.BeamOptions
import com.spotify.scio.testing.SCollectionMatchers

trait JobTestScioContext extends SCollectionMatchers with TimestampedMatchers {
  // required by com.spotify.scio.testing.JobTest
  implicit val beamOptions: BeamOptions = BeamOptions(List.empty)
}
