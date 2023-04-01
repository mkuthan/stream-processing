package org.mkuthan.streamprocessing.shared.test.scio

import com.spotify.scio.testing.JobTest.BeamOptions
import com.spotify.scio.testing.SCollectionMatchers

trait JobTestScioContext extends SCollectionMatchers with TimestampedMatchers {
  // required by com.spotify.scio.testing.JobTest
  implicit val beamOptions = BeamOptions(List.empty)
}
