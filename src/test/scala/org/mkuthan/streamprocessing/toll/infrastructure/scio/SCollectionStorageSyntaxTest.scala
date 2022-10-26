package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptionsFactory

case class Foo(field1: String, field2: Int)

final class SCollectionStorageSyntaxTest extends PipelineSpec with SCollectionStorageSyntax {

  private val options = PipelineOptionsFactory.create()

  ignore should "bar" in {
    val sc = ScioContext(options)

    val location = StorageLocation[Foo]("gs://foo/bar")
    sc.parallelize[Foo](Seq(Foo("foo", 1))).saveToStorage(location)

    sc.run().waitUntilDone()

    // TODO assertions
  }
}
