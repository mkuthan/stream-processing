package org.mkuthan.streamprocessing.shared.scio.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.testing._

import com.twitter.algebird.Semigroup
import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  import SCollectionSyntaxTest._

  val sampleDiagnosticType = BigQueryType[SampleDiagnostic]

  behavior of "Diagnostic SCollection syntax"

  it should "write" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, sampleDiagnosticType.schema) { tableName =>
        val sampleDiagnostic1 = SampleDiagnostic(Instant.parse("2014-09-10T12:00:00.000Z"), "first reason")
        val sampleDiagnostic2 = SampleDiagnostic(Instant.parse("2014-09-10T12:00:00.000Z"), "second reason")
        val sampleDiagnostic3 = SampleDiagnostic(Instant.parse("2014-09-10T12:00:01.000Z"), "first reason")

        val sampleDiagnostics = testStreamOf[SampleDiagnostic]
          .addElements(sampleDiagnostic1, sampleDiagnostic2, sampleDiagnostic3)
          .advanceWatermarkToInfinity()

        sc
          .testStream(sampleDiagnostics)
          .writeDiagnosticToBigQuery(
            IoIdentifier[SampleDiagnostic]("any-id"),
            BigQueryTable[SampleDiagnostic](s"$projectId:$datasetName.$tableName"),
            SampleDiagnosticSemigroup
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(sampleDiagnosticType.fromAvro)

          results should contain.only(
            SampleDiagnostic(Instant.parse("2014-09-10T12:00:00.000Z"), "first reason", 2),
            SampleDiagnostic(Instant.parse("2014-09-10T12:00:00.000Z"), "second reason", 1)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

object SCollectionSyntaxTest {
  @BigQueryType.toTable
  case class SampleDiagnostic(createdAt: Instant, reason: String, count: Long = 1) {
    override def toString: String = reason
  }

  case object SampleDiagnosticSemigroup extends Semigroup[SampleDiagnostic] {
    override def plus(x: SampleDiagnostic, y: SampleDiagnostic): SampleDiagnostic = {
      require(x.toString == y.toString)
      SampleDiagnostic(x.createdAt, x.reason, x.count + y.count)
    }
  }
}
