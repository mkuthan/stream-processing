package org.mkuthan.streamprocessing.infrastructure.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.testing._

import org.joda.time.Duration
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.shared.common.SumByKey
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  import SCollectionSyntaxTest._

  val sampleDiagnosticType = BigQueryType[SampleDiagnostic]

  behavior of "Diagnostic SCollection syntax"

  it should "write into BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, sampleDiagnosticType.schema) { tableName =>
        val sampleDiagnostic1 = SampleDiagnostic("first reason")
        val sampleDiagnostic2 = SampleDiagnostic("second reason")

        val sampleDiagnostics = testStreamOf[SampleDiagnostic]
          .addElementsAtTime("12:00:00", sampleDiagnostic1, sampleDiagnostic2)
          .addElementsAtTime("12:00:59", sampleDiagnostic1)
          .advanceWatermarkToInfinity()

        sc
          .testStream(sampleDiagnostics)
          .writeDiagnosticToBigQuery(
            IoIdentifier[SampleDiagnostic]("any-id"),
            BigQueryTable[SampleDiagnostic](s"$projectId:$datasetName.$tableName"),
            DiagnosticConfiguration().withWindowDuration(Duration.standardMinutes(1))
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(sampleDiagnosticType.fromAvro)

          results should contain.only(
            SampleDiagnostic("first reason", 2),
            SampleDiagnostic("second reason", 1)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

object SCollectionSyntaxTest {
  @BigQueryType.toTable
  case class SampleDiagnostic(reason: String, count: Long = 1)

  object SampleDiagnostic {
    implicit val diagnostic: SumByKey[SampleDiagnostic] =
      SumByKey.create(
        groupKeyFn = _.reason,
        plusFn = (x, y) => x.copy(count = x.count + y.count)
      )
  }
}
