package org.mkuthan.streamprocessing.shared.scio.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.testing._

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

  val diagnosticType = BigQueryType[Diagnostic.Raw]

  val diagnostic = Diagnostic("any reason", 1)

  behavior of "Diagnostic SCollection syntax"

  it should "write" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, diagnosticType.schema) { tableName =>
        val sampleDiagnostics = testStreamOf[Diagnostic]
          .addElements(diagnostic, diagnostic)
          .advanceWatermarkToInfinity()

        sc
          .testStream(sampleDiagnostics)
          .writeDiagnosticToBigQuery(
            IoIdentifier[Diagnostic]("any-id"),
            BigQueryTable[Diagnostic](s"$projectId:$datasetName.$tableName")
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(diagnosticType.fromAvro)

          // TODO: assert results
        }

        run.pipelineResult.cancel()
      }
    }
  }

}
