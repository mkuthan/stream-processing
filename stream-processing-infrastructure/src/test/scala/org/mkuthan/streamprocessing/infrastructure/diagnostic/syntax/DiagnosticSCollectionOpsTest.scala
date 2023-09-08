package org.mkuthan.streamprocessing.infrastructure.diagnostic.syntax

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio._

@Slow
class DiagnosticSCollectionOpsTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with BigQueryContext {

  val sampleDiagnosticType = BigQueryType[IoDiagnostic.Raw]

  val anyDiagnostic = IoDiagnostic(
    id = "any-id",
    reason = "any reason"
  )

  val diagnostic1 = anyDiagnostic.copy(id = "id1")
  val diagnostic2 = anyDiagnostic.copy(id = "id2")

  behavior of "Diagnostic SCollection syntax"

  it should "write unbounded into BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, sampleDiagnosticType.schema) { tableName =>
        val instant = Instant.parse("2023-06-15T12:09:59.999Z")

        val ioDiagnostics = unboundedTestCollectionOf[IoDiagnostic]
          .addElementsAtTime("2023-06-15T12:01:00Z", diagnostic1, diagnostic1, diagnostic2)
          .addElementsAtTime("2023-06-15T12:02:00Z", diagnostic1, diagnostic2)
          .advanceWatermarkToInfinity()

        sc
          .testUnbounded(ioDiagnostics)
          .writeUnboundedDiagnosticToBigQuery(
            IoIdentifier[IoDiagnostic.Raw]("any-id"),
            BigQueryTable[IoDiagnostic.Raw](s"$projectId:$datasetName.$tableName"),
            IoDiagnostic.toRaw
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(sampleDiagnosticType.fromAvro)

          results should contain.only(
            IoDiagnostic.toRaw(diagnostic1.copy(count = 3L), instant),
            IoDiagnostic.toRaw(diagnostic2.copy(count = 2L), instant)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "write bounded into BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      val instant = Instant.parse("2023-06-15T12:09:59.999Z")

      withPartitionedTable(datasetName, "HOUR", sampleDiagnosticType.schema) { tableName =>
        val localDateTime = LocalDateTime.parse("2023-06-15T12:00:00")

        val ioDiagnostics = boundedTestCollectionOf[IoDiagnostic]
          .addElementsAtTime("2023-06-15T12:01:00Z", diagnostic1, diagnostic1, diagnostic2)
          .addElementsAtTime("2023-06-15T12:02:00Z", diagnostic1, diagnostic2)
          .build()

        sc
          .testBounded(ioDiagnostics)
          .writeBoundedDiagnosticToBigQuery(
            IoIdentifier[IoDiagnostic.Raw]("any-id"),
            BigQueryPartition.hourly[IoDiagnostic.Raw](s"$projectId:$datasetName.$tableName", localDateTime),
            IoDiagnostic.toRaw
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(sampleDiagnosticType.fromAvro)

          results should contain.only(
            IoDiagnostic.toRaw(diagnostic1.copy(count = 3L), instant),
            IoDiagnostic.toRaw(diagnostic2.copy(count = 2L), instant)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }
}
