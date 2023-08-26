package org.mkuthan.streamprocessing.infrastructure.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.testing._

import org.joda.time.Instant
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio._

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with BigQueryContext {

  val sampleDiagnosticType = BigQueryType[Diagnostic.Raw]

  val anyDiagnostic = Diagnostic(
    createdAt = Instant.parse("2014-09-10T12:03:01Z"),
    id = "any id",
    reason = "any reason"
  )

  val diagnostic1 = anyDiagnostic.copy(id = "id1")
  val diagnostic2 = anyDiagnostic.copy(id = "id2")

  behavior of "Diagnostic SCollection syntax"

  it should "write unbounded into BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, sampleDiagnosticType.schema) { tableName =>
        val sampleDiagnostics = testStreamOf[Diagnostic.Raw]
          .addElements(diagnostic1, diagnostic1, diagnostic1, diagnostic2, diagnostic2)
          .advanceWatermarkToInfinity()

        sc
          .testStream(sampleDiagnostics)
          .writeUnboundedDiagnosticToBigQuery(
            IoIdentifier[Diagnostic.Raw]("any-id"),
            BigQueryTable[Diagnostic.Raw](s"$projectId:$datasetName.$tableName")
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(sampleDiagnosticType.fromAvro)

          results should contain.only(
            diagnostic1.copy(count = 3L),
            diagnostic2.copy(count = 2L)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "write bounded into BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      withPartitionedTable(datasetName, "HOUR", sampleDiagnosticType.schema) { tableName =>
        val localDateTime = LocalDateTime.parse("2023-06-15T14:00:00")

        sc
          .parallelize(Seq(diagnostic1, diagnostic1, diagnostic1, diagnostic2, diagnostic2))
          .writeBoundedDiagnosticToBigQuery(
            IoIdentifier[Diagnostic.Raw]("any-id"),
            BigQueryPartition.hourly[Diagnostic.Raw](s"$projectId:$datasetName.$tableName", localDateTime)
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(sampleDiagnosticType.fromAvro)

          results should contain.only(
            diagnostic1.copy(count = 3L),
            diagnostic2.copy(count = 2L)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }
}
