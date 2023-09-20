package org.mkuthan.streamprocessing.infrastructure.common

import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio._

@Slow
class IoDiagnosticTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with BigQueryContext {

  val ioDiagnosticType = BigQueryType[IoDiagnostic.Raw]

  val anyIoDiagnostic = IoDiagnostic(
    id = "any-id",
    reason = "any reason"
  )

  val ioDiagnostic1 = anyIoDiagnostic.copy(id = "id1")
  val ioDiagnostic2 = anyIoDiagnostic.copy(id = "id2")

  val tenMinutes = Duration.standardMinutes(10)

  val windowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
    allowedLateness = Duration.ZERO,
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  "Unbounded IO diagnostic" should "be written to BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, ioDiagnosticType.schema) { tableName =>
        val instant = Instant.parse("2023-06-15T12:09:59.999Z")

        val ioDiagnostics = unboundedTestCollectionOf[IoDiagnostic]
          .addElementsAtTime("2023-06-15T12:01:00Z", ioDiagnostic1, ioDiagnostic1, ioDiagnostic2)
          .addElementsAtTime("2023-06-15T12:02:00Z", ioDiagnostic1, ioDiagnostic2)
          .advanceWatermarkToInfinity()

        sc
          .testUnbounded(ioDiagnostics)
          .sumByKeyInFixedWindow(windowDuration = tenMinutes, windowOptions = windowOptions)
          .mapWithTimestamp(IoDiagnostic.toRaw)
          .writeUnboundedToBigQuery(
            IoIdentifier[IoDiagnostic.Raw]("any-id"),
            BigQueryTable[IoDiagnostic.Raw](s"$projectId:$datasetName.$tableName")
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(ioDiagnosticType.fromAvro)

          results should contain.only(
            IoDiagnostic.toRaw(ioDiagnostic1.copy(count = 3L), instant),
            IoDiagnostic.toRaw(ioDiagnostic2.copy(count = 2L), instant)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }

  // TODO: this is oversimplified if batch is done in global window
  "Bounded IO diagnostic" should "be written to BigQuery" in withScioContext { sc =>
    withDataset { datasetName =>
      val instant = Instant.parse("2023-06-15T12:09:59.999Z")

      withPartitionedTable(datasetName, "HOUR", ioDiagnosticType.schema) { tableName =>
        val localDateTime = LocalDateTime.parse("2023-06-15T12:00:00")

        val ioDiagnostics = boundedTestCollectionOf[IoDiagnostic]
          .addElementsAtTime("2023-06-15T12:01:00Z", ioDiagnostic1, ioDiagnostic1, ioDiagnostic2)
          .addElementsAtTime("2023-06-15T12:02:00Z", ioDiagnostic1, ioDiagnostic2)
          .advanceWatermarkToInfinity()

        sc
          .testBounded(ioDiagnostics)
          .sumByKeyInFixedWindow(windowDuration = tenMinutes, windowOptions = windowOptions)
          .mapWithTimestamp(IoDiagnostic.toRaw)
          .writeBoundedToBigQuery(
            IoIdentifier[IoDiagnostic.Raw]("any-id"),
            BigQueryPartition.hourly[IoDiagnostic.Raw](s"$projectId:$datasetName.$tableName", localDateTime)
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName).map(ioDiagnosticType.fromAvro)

          results should contain.only(
            IoDiagnostic.toRaw(ioDiagnostic1.copy(count = 3L), instant),
            IoDiagnostic.toRaw(ioDiagnostic2.copy(count = 2L), instant)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }
}
