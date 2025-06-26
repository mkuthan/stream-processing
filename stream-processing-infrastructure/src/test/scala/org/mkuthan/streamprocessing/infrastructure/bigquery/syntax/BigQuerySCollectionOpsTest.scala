package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import org.apache.beam.sdk.Pipeline.PipelineExecutionException

import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.InMemorySink
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class BigQuerySCollectionOpsTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  behavior of "BigQuery SCollection syntax"

  it should "write bounded into not partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val input = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testBounded(input)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.notPartitioned[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results.toSeq should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "write bounded into hourly partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withPartitionedTable(datasetName, "HOUR", SampleClassBigQuerySchema) { tableName =>
        val localDateTime = LocalDateTime.parse("2023-06-15T14:00:00")
        val input = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testBounded(input)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.hourly[SampleClass](s"$projectId:$datasetName.$tableName", localDateTime)
          )

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results.toSeq should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "write bounded into daily partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withPartitionedTable(datasetName, "DAY", SampleClassBigQuerySchema) { tableName =>
        val localDate = LocalDate.parse("2023-06-15")
        val input = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testBounded(input)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.daily[SampleClass](s"$projectId:$datasetName.$tableName", localDate)
          )

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results.toSeq should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "not write bounded with invalid record into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val invalidObject = SampleObject1.copy(instantField = Instant.ofEpochMilli(Long.MaxValue))

        val input = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(invalidObject)
          .advanceWatermarkToInfinity()

        sc
          .testBounded(input)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.notPartitioned[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        // TODO: why sc.run().waitUntilFinish
        // * throws an exception instead of returning FAILED status?
        // * don't respect given timout

        val thrown = the[PipelineExecutionException] thrownBy sc.run().waitUntilFinish()
        thrown.getCause.getMessage should startWith("Failed to create job with prefix")
      }
    }
  }

  it should "write unbounded into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val input = unboundedTestCollectionOf[SampleClass]
          .addElementsAtWatermarkTime(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testUnbounded(input)
          .writeUnboundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryTable[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results.toSeq should contain.only(SampleObject1, SampleObject2)
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "not write unbounded with invalid record into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val invalidObject = SampleObject1.copy(instantField = Instant.ofEpochMilli(Long.MaxValue))

        val input = unboundedTestCollectionOf[SampleClass]
          .addElementsAtWatermarkTime(invalidObject)
          .advanceWatermarkToInfinity()

        val results = sc
          .testUnbounded(input)
          .writeUnboundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryTable[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        val resultsSink = InMemorySink(results)

        val run = sc.run()

        eventually {
          val deadLetter = resultsSink.toElement

          deadLetter.row should be(invalidObject)
          deadLetter.error should include("Problem converting field root.instantfield expected type: TIMESTAMP")
        }

        run.pipelineResult.cancel()
      }
    }
  }
}
