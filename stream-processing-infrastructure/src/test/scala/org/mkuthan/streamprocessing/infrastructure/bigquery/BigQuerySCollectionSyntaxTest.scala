package org.mkuthan.streamprocessing.infrastructure.bigquery

import org.apache.beam.sdk.Pipeline.PipelineExecutionException

import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio._

@Slow
class BigQuerySCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  behavior of "BigQuery SCollection syntax"

  it should "write bounded into not partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val sampleObjects = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(SampleObject1, SampleObject2)
          .build()

        sc
          .testBounded(sampleObjects)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.notPartitioned[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "write bounded into hourly partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withPartitionedTable(datasetName, "HOUR", SampleClassBigQuerySchema) { tableName =>
        val localDateTime = LocalDateTime.parse("2023-06-15T14:00:00")
        val sampleObjects = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(SampleObject1, SampleObject2)
          .build()

        sc
          .testBounded(sampleObjects)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.hourly[SampleClass](s"$projectId:$datasetName.$tableName", localDateTime)
          )

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "write bounded into daily partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withPartitionedTable(datasetName, "DAY", SampleClassBigQuerySchema) { tableName =>
        val localDate = LocalDate.parse("2023-06-15")
        val sampleObjects = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(SampleObject1, SampleObject2)
          .build()

        sc
          .testBounded(sampleObjects)
          .writeBoundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryPartition.daily[SampleClass](s"$projectId:$datasetName.$tableName", localDate)
          )

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "not write bounded with invalid record into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val invalidObject = SampleObject1.copy(instantField = Instant.ofEpochMilli(Long.MaxValue))
        val sampleObjects = boundedTestCollectionOf[SampleClass]
          .addElementsAtMinimumTime(invalidObject)
          .build()

        sc
          .testBounded(sampleObjects)
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
        val sampleObjects = unboundedTestCollectionOf[SampleClass]
          .addElementsAtWatermarkTime(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testUnbounded(sampleObjects)
          .writeUnboundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryTable[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        val run = sc.run()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results should contain.only(SampleObject1, SampleObject2)
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "not write unbounded with invalid record into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val invalidObject = SampleObject1.copy(instantField = Instant.ofEpochMilli(Long.MaxValue))

        val sampleObjects = unboundedTestCollectionOf[SampleClass]
          .addElementsAtWatermarkTime(invalidObject)
          .advanceWatermarkToInfinity()

        val results = sc
          .testUnbounded(sampleObjects)
          .writeUnboundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryTable[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        results.debug()

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
