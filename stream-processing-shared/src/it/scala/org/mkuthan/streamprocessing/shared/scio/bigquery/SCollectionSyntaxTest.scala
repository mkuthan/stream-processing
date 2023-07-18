package org.mkuthan.streamprocessing.shared.scio.bigquery

import com.spotify.scio.testing._

import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.common.BigQueryPartition
import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures
import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio.InMemorySink
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  behavior of "BigQuery SCollection syntax"

  it should "write bounded into not partitioned table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
          .writeBoundedToBigQuery(
            IoIdentifier("any-id"),
            BigQueryPartition.notPartitioned[SampleClass](s"$datasetName.$tableName")
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

        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
          .writeBoundedToBigQuery(
            IoIdentifier("any-id"),
            BigQueryPartition.hourly[SampleClass](s"$datasetName.$tableName", localDateTime)
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

        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
          .writeBoundedToBigQuery(
            IoIdentifier("any-id"),
            BigQueryPartition.daily[SampleClass](s"$datasetName.$tableName", localDate)
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

  it should "write unbounded into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val sampleObjects = testStreamOf[SampleClass]
          .addElements(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testStream(sampleObjects)
          .writeUnboundedToBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

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

        val sampleObjects = testStreamOf[SampleClass]
          .addElements(invalidObject)
          .advanceWatermarkToInfinity()

        val results = sc
          .testStream(sampleObjects)
          .writeUnboundedToBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          val deadLetter = resultsSink.toElement

          deadLetter.row should be(invalidObject)
          deadLetter.error should include("Problem converting field root.instantField expected type: TIMESTAMP")
        }
      }
    }
  }
}
