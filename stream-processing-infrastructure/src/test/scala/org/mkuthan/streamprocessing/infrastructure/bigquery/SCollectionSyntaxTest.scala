package org.mkuthan.streamprocessing.infrastructure.bigquery

import com.spotify.scio.testing._

import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio._

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

        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
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

        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
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

  it should "write unbounded into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val sampleObjects = testStreamOf[SampleClass]
          .addElements(SampleObject1, SampleObject2)
          .advanceWatermarkToInfinity()

        sc
          .testStream(sampleObjects)
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

        val sampleObjects = testStreamOf[SampleClass]
          .addElements(invalidObject)
          .advanceWatermarkToInfinity()

        val results = sc
          .testStream(sampleObjects)
          .writeUnboundedToBigQuery(
            IoIdentifier[SampleClass]("any-id"),
            BigQueryTable[SampleClass](s"$projectId:$datasetName.$tableName")
          )

        val resultsSink = InMemorySink(results)

        val run = sc.run()

        eventually {
          val deadLetter = resultsSink.toElement

          deadLetter.row should be(invalidObject)
          deadLetter.error should include("Problem converting field root.instantField expected type: TIMESTAMP")
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "map unbounded dead letter into diagnostic" in withScioContext { sc =>
    val instant = Instant.parse("2014-09-10T12:01:00.000Z")
    val id1 = IoIdentifier[SampleClass]("id 1")
    val id2 = IoIdentifier[SampleClass]("id 2")
    val error = "any error"

    val deadLetter1 = BigQueryDeadLetter(id1, SampleObject1, error)
    val deadLetter2 = BigQueryDeadLetter(id2, SampleObject2, error)

    val deadLetters = testStreamOf[BigQueryDeadLetter[SampleClass]]
      .addElementsAtTime(instant.toString, deadLetter1, deadLetter2)
      .advanceWatermarkToInfinity()

    val results = sc.testStream(deadLetters).toDiagnostic()

    results should containInAnyOrder(Seq(
      IoDiagnostic(instant, id1, error),
      IoDiagnostic(instant, id2, error)
    ))
  }
}
