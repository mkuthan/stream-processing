package org.mkuthan.streamprocessing.shared.scio.bigquery

import com.spotify.scio.testing._

import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
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

  it should "write bounded into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
          .writeToBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

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
          .writeToBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

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

  it should "not write invalid record into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        val invalidObject = SampleObject1.copy(instantField = Instant.ofEpochMilli(Long.MaxValue))

        val results = sc
          .parallelize[SampleClass](Seq(invalidObject))
          .writeToBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

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
