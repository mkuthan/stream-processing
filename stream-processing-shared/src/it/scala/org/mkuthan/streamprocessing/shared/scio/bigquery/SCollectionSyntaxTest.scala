package org.mkuthan.streamprocessing.shared.scio.bigquery

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
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  behavior of "BigQuery SCollection syntax"

  it should "save into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
          .saveToBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(datasetName, tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }
}
