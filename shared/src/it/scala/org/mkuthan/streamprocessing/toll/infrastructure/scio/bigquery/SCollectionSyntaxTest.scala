package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.shared.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.scio._
import org.mkuthan.streamprocessing.toll.infrastructure.scio.IntegrationTestFixtures.SampleClass

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  behavior of "BigQuery SCollection syntax"

  it should "save into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable[SampleClass](datasetName) { bigQueryTable =>
        sc
          .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
          .saveToBigQuery(bigQueryTable)

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(bigQueryTable.datasetName, bigQueryTable.tableName)
            .map(SampleClassBigQueryType.fromAvro)

          results should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }
}