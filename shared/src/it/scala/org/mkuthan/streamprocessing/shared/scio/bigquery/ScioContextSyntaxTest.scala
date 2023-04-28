package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures
import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.shared.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.shared.test.scio.InMemorySink
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext

class ScioContextSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures._

  behavior of "BigQuery ScioContext syntax"

  // TODO: implement writeTable to prepare test data
  ignore should "load from table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable[SampleClass](datasetName) { bigQueryTable =>
        writeTable(
          bigQueryTable.datasetName,
          bigQueryTable.tableName,
          SampleClassBigQueryType.toAvro(SampleObject1),
          SampleClassBigQueryType.toAvro(SampleObject2)
        )

        val results = sc.loadFromBigQuery(bigQueryTable)

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          resultsSink.toSeq should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }
}
