package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.it.common.InMemorySink
import org.mkuthan.streamprocessing.shared.it.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.it.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.shared.it.scio.BigQueryScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.scio._

class ScioContextSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with BigQueryScioContext {

  import IntegrationTestFixtures._

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
