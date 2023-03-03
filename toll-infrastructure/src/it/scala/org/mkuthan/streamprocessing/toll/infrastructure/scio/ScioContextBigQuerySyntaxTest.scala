package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.common.InMemorySink
import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.shared.test.scio.BigQueryScioContext

class ScioContextBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with BigQueryScioContext
    with ScioContextBigQuerySyntax
    with SCollectionStorageSyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionBigQuerySyntax"

  // TODO: implement writeTable to prepare test data
  ignore should "load from table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable[SimpleClass](datasetName) { bigQueryTable =>
        writeTable(
          bigQueryTable.datasetName,
          bigQueryTable.tableName,
          simpleClassBigQueryType.toAvro(simpleObject1),
          simpleClassBigQueryType.toAvro(simpleObject2)
        )

        val results = sc
          .loadFromBigQuery(bigQueryTable)

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          resultsSink.toSeq should contain.only(simpleObject1, simpleObject2)
        }
      }
    }
  }
}
