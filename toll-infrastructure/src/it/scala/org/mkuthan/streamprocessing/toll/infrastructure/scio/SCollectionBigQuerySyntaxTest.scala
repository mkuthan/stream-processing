package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient
import org.mkuthan.streamprocessing.shared.test.scio.BigQueryScioContext

class SCollectionBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationPatience
    with BigQueryScioContext
    with BigQueryClient
    with SCollectionBigQuerySyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionBigQuerySyntax"

  it should "save into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable[SimpleClass](datasetName) { bigQueryTable =>
        sc
          .parallelize[SimpleClass](Seq(simpleObject1, simpleObject2))
          .saveToBigQuery(bigQueryTable)

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(bigQueryTable.datasetName, bigQueryTable.tableName)
            .map(simpleClassBigQueryType.fromAvro)

          results should contain.only(simpleObject1, simpleObject2)
        }
      }
    }
  }
}
