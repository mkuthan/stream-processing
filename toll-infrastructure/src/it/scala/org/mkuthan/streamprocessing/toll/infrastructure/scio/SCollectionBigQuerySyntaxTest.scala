package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient
import org.mkuthan.streamprocessing.shared.test.scio.BigQueryScioContext

class SCollectionBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with BigQueryScioContext
    with BigQueryClient
    with SCollectionBigQuerySyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionBigQuerySyntax"

  it should "save into table simple object" in withScioContext { sc =>
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

  it should "save into table complex object" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable[ComplexClass](datasetName) { bigQueryTable =>
        sc
          .parallelize[ComplexClass](Seq(complexObject1, complexObject2))
          .saveToBigQuery(bigQueryTable)

        sc.run().waitUntilDone()

        eventually {
          val results = readTable(bigQueryTable.datasetName, bigQueryTable.tableName)
            .map(complexClassBigQueryType.fromAvro)

          results should contain.only(complexObject1, complexObject2)
        }
      }
    }
  }
}
