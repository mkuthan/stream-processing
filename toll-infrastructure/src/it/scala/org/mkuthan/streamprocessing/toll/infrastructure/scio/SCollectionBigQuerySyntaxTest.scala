package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.shared.test.scio.BigQueryScioContext

class SCollectionBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with BigQueryScioContext
    with SCollectionBigQuerySyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionBigQuerySyntax"

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
