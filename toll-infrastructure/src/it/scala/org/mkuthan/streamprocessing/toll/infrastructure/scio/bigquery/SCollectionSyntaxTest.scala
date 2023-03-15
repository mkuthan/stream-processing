package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.it.client.BigQueryClient._
import org.mkuthan.streamprocessing.shared.it.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.it.context.BigQueryContext
import org.mkuthan.streamprocessing.shared.it.context.ItScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.scio._

class SCollectionBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with ItScioContext
    with BigQueryContext {

  import IntegrationTestFixtures._

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
