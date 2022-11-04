package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.bigquery.types.BigQueryType

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient
import org.mkuthan.streamprocessing.shared.test.scio.BigQueryScioContext

class SCollectionBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with BigQueryScioContext
    with BigQueryClient
    with SCollectionBigQuerySyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionBigQuerySyntax"

  ignore should "save into table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable[SimpleClass](datasetName) { bigQueryTable =>
        sc
          .parallelize[SimpleClass](Seq(simpleObject1, simpleObject2))
          .saveToBigQuery(bigQueryTable)

        sc.run().waitUntilDone()

        val results = readTable(bigQueryTable.datasetName, bigQueryTable.tableName)
        results.foreach { row =>
          println(BigQueryType[SimpleClass].fromAvro(row))
        }
      }
    }
  }
}
