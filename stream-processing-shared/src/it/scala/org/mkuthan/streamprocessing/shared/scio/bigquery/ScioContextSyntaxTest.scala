package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures
import org.mkuthan.streamprocessing.test.gcp.BigQueryClient._
import org.mkuthan.streamprocessing.test.gcp.BigQueryContext
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.scio.InMemorySink
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class ScioContextSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with BigQueryContext {

  import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures._

  behavior of "BigQuery ScioContext syntax"

  it should "load from table" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        writeTable(
          datasetName,
          tableName,
          SampleClassBigQueryType.toTableRow(SampleObject1),
          SampleClassBigQueryType.toTableRow(SampleObject2)
        )

        val results =
          sc.loadFromBigQuery(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          resultsSink.toSeq should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "load from table using SQL" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        writeTable(
          datasetName,
          tableName,
          SampleClassBigQueryType.toTableRow(SampleObject1),
          SampleClassBigQueryType.toTableRow(SampleObject2)
        )

        val results = sc.loadFromBigQuery(
          ioIdentifier = IoIdentifier("any-id"),
          table = BigQueryTable[SampleClass](s"$datasetName.$tableName"),
          configuration = ExportConfiguration()
            .withQuery(ExportQuery.SqlQuery(s"SELECT * FROM $datasetName.$tableName WHERE intField = 1"))
        )

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          resultsSink.toSeq should contain.only(SampleObject1)
        }
      }
    }
  }

  it should "load from table storage" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        writeTable(
          datasetName,
          tableName,
          SampleClassBigQueryType.toTableRow(SampleObject1),
          SampleClassBigQueryType.toTableRow(SampleObject2)
        )

        val results =
          sc.loadFromBigQueryStorage(IoIdentifier("any-id"), BigQueryTable[SampleClass](s"$datasetName.$tableName"))

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          resultsSink.toSeq should contain.only(SampleObject1, SampleObject2)
        }
      }
    }
  }

  it should "load from table storage with row restriction" in withScioContext { sc =>
    withDataset { datasetName =>
      withTable(datasetName, SampleClassBigQuerySchema) { tableName =>
        writeTable(
          datasetName,
          tableName,
          SampleClassBigQueryType.toTableRow(SampleObject1),
          SampleClassBigQueryType.toTableRow(SampleObject2)
        )

        val results = sc.loadFromBigQueryStorage(
          ioIdentifier = IoIdentifier("any-id"),
          table = BigQueryTable[SampleClass](s"$datasetName.$tableName"),
          configuration = StorageReadConfiguration()
            .withRowRestriction(RowRestriction.SqlRowRestriction("intField = 1"))
        )

        val resultsSink = InMemorySink(results)

        sc.run().waitUntilDone()

        eventually {
          resultsSink.toSeq should contain.only(SampleObject1)
        }
      }
    }
  }
}
