package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.ScioContext

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll

object SCollectionBigQuerySyntaxTest {
  @BigQueryType.toTable
  final case class BigQueryCaseClass(field1: String, field2: Int)
}

class SCollectionBigQuerySyntaxTest extends PipelineSpec
    with BeforeAndAfterAll
    with BigQueryClient
    with SCollectionBigQuerySyntax {

  import SCollectionBigQuerySyntaxTest._

  // TODO
  private val options = PipelineOptionsFactory
    .fromArgs("--tempLocation=gs://stream-processing-tmp/")
    .create()

  private val datasetName = generateDatasetName()

  private val record1 = BigQueryCaseClass("foo", 1)
  private val record2 = BigQueryCaseClass("foo", 2)

  override def beforeAll(): Unit =
    createDataset(datasetName)

  override def afterAll(): Unit =
    deleteDataset(datasetName)

  behavior of "SCollectionBigQuerySyntax"

  it should "save into table" in {
    val tableName = generateTableName()
    val schema = Schema.of(
      Field.of("field1", StandardSQLTypeName.STRING),
      Field.of("field2", StandardSQLTypeName.INT64)
    )
    createTable(datasetName, tableName, schema)

    val sc = ScioContext(options)

    val stream = sc.parallelize[BigQueryCaseClass](Seq(record1, record2))

    val bigQueryTable = BigQueryTable[BigQueryCaseClass](s"$datasetName.$tableName")
    stream.saveToBigQuery(bigQueryTable)

    sc.run().waitUntilDone()

    val results = query(s"select * from $datasetName.$tableName")
    results.foreach { row =>
      println(row.get("field1"))
      println(row.get("field2"))
    }
  }
}
