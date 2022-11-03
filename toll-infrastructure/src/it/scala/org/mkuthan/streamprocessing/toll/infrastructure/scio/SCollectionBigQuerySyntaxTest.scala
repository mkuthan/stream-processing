package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.ScioContext

import com.google.cloud.ServiceOptions
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

  private val projectId = ServiceOptions.getDefaultProjectId

  // TODO
  private val options = PipelineOptionsFactory
    .fromArgs(
      "--tempLocation=gs://stream-processing-tmp/",
      s"--projectId=$projectId" // needed by CI
    )
    .create()

  private val datasetName = generateDatasetName()
  private val tableName = generateTableName()

  private val record1 = BigQueryCaseClass("foo", 1)
  private val record2 = BigQueryCaseClass("foo", 2)

  val schema = BigQueryType[BigQueryCaseClass].schema

  override def beforeAll(): Unit = {
    createDataset(datasetName)
    createTable(datasetName, tableName, schema)
  }

  override def afterAll(): Unit = {
    deleteTable(datasetName, tableName)
    deleteDataset(datasetName)
  }

  behavior of "SCollectionBigQuerySyntax"

  it should "save into table" in {
    val sc = ScioContext(options)

    val stream = sc.parallelize[BigQueryCaseClass](Seq(record1, record2))

    val bigQueryTable = BigQueryTable[BigQueryCaseClass](s"$datasetName.$tableName")
    stream.saveToBigQuery(bigQueryTable)

    sc.run().waitUntilDone()

    val results = read(datasetName, tableName)
    results.foreach { row =>
      println(BigQueryType[BigQueryCaseClass].fromAvro(row))
    }
  }
}
