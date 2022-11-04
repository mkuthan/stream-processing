package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.reflect.runtime.universe.TypeTag

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.shared.test.gcp.BigQueryClient
import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext
import org.mkuthan.streamprocessing.toll.shared.configuration.BigQueryTable

class SCollectionBigQuerySyntaxTest extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with IntegrationTestScioContext
    with StorageClient
    with BigQueryClient
    with SCollectionBigQuerySyntax {

  import IntegrationTestFixtures._

  private val tmpBucketName = generateBucketName()
  private val datasetName = generateDatasetName()

  val options = PipelineOptionsFactory
    .create()
    .as(classOf[PipelineOptions])
  options.setTempLocation(s"gs://$tmpBucketName/")

  def withTable[T <: HasAnnotation: TypeTag](fn: BigQueryTable[T] => Any) {
    val tableName = generateTableName()
    val bigQueryTable = BigQueryTable[T](datasetName, tableName)
    createTable(datasetName, tableName, BigQueryType[T].schema)
    try
      fn(bigQueryTable)
    finally
      deleteTable(datasetName, tableName)
  }

  override def beforeAll(): Unit = {
    createBucket(tmpBucketName)
    createDataset(datasetName)
  }

  override def afterAll(): Unit = {
    deleteDataset(datasetName)
    deleteBucket(tmpBucketName)
  }

  behavior of "SCollectionBigQuerySyntax"

  ignore should "save into table" in withScioContext(options) { sc =>
    withTable[SimpleClass] { bigQueryTable =>
      sc
        .parallelize[SimpleClass](Seq(simpleObject1, simpleObject2))
        .saveToBigQuery(bigQueryTable)

      sc.run().waitUntilDone()

      val results = read(datasetName, bigQueryTable.tableId)
      results.foreach { row =>
        println(BigQueryType[SimpleClass].fromAvro(row))
      }
    }
  }
}
