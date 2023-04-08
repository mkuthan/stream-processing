package org.mkuthan.streamprocessing.shared.configuration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class BigQueryTableTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {
  val specs = Table(
    ("id", "project", "dataset", "table"),
    ("project:dataset.table", "project", "dataset", "table"),
    ("project.dataset.table", "project", "dataset", "table"),
    ("dataset.table", null, "dataset", "table")
  )

  forAll(specs) { (id, project, dataset, table) =>
    s"BigQuery $id" should s"be parsed into $project, $dataset and $table" in {
      val bqTable = BigQueryTable(id)

      bqTable.projectName should be(project)
      bqTable.datasetName should be(dataset)
      bqTable.tableName should be(table)
    }
  }
}
