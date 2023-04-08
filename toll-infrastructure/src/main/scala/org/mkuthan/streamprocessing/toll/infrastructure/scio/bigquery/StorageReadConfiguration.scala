package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead

case class StorageReadConfiguration(
    rowRestriction: RowRestriction = RowRestriction.NoRowRestriction,
    selectedFields: SelectedFields = SelectedFields.NoSelectedFields
) {
  def withRowRestriction(rowRestriction: RowRestriction): StorageReadConfiguration =
    copy(rowRestriction = rowRestriction)

  def withSelectedFields(selectedFields: SelectedFields): StorageReadConfiguration =
    copy(selectedFields = selectedFields)

  def configure[T](write: TypedRead[T]): TypedRead[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(write))

  private lazy val ioParams: Set[StorageReadParam] = Set(
    rowRestriction,
    selectedFields
  )
}
