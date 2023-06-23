package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead

case class StorageReadConfiguration(
    rowRestriction: RowRestriction = RowRestriction.NoRowRestriction,
    selectedFields: SelectedFields = SelectedFields.NoSelectedFields
) {
  def withRowRestriction(rowRestriction: RowRestriction): StorageReadConfiguration =
    copy(rowRestriction = rowRestriction)

  def withSelectedFields(selectedFields: SelectedFields): StorageReadConfiguration =
    copy(selectedFields = selectedFields)

  def configure[T](read: TypedRead[T]): TypedRead[T] =
    ioParams.foldLeft(read)((read, param) => param.configure(read))

  private lazy val ioParams: Set[StorageReadParam] = Set(
    rowRestriction,
    selectedFields
  )
}
