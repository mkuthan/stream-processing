package org.mkuthan.streamprocessing.infrastructure.bigquery

import scala.jdk.CollectionConverters._

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

sealed trait BigQueryReadParam {
  def configure[T](read: TypedRead[T]): TypedRead[T]
}

sealed trait BigQueryWriteParam {
  def configure[T](write: Write[T]): Write[T]
}

sealed trait WriteDisposition extends BigQueryWriteParam

object WriteDisposition {
  case object Append extends WriteDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
  }

  case object Empty extends WriteDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_EMPTY)
  }

  case object Truncate extends WriteDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_TRUNCATE)
  }
}

case object CreateDispositionNever extends BigQueryWriteParam {
  override def configure[T](write: Write[T]): Write[T] =
    write.withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
}

sealed trait RowRestriction extends BigQueryReadParam

object RowRestriction {
  case object NoRestriction extends RowRestriction {
    override def configure[T](read: TypedRead[T]): TypedRead[T] = read
  }

  case class SqlRestriction(sql: String) extends RowRestriction {
    override def configure[T](read: TypedRead[T]): TypedRead[T] =
      read.withRowRestriction(sql)
  }
}

sealed trait SelectedFields extends BigQueryReadParam

object SelectedFields {
  case object NoFields extends SelectedFields {
    override def configure[T](read: TypedRead[T]): TypedRead[T] = read
  }

  case class NamedFields(fields: List[String]) extends SelectedFields {
    override def configure[T](read: TypedRead[T]): TypedRead[T] =
      read.withSelectedFields(fields.asJava)
  }
}

case object StorageReadMethod extends BigQueryReadParam {
  override def configure[T](read: TypedRead[T]): TypedRead[T] =
    read.withMethod(TypedRead.Method.DIRECT_READ)
}

case object ExportReadMethod extends BigQueryReadParam {
  override def configure[T](read: TypedRead[T]): TypedRead[T] =
    read.withMethod(TypedRead.Method.EXPORT)
}

case object StorageWriteAtLeastOnceMethod extends BigQueryWriteParam {
  override def configure[T](write: Write[T]): Write[T] =
    write.withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
}

case object FileLoadsWriteMethod extends BigQueryWriteParam {
  override def configure[T](write: Write[T]): Write[T] =
    write.withMethod(Write.Method.FILE_LOADS)
}
