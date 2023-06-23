package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.jdk.CollectionConverters._

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

sealed trait BigQueryReadParam {
  def configure[T](read: TypedRead[T]): TypedRead[T]
}

sealed trait BigQueryWriteParam {
  def configure[T](write: Write[T]): Write[T]
}

sealed trait ExportParam {
  // TODO: passing tableId doesn't match the convention but it's required to configure the export
  def configure[T](read: TypedRead[T], tableId: String): TypedRead[T]
}

sealed trait RowRestriction extends BigQueryReadParam

object RowRestriction {
  case object NoRowRestriction extends RowRestriction {
    override def configure[T](read: TypedRead[T]): TypedRead[T] = read
  }

  case class SqlRowRestriction(sql: String) extends RowRestriction {
    override def configure[T](read: TypedRead[T]): TypedRead[T] =
      read.withRowRestriction(sql)
  }
}

sealed trait SelectedFields extends BigQueryReadParam

object SelectedFields {
  case object NoSelectedFields extends SelectedFields {
    override def configure[T](read: TypedRead[T]): TypedRead[T] = read
  }

  case class NamedSelectedFields(fields: List[String]) extends SelectedFields {
    override def configure[T](read: TypedRead[T]): TypedRead[T] =
      read.withSelectedFields(fields.asJava)
  }
}

sealed trait StorageWriteMethod extends BigQueryWriteParam

object StorageWriteMethod {
  case object ExactlyOnce extends StorageWriteMethod {
    override def configure[T](write: Write[T]): Write[T] =
      write.withMethod(Write.Method.STORAGE_WRITE_API)
  }

  case object AtLeastOnce extends StorageWriteMethod {
    override def configure[T](write: Write[T]): Write[T] =
      write.withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
  }
}

sealed trait WriteDisposition extends BigQueryWriteParam

object WriteDisposition {
  case object WriteAppend extends WriteDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
  }

  case object WriteEmpty extends WriteDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_EMPTY)
  }

  case object WriteTruncate extends WriteDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_TRUNCATE)
  }
}

sealed trait CreateDisposition extends BigQueryWriteParam

object CreateDisposition {
  case object CreateNever extends CreateDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
  }

  case object CreateIfNeeded extends CreateDisposition {
    override def configure[T](write: Write[T]): Write[T] =
      write.withCreateDisposition(Write.CreateDisposition.CREATE_IF_NEEDED)
  }
}

sealed trait ExportQuery extends ExportParam

object ExportQuery {
  case object NoQuery extends ExportQuery {
    override def configure[T](read: TypedRead[T], tableId: String): TypedRead[T] =
      read.from(tableId)
  }

  case class SqlQuery(sql: String) extends ExportQuery {
    override def configure[T](read: TypedRead[T], tableId: String): TypedRead[T] =
      read.fromQuery(sql)
  }
}
