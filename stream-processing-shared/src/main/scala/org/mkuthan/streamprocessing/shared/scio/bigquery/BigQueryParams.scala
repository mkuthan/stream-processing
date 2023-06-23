package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.jdk.CollectionConverters._

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

sealed trait BigQueryReadParam {
  def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T]
}

sealed trait BigQueryWriteParam {
  def configure[T](tableId: String, write: Write[T]): Write[T]
}

// Common read parameters

case object ReadFrom extends BigQueryReadParam {
  override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
    read.from(tableId)
}

// Common write parameters

case object WriteTo extends BigQueryWriteParam {
  override def configure[T](tableId: String, write: Write[T]): Write[T] =
    write.to(tableId)
}

sealed trait WriteDisposition extends BigQueryWriteParam

object WriteDisposition {
  case object WriteAppend extends WriteDisposition {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
  }

  case object WriteEmpty extends WriteDisposition {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_EMPTY)
  }

  case object WriteTruncate extends WriteDisposition {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withWriteDisposition(Write.WriteDisposition.WRITE_TRUNCATE)
  }
}

sealed trait CreateDisposition extends BigQueryWriteParam

object CreateDisposition {
  case object CreateNever extends CreateDisposition {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
  }

  case object CreateIfNeeded extends CreateDisposition {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withCreateDisposition(Write.CreateDisposition.CREATE_IF_NEEDED)
  }
}

// Storage read API parameters

case object StorageReadMethod extends BigQueryReadParam {
  override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
    read.withMethod(TypedRead.Method.DIRECT_READ)
}

sealed trait RowRestriction extends BigQueryReadParam

object RowRestriction {
  case object NoRowRestriction extends RowRestriction {
    override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] = read
  }

  case class SqlRowRestriction(sql: String) extends RowRestriction {
    override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
      read.withRowRestriction(sql)
  }
}

sealed trait SelectedFields extends BigQueryReadParam

object SelectedFields {
  case object NoSelectedFields extends SelectedFields {
    override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] = read
  }

  case class NamedSelectedFields(fields: List[String]) extends SelectedFields {
    override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
      read.withSelectedFields(fields.asJava)
  }
}

// Storage write API parameters

sealed trait StorageWriteMethod extends BigQueryWriteParam

object StorageWriteMethod {
  case object ExactlyOnce extends StorageWriteMethod {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withMethod(Write.Method.STORAGE_WRITE_API)
  }

  case object AtLeastOnce extends StorageWriteMethod {
    override def configure[T](tableId: String, write: Write[T]): Write[T] =
      write.withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
  }
}

// File loads parameters

case object FileLoadsWriteMethod extends BigQueryWriteParam {
  override def configure[T](tableId: String, write: Write[T]): Write[T] =
    write.withMethod(Write.Method.FILE_LOADS)
}

// Export parameters

case object ExportReadMethod extends BigQueryReadParam {
  override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
    read.withMethod(TypedRead.Method.EXPORT)
}

sealed trait ExportQuery extends BigQueryReadParam

object ExportQuery {
  case object NoQuery extends ExportQuery {
    override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
      read.from(tableId)
  }

  case class SqlQuery(sql: String) extends ExportQuery {
    override def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
      read.fromQuery(sql)
  }
}
