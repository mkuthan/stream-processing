package org.mkuthan.streamprocessing.shared.scio.dlq

import org.apache.beam.sdk.io.FileIO

sealed trait StorageWriteParam {
  def configure(write: StorageWriteParam.Type): StorageWriteParam.Type
}

object StorageWriteParam {
  type Type = FileIO.Write[Void, String]
}

sealed trait NumShards extends StorageWriteParam

object NumShards {
  case class Explicit(value: Int) extends NumShards {
    require(value > 0)

    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withNumShards(value)
  }

  case object RunnerSpecific extends NumShards {
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withNumShards(0)
  }

  val One: NumShards = Explicit(1)
}

sealed trait Prefix extends StorageWriteParam

object Prefix {
  object Empty extends Prefix {
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withPrefix("")
  }

  case class Explicit(value: String) extends Prefix {
    require(value.nonEmpty)
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withPrefix(value)
  }
}

case object JsonSuffix extends StorageWriteParam {
  override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
    write.withSuffix(".json")
}
