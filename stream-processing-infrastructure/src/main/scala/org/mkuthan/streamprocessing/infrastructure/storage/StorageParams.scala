package org.mkuthan.streamprocessing.infrastructure.storage

import org.apache.beam.sdk.io.FileIO

sealed trait StorageWriteParam {
  def configure(write: StorageWriteParam.Type): StorageWriteParam.Type
}

object StorageWriteParam {
  type Type = FileIO.Write[Void, String]
}

sealed trait NumShards extends StorageWriteParam

object NumShards {
  final case class Explicit(value: Int) extends NumShards {
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

  final case class Explicit(value: String) extends Prefix {
    require(value.nonEmpty)
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withPrefix(value)
  }
}

sealed trait Suffix extends StorageWriteParam

object Suffix {
  object Empty extends Suffix {
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withSuffix("")
  }

  final case class Explicit(value: String) extends Suffix {
    require(value.nonEmpty)
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withSuffix(value)
  }

  case object Json extends Suffix {
    override def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
      write.withSuffix(".json")
  }
}
