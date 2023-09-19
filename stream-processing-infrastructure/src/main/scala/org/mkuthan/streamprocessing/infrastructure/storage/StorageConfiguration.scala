package org.mkuthan.streamprocessing.infrastructure.storage

case class StorageConfiguration(
    prefix: Prefix = Prefix.Empty,
    suffix: Suffix = Suffix.Json,
    numShards: NumShards = NumShards.RunnerSpecific
) {
  def withPrefix(prefix: Prefix): StorageConfiguration =
    copy(prefix = prefix)

  def withSuffix(suffix: Suffix): StorageConfiguration =
    copy(suffix = suffix)

  def withNumShards(numShards: NumShards): StorageConfiguration =
    copy(numShards = numShards)

  def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
    params.foldLeft(write)((write, param) => param.configure(write))

  private lazy val params: Set[StorageWriteParam] = Set(
    suffix,
    prefix,
    numShards
  )
}
