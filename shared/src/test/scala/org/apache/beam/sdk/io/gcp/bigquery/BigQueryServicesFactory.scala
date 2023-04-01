package org.apache.beam.sdk.io.gcp.bigquery

/**
 * Wrapper for BigQueryServicesImpl with relaxed access modifiers.
 */
object BigQueryServicesFactory extends BigQueryServices {
  override def getJobService(options: BigQueryOptions): BigQueryServices.JobService =
    new BigQueryServicesImpl().getJobService(options)

  override def getDatasetService(options: BigQueryOptions): BigQueryServices.DatasetService =
    new BigQueryServicesImpl().getDatasetService(options)

  override def getStorageClient(options: BigQueryOptions): BigQueryServices.StorageClient =
    new BigQueryServicesImpl().getStorageClient(options)
}
