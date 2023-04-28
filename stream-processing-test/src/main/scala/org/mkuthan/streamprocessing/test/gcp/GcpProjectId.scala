package org.mkuthan.streamprocessing.test.gcp

import com.google.cloud.ServiceOptions

trait GcpProjectId {
  lazy val projectId: String = ServiceOptions.getDefaultProjectId
}
