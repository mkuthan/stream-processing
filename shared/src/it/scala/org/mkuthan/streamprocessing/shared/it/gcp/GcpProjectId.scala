package org.mkuthan.streamprocessing.shared.it.gcp

import com.google.cloud.ServiceOptions

trait GcpProjectId {
  lazy val projectId: String = ServiceOptions.getDefaultProjectId
}
