package org.mkuthan.streamprocessing.shared.it.common

import com.google.cloud.ServiceOptions

trait GcpProjectId {
  lazy val projectId: String = ServiceOptions.getDefaultProjectId
}
