package org.mkuthan.streamprocessing.shared.test.gcp

import com.google.api.client.http.HttpTransport
import com.google.api.client.json.JsonFactory
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.ServiceOptions
import org.apache.beam.sdk.extensions.gcp.util.Transport

private[gcp] object GoogleJsonClientUtils {

  lazy val projectId: String = ServiceOptions.getDefaultProjectId
  lazy val appName: String = getClass.getName

  lazy val httpTransport: HttpTransport = Transport.getTransport
  lazy val jsonFactory: JsonFactory = Transport.getJsonFactory

  def credentials(scopes: String*): GoogleCredentials = GoogleCredentials
    .getApplicationDefault
    .createScoped(scopes: _*)

  def requestInitializer(credentials: GoogleCredentials) =
    new HttpCredentialsAdapter(credentials)

}
