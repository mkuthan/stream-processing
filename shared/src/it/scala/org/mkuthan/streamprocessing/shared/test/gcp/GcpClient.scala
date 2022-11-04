package org.mkuthan.streamprocessing.shared.test.gcp

import java.util.UUID

import com.google.api.client.http.HttpTransport
import com.google.api.client.json.JsonFactory
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.ServiceOptions
import org.apache.beam.sdk.extensions.gcp.util.Transport

trait GcpClient {

  private[gcp] lazy val projectId: String = ServiceOptions.getDefaultProjectId
  private[gcp] lazy val appName: String = getClass.getName

  private[gcp] lazy val httpTransport: HttpTransport = Transport.getTransport
  private[gcp] lazy val jsonFactory: JsonFactory = Transport.getJsonFactory

  private[gcp] def credentials(scopes: String*): GoogleCredentials = GoogleCredentials
    .getApplicationDefault
    .createScoped(scopes: _*)

  private[gcp] def requestInitializer(credentials: GoogleCredentials) =
    new HttpCredentialsAdapter(credentials)

  private[gcp] def randomString(): String =
    UUID.randomUUID.toString

  private[gcp] def randomStringUnderscored(): String =
    randomString.replace('-', '_')
}
