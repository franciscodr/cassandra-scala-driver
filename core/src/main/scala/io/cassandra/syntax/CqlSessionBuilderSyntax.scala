package io.cassandra.syntax

import java.io.ByteArrayInputStream
import java.security.{KeyStore, SecureRandom}

import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.cassandra.config.SslConfig
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import org.apache.commons.codec.binary.Base64

object cqlSessionBuilder extends CqlSessionBuilderSyntax

trait CqlSessionBuilderSyntax {
  implicit class CqlSessionBuilderOps(builder: CqlSessionBuilder) {
    def withSslContextFromConfig(config: Option[SslConfig]): CqlSessionBuilder = {
      config.fold(builder) { sslConfig =>
        val ks = KeyStore.getInstance("JKS")
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        val trustStore =
          new ByteArrayInputStream(Base64.decodeBase64(sslConfig.trustStoreBase64.stripLineEnd))

        ks.load(trustStore, sslConfig.trustStorePassword.toCharArray)
        tmf.init(ks)

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, tmf.getTrustManagers, new SecureRandom())

        trustStore.close()
        builder.withSslContext(sslContext)
      }
    }
  }
}
