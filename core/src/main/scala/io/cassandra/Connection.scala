package io.cassandra

import java.net.InetSocketAddress

import cats.effect.{IO, Resource}
import cats.syntax.functor._
import com.datastax.dse.driver.api.core._
import com.datastax.dse.driver.api.core.config.DseDriverConfigLoader
import com.datastax.dse.driver.internal.core.loadbalancing.DseLoadBalancingPolicy
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.oss.driver.api.core.config._
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy
import io.cassandra.config.{CassandraConfig, SslConfig}
import io.cassandra.syntax.dseSessionBuilder._

import scala.collection.JavaConverters._

object Connection extends CatsEffectConverters {
  def buildConnectionAsStream(
    config: CassandraConfig,
    requestPageSize: Int
  ): fs2.Stream[IO, DseSession] = {
    val driverConfigLoader = DseDriverConfigLoader.programmaticBuilder
      .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, classOf[DsePlainTextAuthProvider])
      .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, config.username)
      .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, config.passwordKey)
      .withString(DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.LOCAL_ONE.name)
      .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, requestPageSize)
      .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, classOf[DseLoadBalancingPolicy])
      .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, classOf[DefaultRetryPolicy])
      .build

    val sslSettings =
      if (config.enableSSL)
        Some(SslConfig(config.truststoreBase64, config.truststorePasswordKey))
      else
        None

    fs2.Stream.bracket(
      fromCompletionStage[IO](
        DseSession.builder
          .addContactPoints(
            config.hosts.split(",").map(InetSocketAddress.createUnresolved(_, 9042)).toList.asJava
          )
          .withLocalDatacenter(config.localDataCenter)
          .withConfigLoader(driverConfigLoader)
          .withKeyspace(config.keyspace)
          .withSslContextFromConfig(sslSettings)
          .buildAsync
      )
    )(session => fromCompletionStage[IO](session.closeAsync.toCompletableFuture).void)
  }

  def buildConnectionAsResource(
    config: CassandraConfig,
    requestPageSize: Int
  ): Resource[IO, DseSession] = {
    val driverConfigLoader = DseDriverConfigLoader.programmaticBuilder
      .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, classOf[DsePlainTextAuthProvider])
      .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, config.username)
      .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, config.passwordKey)
      .withString(DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.LOCAL_ONE.name)
      .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, requestPageSize)
      .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, classOf[DseLoadBalancingPolicy])
      .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, classOf[DefaultRetryPolicy])
      .withStringList(DefaultDriverOption.CONTACT_POINTS, config.hosts.split(",").toList.asJava)
      .build

    val sslSettings =
      if (config.enableSSL)
        Some(SslConfig(config.truststoreBase64, config.truststorePasswordKey))
      else
        None

    Resource.make(
      fromCompletionStage[IO](
        DseSession.builder
          .addContactPoints(
            config.hosts.split(",").map(InetSocketAddress.createUnresolved(_, 9042)).toList.asJava
          )
          .withConfigLoader(driverConfigLoader)
          .withKeyspace(config.keyspace)
          .withSslContextFromConfig(sslSettings)
          .buildAsync
      )
    )(session => fromCompletionStage[IO](session.closeAsync.toCompletableFuture).void)
  }
}
