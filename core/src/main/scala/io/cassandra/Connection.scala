package io.cassandra

import cats.effect.{IO, Resource}
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.config._
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy
import io.cassandra.config.CassandraConfig
import io.cassandra.syntax.cqlSessionBuilder._

import scala.jdk.CollectionConverters._

object Connection extends CatsEffectConverters {
  private[this] def buildDriverConfigLoader(config: CassandraConfig) =
    DriverConfigLoader.programmaticBuilder
      .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, classOf[PlainTextAuthProvider])
      .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, config.username.value)
      .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, config.password.value)
      .withString(DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.LOCAL_ONE.name)
      .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, config.requestPageSize)
      .withClass(
        DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
        classOf[DefaultLoadBalancingPolicy]
      )
      .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, classOf[DefaultRetryPolicy])
      .build

  def buildConnectionAsStream(config: CassandraConfig): fs2.Stream[IO, CqlSession] =
    fs2.Stream.bracket(
      fromCompletionStage[IO](
        CqlSession.builder
          .addContactPoints(config.hosts.toList.asJava)
          .withLocalDatacenter(config.localDataCenter.value)
          .withConfigLoader(buildDriverConfigLoader(config))
          .withKeyspace(config.keyspace.value)
          .withSslContextFromConfig(config.ssl)
          .buildAsync
      )
    )(session => fromCompletionStage[IO](session.closeAsync.toCompletableFuture).void)

  def buildConnectionAsResource(config: CassandraConfig): Resource[IO, CqlSession] =
    Resource.make(
      fromCompletionStage[IO](
        CqlSession.builder
          .addContactPoints(config.hosts.toList.asJava)
          .withLocalDatacenter(config.localDataCenter.value)
          .withConfigLoader(buildDriverConfigLoader(config))
          .withKeyspace(config.keyspace.value)
          .withSslContextFromConfig(config.ssl)
          .buildAsync
      )
    )(session => fromCompletionStage[IO](session.closeAsync.toCompletableFuture).void)
}
