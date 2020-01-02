package io.cassandra

import cats.effect.{IO, Resource}
import cats.syntax.functor._
import com.datastax.dse.driver.api.core._
import com.datastax.dse.driver.api.core.config.DseDriverConfigLoader
import com.datastax.dse.driver.internal.core.loadbalancing.DseLoadBalancingPolicy
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.oss.driver.api.core.config._
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy

import scala.collection.JavaConverters._

object Connection extends CatsEffectConverters {
  def buildConnectionAsStream(keyspace: String): fs2.Stream[IO, DseSession] = {
    val driverConfigLoader = DseDriverConfigLoader.programmaticBuilder
      .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, classOf[DsePlainTextAuthProvider])
      .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
      .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra")
      .withString(DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.LOCAL_ONE.name)
      .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 500)
      .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, classOf[DseLoadBalancingPolicy])
      .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, classOf[DefaultRetryPolicy])
      .withStringList(DefaultDriverOption.CONTACT_POINTS, List("localhost").asJava)
      .build

    fs2.Stream.bracket(
      fromCompletionStage[IO](
        DseSession.builder
          .withConfigLoader(driverConfigLoader)
          .withKeyspace(keyspace)
          .buildAsync))(session =>
      fromCompletionStage[IO](session.closeAsync.toCompletableFuture).as(()))
  }

  def buildConnectionAsResource(keyspace: String): Resource[IO, DseSession] = {
    val driverConfigLoader = DseDriverConfigLoader.programmaticBuilder
      .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, classOf[DsePlainTextAuthProvider])
      .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
      .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra")
      .withString(DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.LOCAL_ONE.name)
      .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 500)
      .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, classOf[DseLoadBalancingPolicy])
      .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, classOf[DefaultRetryPolicy])
      .withStringList(DefaultDriverOption.CONTACT_POINTS, List("localhost").asJava)
      .build

    Resource.make(
      fromCompletionStage[IO](
        DseSession.builder
          .withConfigLoader(driverConfigLoader)
          .withKeyspace(keyspace)
          .buildAsync))(session =>
      fromCompletionStage[IO](session.closeAsync.toCompletableFuture).as(()))
  }
}
