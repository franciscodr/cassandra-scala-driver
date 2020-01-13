package io.cassandra

import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource}
import cats.syntax.functor._
import com.datastax.dse.driver.api.core.DseSession
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.dse.driver.api.core.graph.{AsyncGraphResultSet, GraphStatement}
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.{
  PrepareRequest,
  PreparedStatement,
  SimpleStatement,
  Statement
}
import fs2.interop.reactivestreams._

import scala.compat.java8.OptionConverters

class Session[F[_]: ConcurrentEffect](session: DseSession) extends CatsEffectConverters {

  def checkSchemaAgreement: F[Boolean] =
    fromCompletionStage[F](session.checkSchemaAgreementAsync()).map(b => Boolean.box(b))

  def close: F[Unit] = fromCompletionStage[F](session.closeAsync()).as(())

  def execute(query: String): F[AsyncResultSet] =
    fromCompletionStage[F](session.executeAsync(query))
      .map(AsyncResultSet.apply)

  def execute(query: Statement[_]): F[AsyncResultSet] =
    fromCompletionStage[F](session.executeAsync(query))
      .map(AsyncResultSet.apply)

  def execute(query: GraphStatement[_]): F[AsyncGraphResultSet] =
    fromCompletionStage[F](session.executeAsync(query))

  def executeStream(query: String): fs2.Stream[F, ReactiveRow] =
    session.executeReactive(query).toStream[F]

  def executeStream(query: Statement[_]): fs2.Stream[F, ReactiveRow] =
    session.executeReactive(query).toStream[F]

  def keyspace: Option[CqlIdentifier] = OptionConverters.toScala(session.getKeyspace)

  def prepare(query: String): F[PreparedStatement] =
    fromCompletionStage[F](session.prepareAsync(query))

  def prepare(request: PrepareRequest): F[PreparedStatement] =
    fromCompletionStage[F](session.prepareAsync(request))

  def prepare(statement: SimpleStatement): F[PreparedStatement] =
    fromCompletionStage[F](session.prepareAsync(statement))
}

object Session {
  def buildAsStream(keyspace: String, requestPageSize: Int)(
    implicit CS: ContextShift[IO]
  ): fs2.Stream[IO, Session[IO]] =
    Connection
      .buildConnectionAsStream(keyspace, requestPageSize)
      .map(connection => new Session[IO](connection))

  def build(keyspace: String, requestPageSize: Int)(
    implicit CS: ContextShift[IO]
  ): Resource[IO, Session[IO]] =
    Connection
      .buildConnectionAsResource(keyspace, requestPageSize)
      .map(connection => new Session[IO](connection))
}
