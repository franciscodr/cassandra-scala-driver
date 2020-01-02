package io.cassandra

import cats.effect.{Async, IO, Resource}
import cats.syntax.functor._
import com.datastax.dse.driver.api.core.DseSession
import com.datastax.dse.driver.api.core.graph.{AsyncGraphResultSet, GraphStatement}
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.PrepareRequest
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.cql.Statement

import scala.compat.java8.OptionConverters

class Session[F[_]: Async](session: DseSession) extends CatsEffectConverters {

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

  def keyspace: Option[CqlIdentifier] = OptionConverters.toScala(session.getKeyspace)

  def prepare(query: String): F[PreparedStatement] =
    fromCompletionStage[F](session.prepareAsync(query))

  def prepare(request: PrepareRequest): F[PreparedStatement] =
    fromCompletionStage[F](session.prepareAsync(request))

  def prepare(statement: SimpleStatement): F[PreparedStatement] =
    fromCompletionStage[F](session.prepareAsync(statement))
}

object Session {
  def buildAsStream(keyspace: String): fs2.Stream[IO, Session[IO]] =
    Connection
      .buildConnectionAsStream(keyspace)
      .map(connection => new Session[IO](connection))

  def build(keyspace: String): Resource[IO, Session[IO]] =
    Connection
      .buildConnectionAsResource(keyspace)
      .map(connection => new Session[IO](connection))
}
