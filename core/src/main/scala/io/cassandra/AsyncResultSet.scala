package io.cassandra

import cats.ApplicativeError
import cats.effect.Async
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import com.datastax.oss.driver.api.core.cql.{Row, AsyncResultSet => JavaAsyncResultSet}
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

import scala.collection.JavaConverters._

case class AsyncResultSet(resultSet: JavaAsyncResultSet) extends AnyVal {

  def asStream[F[_]: Async: Logger]: Stream[F, Row] =
    fs2.Stream
      .unfoldEval(Option(this)) {
        case Some(resultSet) =>
          if (resultSet.hasMorePages)
            Logger[F]
              .debug("Fetching from database")
              .flatMap(
                _ =>
                  resultSet
                    .fetchNextPage[F]
                    .map { rs =>
                      (
                        resultSet.currentPage,
                        rs.some
                      ).some
                  }
              )
          else
            (resultSet.currentPage, none[AsyncResultSet]).some.pure[F]
        case None =>
          none[(Iterator[Row], Option[AsyncResultSet])].pure[F]
      }
      .flatMap(fs2.Stream.fromIterator[F].apply)

  def currentPage: Iterator[Row] = resultSet.currentPage().asScala.iterator

  def fetchNextPage[F[_]: Async]: F[AsyncResultSet] =
    CatsEffectConverters
      .fromCompletionStage[F](resultSet.fetchNextPage())
      .map(AsyncResultSet.apply)

  def hasMorePages: Boolean = resultSet.hasMorePages

  def one: Row = resultSet.one()

  def oneOrError[F[_]](error: Throwable)(implicit AE: ApplicativeError[F, Throwable]): F[Row] = {
    val row = resultSet.one()
    if (row == null) AE.raiseError(error) else AE.pure(row)
  }

  def oneOrNone: Option[Row] = Option(resultSet.one())

  def wasApplied: Boolean = resultSet.wasApplied()
}
