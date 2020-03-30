package io.cassandra.example.http

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import cats.effect.IO
import io.cassandra.example.http.auth.HttpRoutesWithBasicAuthentication
import io.cassandra.example.http.codecs._
import io.cassandra.example.http.model.Transaction
import io.cassandra.example.model._
import io.circe.Encoder
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._

import scala.xml.NamespaceBinding

class ServiceApi(query: TransactionQuery) extends HeaderExtractors with QueryParamMatchers {
  val httpService: HttpRoutes[IO] = HttpRoutesWithBasicAuthentication {
    case authedRequest @ GET -> Root / "order" / UUIDVar(accountId)
          :? PaymentMethodsQueryParamMatcher(paymentMethods)
            +& LimitQueryParamMatcher(limit)
            +& OffsetQueryParamMatcher(offset) as account =>
      getTransactionsByAccountIdAndPaymentMethod(
        accountId,
        paymentMethods.getOrElse(Nil),
        extractIfModifiedSinceHeader(authedRequest.req),
        limit.getOrElse(50),
        offset.getOrElse(0)
      )
  }

  def buildResponse[A: Encoder](data: List[A]): IO[Response[IO]] =
    NonEmptyList.fromList(data).fold(NotFound())(list => Ok(list.asJson))

  def getTransactionsByAccountIdAndPaymentMethod(
    accountId: UUID,
    paymentMethods: List[PaymentMethod],
    modifiedSince: Option[Instant],
    limit: Long,
    offset: Long
  ): IO[Response[IO]] = {
    val transactionStream: fs2.Stream[IO, Transaction] = modifiedSince
      .fold(
        query
          .selectTransactionsByAccountIdAndPaymentMethod(accountId, paymentMethods, limit, offset)
      )(date =>
        query
          .selectTransactionsByAccountIdAndPaymentMethodAndModifiedSince(
            accountId,
            date,
            paymentMethods,
            limit,
            offset
          )
      )
      .map(_.toTransactionResponse)
    for {
      transactions <- transactionStream.compile.toList
      response <- buildResponse(transactions)
    } yield response
  }.handleErrorWith { error =>
    InternalServerError(error.getMessage)
  }
}
