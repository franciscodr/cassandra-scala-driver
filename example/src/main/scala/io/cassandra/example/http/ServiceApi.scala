package io.cassandra.example.http

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import io.cassandra.example.http.auth.HttpRoutesWithBasicAuthentication
import io.cassandra.example.http.codecs._
import io.cassandra.example.model._
import io.circe.Encoder
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._
import scalaxb.XMLFormat

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
        extractAcceptHeader(authedRequest.req),
        limit.getOrElse(50),
        offset.getOrElse(0)
      )
  }

  def buildResponse[A: Encoder: XMLFormat](contentType: ContentType, data: A): IO[Response[IO]] = {
    contentType match {
      case Json => Ok(data.asJson)
      case Xml =>
        Ok(
          scalaxb
            .toXML(
              data,
              namespace = None,
              elementLabel = "transactions",
              scope = NamespaceBinding(
                prefix = null,
                uri = "http://schemas.datacontract.org/2020/01/Transaction.Model",
                parent = w3orgScope
              )
            )
        )
    }
  }

  def getTransactionsByAccountIdAndPaymentMethod(
    accountId: UUID,
    paymentMethods: List[PaymentMethod],
    modifiedSince: Option[Instant],
    contentType: ContentType,
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
      response <- buildResponse(contentType, Transactions(transactions))
    } yield response
  }.handleErrorWith { error =>
    InternalServerError(error.getMessage)
  }
}
