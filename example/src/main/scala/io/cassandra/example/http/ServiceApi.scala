package io.cassandra.example.http

import java.util.UUID

import cats.effect.IO
import io.cassandra.example.model.{Transaction, TransactionQuery}
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._

class ServiceApi(query: TransactionQuery) extends QueryParamMatchers {

  implicit def transactionEncoder: EntityEncoder[IO, Transaction] = jsonEncoderOf[IO, Transaction]

  implicit def transactionsEncoder: EntityEncoder[IO, List[Transaction]] =
    jsonEncoderOf[IO, List[Transaction]]

  val httpService: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case _ @GET -> Root / "order" / accountId :? PaymentMethodsQueryParamMatcher(paymentMethods) +& LimitQueryParamMatcher(
          limit) +& OffsetQueryParamMatcher(offset) => {
      for {
        transactions <- query
          .selectTransactionsByAccountIdAndPaymentMethod(
            UUID.fromString(accountId),
            paymentMethods.getOrElse(Nil),
            limit.getOrElse(50),
            offset.getOrElse(0))
          .compile
          .toList
        response <- Ok(transactions)
      } yield response
    }.handleErrorWith { error =>
      InternalServerError(error.getMessage)
    }
  }
}
