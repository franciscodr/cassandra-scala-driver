package io.cassandra.example

import java.time.Instant
import java.util.UUID

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import io.cassandra.Session
import io.cassandra.example.model.PaymentMethod.{ApplePay, Cash, CreditCard, GooglePay, SvcCard}
import io.cassandra.example.model.TransactionQuery
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object ConsumerProgram extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    for {
      implicit0(logger: Logger[IO]) <- fs2.Stream.eval(Slf4jLogger.create[IO])
      session <- Session.buildAsStream("customers")
      query <- TransactionQuery.buildAsStream(session)
      _ <- query
        .countTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
          ApplePay)
        .evalTap(count => logger.info(count + " transactions paid with ApplePay were found"))
      _ <- query
        .countTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
          Cash)
        .evalTap(count => logger.info(count + " transactions paid with cash were found"))
      _ <- query
        .countTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
          CreditCard)
        .evalTap(count => logger.info(count + " transactions paid with credit card were found"))
      _ <- query
        .countTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
          GooglePay)
        .evalTap(count => logger.info(count + " transactions paid with GooglePay were found"))
      _ <- query
        .countTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
          SvcCard)
        .evalTap(count => logger.info(count + " transactions paid with SVC card were found"))
      transactions <- query
        .selectTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
          CreditCard,
          50,
          1)
        .evalTap(txn => logger.info("Transaction: " + txn.transactionId + " - " + txn.orderAt))
      _ <- fs2.Stream
        .eval(query.selectTransactionByPrimaryKey(
          UUID.fromString("1e0a8d98-2e3f-4528-a1d0-f8d167578ae0"),
          Instant.parse("2019-12-25T20:43:18.807Z"),
          UUID.fromString("b39ec61d-29d3-4d58-bced-4d00f17038af")
        ))
        .evalTap(txn =>
          logger.info("Transaction found!: " + txn.transactionId + " - " + txn.orderAt))
    } yield transactions
  }.compile.drain.as(ExitCode.Success)
}
