package io.cassandra.example

import java.text.NumberFormat
import java.util.UUID

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import io.cassandra.Session
import io.cassandra.example.model.PaymentMethod.CreditCard
import io.cassandra.example.model.TransactionQuery
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object ConsumerProgram extends IOApp {

  private[this] val numberFormat = NumberFormat.getNumberInstance
  numberFormat.setMaximumFractionDigits(2)
  numberFormat.setMinimumFractionDigits(2)
  numberFormat.setMinimumIntegerDigits(2)

  override def run(args: List[String]): IO[ExitCode] = {
    for {
      implicit0(logger: Logger[IO]) <- fs2.Stream.eval(Slf4jLogger.create[IO])
      session <- Session.buildAsStream("customer")
      query <- TransactionQuery.buildAsStream(session)
      count <- query.countTransactionsByAccountIdAndPaymentMethod(
        UUID.fromString("e1e597f7-3470-42a5-8df6-22623faed800"),
        CreditCard)
      _ <- fs2.Stream.eval(logger.info(s"Transactions paid by credit card: $count"))
      _ <- fs2.Stream.eval(logger.info("Latest transactions paid by credit card"))
      transactions <- query
        .selectTransactionsByAccountIdAndPaymentMethod(
          UUID.fromString("e1e597f7-3470-42a5-8df6-22623faed800"),
          List(CreditCard))
        .evalTap(transaction =>
          logger.info(s"Transaction of ${numberFormat
            .format(transaction.amount)} dollars paid by ${transaction.paymentMethod.entryName} at ${transaction.orderAt}"))
    } yield transactions
  }.compile.drain.as(ExitCode.Success)
}
