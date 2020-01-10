package io.cassandra.example

import java.text.NumberFormat
import java.util.UUID

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import io.cassandra.Session
import io.cassandra.example.model.PaymentMethod.{ApplePay, Cash, CreditCard, GooglePay, SvcCard}
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
      session <- Session.buildAsStream("customer", 500)
      query <- TransactionQuery.buildAsStream(session)
      applePayCount <- query.nativeCountTransactionsByAccountIdAndPaymentMethod(
        UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
        ApplePay)
      _ <- fs2.Stream.eval(logger.info(s"Transactions paid by ApplePay: $applePayCount"))
      cashCount <- query.nativeCountTransactionsByAccountIdAndPaymentMethod(
        UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
        Cash)
      _ <- fs2.Stream.eval(logger.info(s"Transactions paid by cash: $cashCount"))
      creditCardCount <- query.nativeCountTransactionsByAccountIdAndPaymentMethod(
        UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
        CreditCard)
      _ <- fs2.Stream.eval(logger.info(s"Transactions paid by credit card: $creditCardCount"))
      googlePayCount <- query.nativeCountTransactionsByAccountIdAndPaymentMethod(
        UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
        GooglePay)
      _ <- fs2.Stream.eval(logger.info(s"Transactions paid by GooglePay: $googlePayCount"))
      svcCardCount <- query.nativeCountTransactionsByAccountIdAndPaymentMethod(
        UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
        SvcCard)
      _ <- fs2.Stream.eval(logger.info(s"Transactions paid by SVC card: $svcCardCount"))
      _ <- fs2.Stream.eval(logger.info("Latest transactions paid by credit card"))
      _ <- query
        .nativeSelectTransactionsByAccountIdAndPaymentMethod(
          accountId = UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
          paymentMethods = List(CreditCard))
        .evalTap(transaction =>
          logger.info(s"Transaction of ${numberFormat
            .format(transaction.amount)} dollars paid by ${transaction.paymentMethod.entryName} at ${transaction.orderAt}"))
    } yield ()
  }.compile.drain.as(ExitCode.Success)
}
