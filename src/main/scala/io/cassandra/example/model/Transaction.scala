package io.cassandra.example.model

import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS
import java.util.UUID

import cats.ApplicativeError
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.cql.Row
import io.cassandra.error.ConversionError
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

case class Transaction(
  transactionId: UUID,
  accountId: UUID,
  orderAt: Instant,
  amount: BigDecimal,
  paymentMethod: PaymentMethod)

object Transaction {
  private[this] def transactionGenerator(accountId: UUID): Gen[Transaction] =
    for {
      transactionId <- Gen.uuid
      orderAt <- Gen
        .chooseNum[Long](0, 360)
        .map(value => Instant.now.minus(value, DAYS))
      amount <- Gen.chooseNum[Double](1.0, 50.00).map(BigDecimal.valueOf)
      paymentMethod <- arbitrary[PaymentMethod]
    } yield
      Transaction(
        transactionId,
        accountId,
        orderAt,
        amount,
        paymentMethod,
      )

  implicit val generator: Gen[List[Transaction]] =
    Gen.uuid.flatMap(accountId => Gen.listOfN(10000, transactionGenerator(accountId)))

  def fromRow[F[_]](row: Row)(implicit AE: ApplicativeError[F, Throwable]): F[Transaction] =
    AE.fromOption(
        PaymentMethod.withNameOption(row.getString("payment_method")),
        ConversionError("The payment method read from database is not valid"))
      .map(paymentMethod =>
        Transaction(
          transactionId = row.getUuid("transaction_id"),
          accountId = row.getUuid("account_id"),
          orderAt = row.getInstant("order_at"),
          amount = row.getBigDecimal("amount"),
          paymentMethod = paymentMethod
      ))

}
