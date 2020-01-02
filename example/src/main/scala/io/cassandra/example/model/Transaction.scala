package io.cassandra.example.model

import java.time.Instant
import java.time.temporal.ChronoUnit.MINUTES
import java.util.UUID

import cats.ApplicativeError
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.cql.Row
import io.cassandra.error.ConversionError
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

case class Transaction(
  transactionId: UUID,
  accountId: UUID,
  orderAt: Instant,
  amount: BigDecimal,
  paymentMethod: PaymentMethod)

object Transaction {
  implicit val decoder: Decoder[Transaction] = deriveDecoder[Transaction]
  implicit val encoder: Encoder[Transaction] = deriveEncoder[Transaction]

  private[this] def transactionGenerator(accountId: UUID): Gen[Transaction] =
    for {
      transactionId <- Gen.uuid
      orderAt <- Gen
        .chooseNum[Long](0, 518400)
        .map(value => Instant.now.minus(value, MINUTES))
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
    Gen.uuid.flatMap(accountId => Gen.listOfN(25000, transactionGenerator(accountId)))

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
