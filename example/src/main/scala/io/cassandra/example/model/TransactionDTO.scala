package io.cassandra.example.model

import java.time.Instant
import java.time.temporal.ChronoUnit.MINUTES
import java.util.UUID

import cats.ApplicativeError
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.cql.Row
import io.cassandra.error.ConversionError
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

import scala.util.Random

case class TransactionDTO(
  transactionId: UUID,
  accountId: UUID,
  orderAt: Instant,
  amount: BigDecimal,
  paymentMethod: PaymentMethod
) {
  def toTransactionResponse: Transaction =
    Transaction(
      transactionId = transactionId.toString,
      accountId = accountId.toString,
      orderAt = orderAt.toString,
      amount = amount,
      paymentMethod = paymentMethod.entryName
    )
}

object TransactionDTO {

  private[this] def transactionGenerator(accountId: UUID): Gen[TransactionDTO] =
    for {
      transactionId <- Gen.uuid
      orderAt <- Gen.const(Instant.now.minus(Random.nextInt(518400).toLong, MINUTES))
      amount <- Gen.chooseNum[Double](1.0, 50.00).map(BigDecimal.valueOf)
      paymentMethod <- arbitrary[PaymentMethod]
    } yield TransactionDTO(
      transactionId,
      accountId,
      orderAt,
      amount,
      paymentMethod
    )

  implicit val generator: Gen[List[TransactionDTO]] =
    Gen.uuid.flatMap(accountId => Gen.listOfN(1000, transactionGenerator(accountId)))

  def fromRow[F[_]](row: Row)(implicit AE: ApplicativeError[F, Throwable]): F[TransactionDTO] =
    AE.fromOption(
        PaymentMethod.withNameOption(row.getString("payment_method")),
        ConversionError("The payment method read from database is not valid")
      )
      .map(paymentMethod =>
        TransactionDTO(
          transactionId = row.getUuid("transaction_id"),
          accountId = row.getUuid("account_id"),
          orderAt = row.getInstant("order_at"),
          amount = row.getBigDecimal("amount"),
          paymentMethod = paymentMethod
        )
      )

}
