package io.cassandra.benchmarks

import java.util.UUID

import cats.effect.IO
import io.cassandra.Session
import io.cassandra.example.model.PaymentMethod.CreditCard
import io.cassandra.example.model.{PaymentMethod, TransactionQuery}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

object QueryBenchmark extends Bench.LocalTime {

  val limits: Gen[Long] = Gen.range("limit")(5, 100, 5).map(_.toLong)
  val offsets: Gen[Long] = Gen.range("offset")(0, 10, 1).map(_.toLong)
  val offsetAndLimit: Gen[(Long, Long)] = Gen.crossProduct(offsets, limits)
  val paymentMethods: Gen[PaymentMethod] =
    Gen.enumeration("payment methods")(PaymentMethod.values: _*)

  implicit val logger: Logger[IO] = Slf4jLogger.create[IO].unsafeRunSync()
  val (session, releaseSession) =
    Session.build("customers").allocated.unsafeRunSync()
  val query: TransactionQuery = TransactionQuery.build(session).unsafeRunSync()

  performance of "Cassandra driver" in {
    measure method "selectTransactionsByAccountIdAndPaymentMethod" in {
      using(offsetAndLimit) in {
        case (offset, limit) =>
          println(s"Using offset $offset and limit $limit...")
          query
            .selectTransactionsByAccountIdAndPaymentMethod(
              UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
              CreditCard,
              limit,
              offset)
            .compile
            .toList
            .unsafeRunSync()
      }
    }

    measure method "countTransactionsByAccountIdAndPaymentMethod" in {
      using(paymentMethods) in { paymentMethod =>
        println(s"Using paymentMethod $paymentMethod...")
        query
          .countTransactionsByAccountIdAndPaymentMethod(
            UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"),
            paymentMethod)
          .compile
          .toList
          .unsafeRunSync()
      }
    }

    measure method "countByAccountId" in {
      using(
        Gen.single("AccountId")(
          UUID.fromString("bf010fcc-928a-42ed-995c-577171e07ae9"))) in {
        accountId =>
          println(s"Getting count by account id...")
          query
            .countByAccountId(accountId)
            .unsafeRunSync()
      }
    }
  }
}
