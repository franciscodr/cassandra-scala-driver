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
    Session.build("customer").allocated.unsafeRunSync()
  val query: TransactionQuery = TransactionQuery.build(session).unsafeRunSync()

  performance of "Cassandra driver" in {
    measure method "selectTransactionsByAccountIdAndPaymentMethod" in {
      using(offsetAndLimit) in {
        case (offset, limit) =>
          logger.debug(s"Using offset $offset and limit $limit...")
          query
            .selectTransactionsByAccountIdAndPaymentMethod(
              UUID.fromString("e1e597f7-3470-42a5-8df6-22623faed800"),
              List(CreditCard),
              limit,
              offset)
            .compile
            .toList
            .unsafeRunSync()
      }
    }

    measure method "countTransactionsByAccountIdAndPaymentMethod" in {
      using(paymentMethods) in { paymentMethod =>
        logger.debug(s"Using paymentMethod $paymentMethod...")
        query
          .countTransactionsByAccountIdAndPaymentMethod(
            UUID.fromString("e1e597f7-3470-42a5-8df6-22623faed800"),
            paymentMethod)
          .compile
          .toList
          .unsafeRunSync()
      }
    }

    measure method "countByAccountId" in {
      using(Gen.single("AccountId")(UUID.fromString("e1e597f7-3470-42a5-8df6-22623faed800"))) in {
        accountId =>
          logger.debug(s"Getting count by account id...")
          query
            .countByAccountId(accountId)
            .unsafeRunSync()
      }
    }
  }
}
