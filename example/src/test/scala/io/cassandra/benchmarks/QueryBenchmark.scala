package io.cassandra.benchmarks

import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import io.cassandra.Session
import io.cassandra.config._
import io.cassandra.example.config._
import io.cassandra.example.model.PaymentMethod.ApplePay
import io.cassandra.example.model.{NativeTransactionQuery, PaymentMethod, TransactionQuery}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

import scala.concurrent.ExecutionContext

object QueryBenchmark extends Bench.LocalTime {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8)))

  val limits: Gen[Long] = Gen.range("limit")(5, 100, 5).map(_.toLong)
  val offsets: Gen[Long] = Gen.range("offset")(0, 1000, 100).map(_.toLong)
  val offsetAndLimit: Gen[(Long, Long)] = Gen.crossProduct(offsets, limits)
  val paymentMethods: Gen[PaymentMethod] =
    Gen.enumeration("payment methods")(PaymentMethod.values: _*)

  implicit val logger: Logger[IO] = Slf4jLogger.create[IO].unsafeRunSync()
  val config: CassandraConfig = loadConfig[IO, CassandraConfig]("example.cassandra").unsafeRunSync()

  val (session, releaseSession) =
    Session.build(config).allocated.unsafeRunSync()

  val (sessionForLargeQueries, releaseSessionForLargeQueries) =
    Session.build(config.withRequestPageSize(5000)).allocated.unsafeRunSync()

  val query: TransactionQuery = TransactionQuery.build(session).unsafeRunSync()

  val queryForLargeQueries: TransactionQuery =
    TransactionQuery.build(sessionForLargeQueries).unsafeRunSync()

  val nativeQuery: NativeTransactionQuery = NativeTransactionQuery.build(session).unsafeRunSync()

  val nativeQueryForLargeQueries: NativeTransactionQuery =
    NativeTransactionQuery.build(sessionForLargeQueries).unsafeRunSync()

  performance of "Cassandra driver" in {
    measure method "selectTransactionsByAccountIdAndPaymentMethod" in {
      using(offsetAndLimit) in {
        case (offset, limit) =>
          logger.debug(s"Using offset $offset and limit $limit...")
          query
            .selectTransactionsByAccountIdAndPaymentMethod(
              UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
              List(ApplePay),
              limit,
              offset
            )
            .compile
            .toList
            .unsafeRunSync()
      }
    }

    measure method "countTransactionsByAccountIdAndPaymentMethod" in {
      using(paymentMethods) in { paymentMethod =>
        logger.debug(s"Using paymentMethod $paymentMethod...")
        queryForLargeQueries
          .countTransactionsByAccountIdAndPaymentMethod(
            UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
            paymentMethod
          )
          .compile
          .toList
          .unsafeRunSync()
      }
    }

    measure method "countByAccountId" in {
      using(Gen.single("AccountId")(UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"))) in {
        accountId =>
          logger.debug(s"Getting count by account id...")
          query
            .countByAccountId(accountId)
            .unsafeRunSync()
      }
    }
  }

  performance of "Cassandra driver with reactive stream" in {
    measure method "selectTransactionsByAccountIdAndPaymentMethod" in {
      using(offsetAndLimit) in {
        case (offset, limit) =>
          logger.debug(s"Using offset $offset and limit $limit...")
          nativeQuery
            .selectTransactionsByAccountIdAndPaymentMethod(
              UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
              List(ApplePay),
              limit,
              offset
            )
            .compile
            .toList
            .unsafeRunSync()
      }
    }

    measure method "countTransactionsByAccountIdAndPaymentMethod" in {
      using(paymentMethods) in { paymentMethod =>
        logger.debug(s"Using paymentMethod $paymentMethod...")
        nativeQueryForLargeQueries
          .countTransactionsByAccountIdAndPaymentMethod(
            UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"),
            paymentMethod
          )
          .compile
          .toList
          .unsafeRunSync()
      }
    }

    measure method "countByAccountId" in {
      using(Gen.single("AccountId")(UUID.fromString("9158b076-38a2-480e-a1fe-32efc2448ac1"))) in {
        accountId =>
          logger.debug(s"Getting count by account id...")
          nativeQuery
            .countByAccountId(accountId)
            .unsafeRunSync()
      }
    }
  }
}
