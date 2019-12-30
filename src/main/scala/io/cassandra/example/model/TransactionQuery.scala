package io.cassandra.example.model

import java.time.Instant
import java.util.UUID

import cats.effect.{IO, Sync}
import cats.effect.IO._
import cats.effect.concurrent.Ref
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker
import io.cassandra.error.ResultNotFound
import io.cassandra.{AsyncResultSet, CatsEffectConverters, Session}
import io.chrisdavenport.log4cats.Logger

final class Counter[F[_]](private val ref: Ref[F, Long]) {
  def increment: F[Unit] = ref.update(_ + 1)

  def decrement: F[Unit] = ref.update(_ - 1)

  def get: F[Long] = ref.get
}

object Counter {
  def apply[F[_]: Sync]: F[Counter[F]] = Ref.of[F, Long](0L).map(new Counter(_))
}

case class TransactionQuery(
  session: Session[IO],
  countStatement: PreparedStatement,
  insertStatement: PreparedStatement,
  selectStatement: PreparedStatement,
  selectStatementByPrimaryKey: PreparedStatement)(
  implicit logger: Logger[IO]
) extends CatsEffectConverters {

  def countByAccountId(accountId: UUID): IO[Long] =
    session
      .execute(
        countStatement
          .bind()
          .setUuid("account_id", accountId)
      )
      .map(_.oneOrNone.map(_.getLong("count")).getOrElse(0L))

  def insertTransaction(transaction: Transaction): IO[AsyncResultSet] =
    session
      .execute(
        insertStatement
          .bind()
          .setUuid("account_id", transaction.accountId)
          .setInstant("order_at", transaction.orderAt)
          .setUuid("transaction_id", transaction.transactionId)
          .setBigDecimal("amount", transaction.amount.bigDecimal)
          .setString("payment_method", transaction.paymentMethod.entryName)
      )

  private[this] def fetchTransactionsByAccountId(accountId: UUID): fs2.Stream[IO, Transaction] =
    fs2.Stream
      .eval(session.execute(selectStatement.bind().setUuid("account_id", accountId)))
      .flatMap(_.asStream)
      .evalMap(Transaction.fromRow[IO])

  def selectTransactionByPrimaryKey(
    accountId: UUID,
    orderAt: Instant,
    transactionId: UUID): IO[Transaction] =
    for {
      resultSet <- session
        .execute(
          selectStatementByPrimaryKey
            .bind()
            .setUuid("account_id", accountId)
            .setInstant("order_at", orderAt)
            .setUuid("transaction_id", transactionId))
      row <- resultSet.oneOrError(ResultNotFound("Transaction not found"))
      transaction <- Transaction.fromRow(row)
    } yield transaction

  def selectTransactionsByAccountId(
    accountId: UUID,
    limit: Long = 50,
    offset: Long = 0): fs2.Stream[IO, Transaction] =
    fetchTransactionsByAccountId(accountId)
      .drop(offset * limit)
      .take(limit)

  def selectTransactionsByAccountIdAndPaymentMethod(
    accountId: UUID,
    paymentMethod: PaymentMethod,
    limit: Long = 50,
    offset: Long = 0): fs2.Stream[IO, Transaction] =
    fetchTransactionsByAccountId(accountId)
      .filter(_.paymentMethod.equals(paymentMethod))
      .drop(offset * limit)
      .take(limit)

  def countTransactionsByAccountIdAndPaymentMethod(
    accountId: UUID,
    paymentMethod: PaymentMethod): fs2.Stream[IO, Int] =
    fetchTransactionsByAccountId(accountId)
      .filter(_.paymentMethod.equals(paymentMethod))
      .as(1)
      .fold(0)(_ + _)
}

object TransactionQuery extends CatsEffectConverters {

  def buildAsStream(session: Session[IO])(
    implicit logger: Logger[IO]): fs2.Stream[IO, TransactionQuery] =
    fs2.Stream.eval(build(session))

  def build(session: Session[IO])(implicit logger: Logger[IO]): IO[TransactionQuery] =
    for {
      countStatement <- countByAccountIdPreparedStatement(session)
      insertStatement <- insertPreparedStatement(session)
      selectStatement <- selectByAccountIdPreparedStatement(session)
      selectByPrimaryKeyStatement <- selectByPrimaryKeyPreparedStatement(session)
    } yield
      TransactionQuery(
        session,
        countStatement,
        insertStatement,
        selectStatement,
        selectByPrimaryKeyStatement)

  def countByAccountIdPreparedStatement(session: Session[IO]): IO[PreparedStatement] =
    session
      .prepare(
        QueryBuilder
          .selectFrom(CqlIdentifier.fromCql("transaction"))
          .countAll()
          .as("count")
          .whereColumn("account_id")
          .isEqualTo(bindMarker)
          .build()
      )

  def insertPreparedStatement(session: Session[IO]): IO[PreparedStatement] =
    session
      .prepare(
        QueryBuilder
          .insertInto(CqlIdentifier.fromCql("transaction"))
          .value(CqlIdentifier.fromCql("account_id"), bindMarker)
          .value(CqlIdentifier.fromCql("order_at"), bindMarker)
          .value(CqlIdentifier.fromCql("transaction_id"), bindMarker)
          .value(CqlIdentifier.fromCql("amount"), bindMarker)
          .value(CqlIdentifier.fromCql("payment_method"), bindMarker)
          .build)

  def selectByAccountIdPreparedStatement(session: Session[IO]): IO[PreparedStatement] =
    session
      .prepare(
        QueryBuilder
          .selectFrom(CqlIdentifier.fromCql("transaction"))
          .all()
          .whereColumn("account_id")
          .isEqualTo(bindMarker)
          .build()
      )

  def selectByPrimaryKeyPreparedStatement(session: Session[IO]): IO[PreparedStatement] =
    session
      .prepare(
        QueryBuilder
          .selectFrom(CqlIdentifier.fromCql("transaction"))
          .all()
          .whereColumn("account_id")
          .isEqualTo(bindMarker)
          .whereColumn("order_at")
          .isEqualTo(bindMarker)
          .whereColumn("transaction_id")
          .isEqualTo(bindMarker)
          .build()
      )
}
