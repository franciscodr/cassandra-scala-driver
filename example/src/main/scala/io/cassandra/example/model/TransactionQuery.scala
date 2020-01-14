package io.cassandra.example.model

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.IO._
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker
import io.cassandra.error.ResultNotFound
import io.cassandra.{AsyncResultSet, CatsEffectConverters, Session}
import io.chrisdavenport.log4cats.Logger

case class TransactionQuery(
  session: Session[IO],
  countStatementByAccountId: PreparedStatement,
  insertStatement: PreparedStatement,
  selectPaymentMethodStatement: PreparedStatement,
  selectStatementByAccountId: PreparedStatement,
  selectByAccountIdAndModifiedSince: PreparedStatement,
  selectStatementByPrimaryKey: PreparedStatement
)(
  implicit logger: Logger[IO]
) extends CatsEffectConverters {

  def countByAccountId(accountId: UUID): IO[Long] =
    session
      .execute(
        countStatementByAccountId
          .bind()
          .setUuid("account_id", accountId)
      )
      .map(_.oneOrNone.map(_.getLong("count")).getOrElse(0L))

  def countTransactionsByAccountIdAndPaymentMethod(
    accountId: UUID,
    paymentMethod: PaymentMethod
  ): fs2.Stream[IO, Int] =
    fs2.Stream
      .eval(session.execute(selectPaymentMethodStatement.bind().setUuid("account_id", accountId)))
      .flatMap(_.asStream)
      .map(row => PaymentMethod.withNameOption(row.getString("payment_method")))
      .filter(_.contains(paymentMethod))
      .as(1)
      .fold(0)(_ + _)

  def insertTransaction(transaction: TransactionDTO): IO[AsyncResultSet] =
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

  private[this] def fetchTransactionsByAccountId(accountId: UUID): fs2.Stream[IO, Row] =
    fs2.Stream
      .eval(session.execute(selectStatementByAccountId.bind().setUuid("account_id", accountId)))
      .flatMap(_.asStream)

  private[this] def fetchTransactionsByAccountIdAndModifiedSince(
    accountId: UUID,
    modifiedSince: Instant
  ): fs2.Stream[IO, Row] =
    fs2.Stream
      .eval(
        session.execute(
          selectByAccountIdAndModifiedSince
            .bind()
            .setUuid("account_id", accountId)
            .setInstant("order_at", modifiedSince)
        )
      )
      .flatMap(_.asStream)

  def selectTransactionByPrimaryKey(
    accountId: UUID,
    orderAt: Instant,
    transactionId: UUID
  ): IO[TransactionDTO] =
    for {
      resultSet <- session
        .execute(
          selectStatementByPrimaryKey
            .bind()
            .setUuid("account_id", accountId)
            .setInstant("order_at", orderAt)
            .setUuid("transaction_id", transactionId)
        )
      row <- resultSet.oneOrError(ResultNotFound("Transaction not found"))
      transaction <- TransactionDTO.fromRow(row)
    } yield transaction

  def selectTransactionsByAccountId(
    accountId: UUID,
    limit: Long = 50,
    offset: Long = 0
  ): fs2.Stream[IO, TransactionDTO] =
    fetchTransactionsByAccountId(accountId)
      .drop(offset)
      .take(limit)
      .evalMap(TransactionDTO.fromRow[IO]) // At most "limit" elements

  def selectTransactionsByAccountIdAndModifiedSince(
    accountId: UUID,
    modifiedSince: Instant,
    limit: Long = 50,
    offset: Long = 0
  ): fs2.Stream[IO, TransactionDTO] =
    fetchTransactionsByAccountIdAndModifiedSince(accountId, modifiedSince)
      .drop(offset)
      .take(limit)
      .evalMap(TransactionDTO.fromRow[IO]) // At most "limit" elements

  private[this] def selectTransactionsByAccountIdWithFilter(
    accountId: UUID,
    predicate: TransactionDTO => Boolean,
    limit: Long,
    offset: Long
  ): fs2.Stream[IO, TransactionDTO] =
    fetchTransactionsByAccountId(accountId)
      .evalMap(TransactionDTO.fromRow[IO])
      .filter(predicate)
      .drop(offset)
      .take(limit)

  private[this] def selectTransactionsByAccountIdAndModifiedSinceWithFilter(
    accountId: UUID,
    modifiedSince: Instant,
    predicate: TransactionDTO => Boolean,
    limit: Long,
    offset: Long
  ): fs2.Stream[IO, TransactionDTO] =
    fetchTransactionsByAccountIdAndModifiedSince(accountId, modifiedSince)
      .evalMap(TransactionDTO.fromRow[IO])
      .filter(predicate)
      .drop(offset)
      .take(limit)

  def selectTransactionsByAccountIdAndPaymentMethod(
    accountId: UUID,
    paymentMethods: List[PaymentMethod],
    limit: Long = 50,
    offset: Long = 0
  ): fs2.Stream[IO, TransactionDTO] =
    if (paymentMethods.isEmpty)
      selectTransactionsByAccountId(accountId, limit, offset)
    else
      selectTransactionsByAccountIdWithFilter(
        accountId = accountId,
        predicate = transaction => paymentMethods.contains(transaction.paymentMethod),
        limit = limit,
        offset = offset
      )

  def selectTransactionsByAccountIdAndPaymentMethodAndModifiedSince(
    accountId: UUID,
    modifiedSince: Instant,
    paymentMethods: List[PaymentMethod],
    limit: Long = 50,
    offset: Long = 0
  ): fs2.Stream[IO, TransactionDTO] =
    if (paymentMethods.isEmpty)
      selectTransactionsByAccountIdAndModifiedSince(accountId, modifiedSince, limit, offset)
    else
      selectTransactionsByAccountIdAndModifiedSinceWithFilter(
        accountId = accountId,
        modifiedSince = modifiedSince,
        predicate = transaction => paymentMethods.contains(transaction.paymentMethod),
        limit = limit,
        offset = offset
      )
}

object TransactionQuery extends CatsEffectConverters {

  def buildAsStream(
    session: Session[IO]
  )(implicit logger: Logger[IO]): fs2.Stream[IO, TransactionQuery] =
    fs2.Stream.eval(build(session))

  def build(session: Session[IO])(implicit logger: Logger[IO]): IO[TransactionQuery] =
    for {
      countStatementByAccountId <- countByAccountIdPreparedStatement(session)
      insertStatement <- insertPreparedStatement(session)
      selectPaymentMethodStatement <- selectPaymentMethodByAccountIdPreparedStatement(session)
      selectStatement <- selectByAccountIdPreparedStatement(session)
      selectByAccountIdAndModifiedSince <- selectByAccountIdAndModifiedSincePreparedStatement(
        session
      )
      selectByPrimaryKeyStatement <- selectByPrimaryKeyPreparedStatement(session)
    } yield TransactionQuery(
      session,
      countStatementByAccountId,
      insertStatement,
      selectPaymentMethodStatement,
      selectStatement,
      selectByAccountIdAndModifiedSince,
      selectByPrimaryKeyStatement
    )

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
          .build
      )

  def selectPaymentMethodByAccountIdPreparedStatement(session: Session[IO]): IO[PreparedStatement] =
    session
      .prepare(
        QueryBuilder
          .selectFrom(CqlIdentifier.fromCql("transaction"))
          .column("payment_method")
          .whereColumn("account_id")
          .isEqualTo(bindMarker)
          .build()
      )

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

  def selectByAccountIdAndModifiedSincePreparedStatement(
    session: Session[IO]
  ): IO[PreparedStatement] =
    session
      .prepare(
        QueryBuilder
          .selectFrom(CqlIdentifier.fromCql("transaction"))
          .all()
          .whereColumn("account_id")
          .isEqualTo(bindMarker)
          .whereColumn("order_at")
          .isGreaterThanOrEqualTo(bindMarker)
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
