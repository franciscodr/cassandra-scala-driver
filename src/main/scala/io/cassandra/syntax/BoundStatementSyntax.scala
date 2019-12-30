package io.cassandra.syntax

import java.time.Instant

import com.datastax.oss.driver.api.core.cql.BoundStatement

trait BoundStatementSyntax {
  implicit class BoundStatementOps(boundStatement: BoundStatement) {

    private[this] def setOptionalValue[A](identifier: String, value: Option[A])(
        f: (String, A) => BoundStatement): BoundStatement =
      value.fold(boundStatement.unset(identifier))(v => f(identifier, v))

    def setOptionalInstant(
        identifier: String,
        value: Option[Instant]): BoundStatement =
      setOptionalValue(identifier, value)(boundStatement.setInstant)

    def setOptionalString(
        identifier: String,
        value: Option[String]): BoundStatement =
      setOptionalValue(identifier, value)(boundStatement.setString)
  }
}

object boundStatement extends BoundStatementSyntax
