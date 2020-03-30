package io.cassandra.syntax

import java.time.Instant

import com.datastax.oss.driver.api.core.cql.Row

object row extends RowSyntax

trait RowSyntax {
  implicit class RowOps(row: Row) {

    private[this] def getOptionalValue[A](identifier: String, f: String => A): Option[A] =
      if (row.isNull(identifier)) None else Some(f(identifier))

    def getOptionalBoolean(identifier: String): Option[Boolean] =
      getOptionalValue(identifier, row.getBoolean)

    def getOptionalInstant(identifier: String): Option[Instant] =
      getOptionalValue(identifier, row.getInstant)

    def getOptionalString(identifier: String): Option[String] =
      getOptionalValue(identifier, row.getString)
  }
}
