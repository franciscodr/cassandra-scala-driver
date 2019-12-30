package io.cassandra.syntax

import java.time.Instant

import com.datastax.oss.driver.api.core.cql.Row

object row extends RowSyntax

trait RowSyntax {
  implicit class RowOps(row: Row) {

    private[this] def getOptionalValue[A](
        identifier: String,
        f: String => A): Option[A] =
      if (row.isNull(identifier)) None else Some(f(identifier))

    def getOptionalInstant(identifier: String): Option[Instant] =
      getOptionalValue(identifier, row.getInstant)
  }
}
