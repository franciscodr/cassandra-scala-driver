package io.cassandra.syntax

import java.time.Instant

import cats.data.ValidatedNel
import cats.syntax.option._
import cats.syntax.validated._
import com.datastax.oss.driver.api.core.cql.Row
import io.cassandra.error.JsonParseError
import io.circe._
import io.circe.parser._

object row extends RowSyntax

trait RowSyntax {
  implicit class RowOps(row: Row) {

    private[this] def getOptionalValue[A](identifier: String, f: String => A): Option[A] =
      if (row.isNull(identifier)) None else Some(f(identifier))

    def getJsonList[A: Decoder](name: String): ValidatedNel[JsonParseError, List[A]] =
      getOptionalString(name).fold(List.empty[A].validNel[JsonParseError])(s =>
        decodeAccumulating[List[A]](s)
          .leftMap(JsonParseError.withColumnName(name))
          .toValidatedNel
      )

    def getOptionalBoolean(identifier: String): Option[Boolean] =
      getOptionalValue(identifier, row.getBoolean)

    def getOptionalJson[A: Decoder](name: String): ValidatedNel[JsonParseError, Option[A]] =
      getOptionalString(name).fold(none[A].validNel[JsonParseError])(s =>
        decodeAccumulating[A](s)
          .bimap(JsonParseError.withColumnName(name), _.some)
          .toValidatedNel
      )

    def getOptionalInstant(identifier: String): Option[Instant] =
      getOptionalValue(identifier, row.getInstant)

    def getOptionalString(identifier: String): Option[String] =
      getOptionalValue(identifier, row.getString)
  }
}
