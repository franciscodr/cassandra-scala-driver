package io.cassandra.error

import cats.data.NonEmptyList
import cats.syntax.show._
import io.circe.Error

abstract class DatabaseError(message: String) extends Throwable(message)

case class ConversionError(message: String) extends DatabaseError(message)

case class JsonParseError(message: String, decodingErrors: NonEmptyList[String])
    extends DatabaseError(message)

object JsonParseError {
  def withColumnName(name: String)(decodingErrors: NonEmptyList[Error]): JsonParseError =
    JsonParseError(s"Error decoding JSON from DB column $name", decodingErrors.map(_.show))
}

case class ResultNotFound(message: String) extends DatabaseError(message)
