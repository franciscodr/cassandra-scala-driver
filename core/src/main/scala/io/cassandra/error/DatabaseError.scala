package io.cassandra.error

abstract class DatabaseError(message: String) extends Throwable(message)

case class ConversionError(message: String) extends DatabaseError(message)

case class ResultNotFound(message: String) extends DatabaseError(message)
