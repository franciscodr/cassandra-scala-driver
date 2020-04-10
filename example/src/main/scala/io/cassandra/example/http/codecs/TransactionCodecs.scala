package io.cassandra.example.http.codecs

import cats.data.NonEmptyList
import cats.effect.IO
import io.cassandra.example.http.model.Transaction
import io.circe.generic.semiauto._
import io.circe.{Encoder, Json}
import org.http4s.EntityEncoder
import org.http4s.circe.jsonEncoderOf

trait TransactionCodecs {
  implicit val transactionEncoder: Encoder[Transaction] = deriveEncoder[Transaction]
  implicit val transactionsEncoder: Encoder[NonEmptyList[Transaction]] =
    Encoder.instance[NonEmptyList[Transaction]](value =>
      Json.fromValues(value.toList map transactionEncoder.apply)
    )

  implicit val transactionsEntityEncoder: EntityEncoder[IO, NonEmptyList[Transaction]] =
    jsonEncoderOf[IO, NonEmptyList[Transaction]]
}
