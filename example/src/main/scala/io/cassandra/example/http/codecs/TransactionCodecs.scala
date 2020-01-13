package io.cassandra.example.http.codecs

import cats.effect.IO
import io.cassandra.example.model.{Transaction, Transactions}
import io.circe.generic.semiauto._
import io.circe.{Encoder, Json}
import org.http4s.EntityEncoder
import org.http4s.circe.jsonEncoderOf

trait TransactionCodecs {
  implicit val transactionEncoder: Encoder[Transaction] = deriveEncoder[Transaction]
  implicit val transactionsEncoder: Encoder[Transactions] =
    Encoder.instance[Transactions](value =>
      Json.fromValues(value.transaction map transactionEncoder.apply)
    )

  implicit val transactionsEntityEncoder: EntityEncoder[IO, Transactions] =
    jsonEncoderOf[IO, Transactions]
}
