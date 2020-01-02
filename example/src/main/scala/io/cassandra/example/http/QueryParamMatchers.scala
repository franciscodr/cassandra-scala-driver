package io.cassandra.example.http

import cats.data.{Validated, ValidatedNel}
import cats.instances.list._
import cats.syntax.traverse._
import io.cassandra.example.model.PaymentMethod
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue}
import org.http4s.dsl.io.OptionalQueryParamDecoderMatcher

trait QueryParamMatchers {
  object LimitQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Long]("limit")

  object OffsetQueryParamMatcher extends OptionalQueryParamDecoderMatcher[Long]("offset")

  implicit val paymentMethodQueryParamDecoder: QueryParamDecoder[PaymentMethod] =
    new QueryParamDecoder[PaymentMethod] {
      override def decode(value: QueryParameterValue): ValidatedNel[ParseFailure, PaymentMethod] =
        Validated
          .fromOption(
            PaymentMethod.withNameInsensitiveOption(value.value),
            ParseFailure(
              "Failed to parse PaymentMethod query parameter",
              s"Could not parse ${value.value} as a payment method")
          )
          .toValidatedNel
    }

  implicit val paymentMethodsQueryParamDecoder: QueryParamDecoder[List[PaymentMethod]] =
    new QueryParamDecoder[List[PaymentMethod]] {
      override def decode(
        value: QueryParameterValue): ValidatedNel[ParseFailure, List[PaymentMethod]] =
        value.value.trim
          .split(",")
          .toList
          .traverse(v => paymentMethodQueryParamDecoder.decode(QueryParameterValue(v)))
    }

  object PaymentMethodsQueryParamMatcher
      extends OptionalQueryParamDecoderMatcher[List[PaymentMethod]]("payment_methods")
}
