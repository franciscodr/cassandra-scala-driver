package io.cassandra.example.model

import enumeratum._
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.immutable.IndexedSeq

sealed trait PaymentMethod extends EnumEntry

object PaymentMethod extends Enum[PaymentMethod] {
  val values: IndexedSeq[PaymentMethod] = findValues

  case object ApplePay extends PaymentMethod
  case object Cash extends PaymentMethod
  case object CreditCard extends PaymentMethod
  case object GooglePay extends PaymentMethod
  case object SvcCard extends PaymentMethod

  implicit val arbitrary: Arbitrary[PaymentMethod] = Arbitrary(
    Gen.oneOf(values)
  )
}
