package io.cassandra.example.http.model

case class Transaction(
  transactionId: String,
  accountId: String,
  orderAt: String,
  amount: Float,
  paymentMethod: String
)
