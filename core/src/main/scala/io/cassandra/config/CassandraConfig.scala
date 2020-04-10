package io.cassandra.config

import java.net.InetSocketAddress

import cats.data.NonEmptyList

case class LocalDataCenter(value: String) extends AnyVal
case class Keyspace(value: String) extends AnyVal
case class Username(value: String) extends AnyVal
case class Password(value: String) extends AnyVal

case class CassandraConfig(
  hosts: NonEmptyList[InetSocketAddress],
  localDataCenter: LocalDataCenter,
  keyspace: Keyspace,
  username: Username,
  password: Password,
  requestPageSize: Int,
  ssl: Option[SslConfig]
) {
  def withRequestPageSize(value: Int): CassandraConfig = this.copy(requestPageSize = value)
}
