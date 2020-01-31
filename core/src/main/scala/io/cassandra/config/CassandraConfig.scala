package io.cassandra.config

case class CassandraConfig(
  hosts: String,
  localDataCenter: String,
  keyspace: String,
  username: String,
  passwordKey: String,
  enableSSL: Boolean,
  truststoreBase64: String,
  truststorePasswordKey: String
)
