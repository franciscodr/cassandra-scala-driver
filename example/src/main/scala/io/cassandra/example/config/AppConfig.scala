package io.cassandra.example.config

import io.cassandra.config.CassandraConfig

case class AppConfig(cassandra: CassandraConfig, http: HttpConfig)
