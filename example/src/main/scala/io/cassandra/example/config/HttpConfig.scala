package io.cassandra.example.config

case class Host(value: String) extends AnyVal
case class Port(value: Int) extends AnyVal

case class HttpConfig(host: Host, port: Port)
