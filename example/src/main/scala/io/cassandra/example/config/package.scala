package io.cassandra.example

import java.net.InetSocketAddress

import cats.ApplicativeError
import cats.data.NonEmptyList
import cats.instances.either._
import cats.syntax.either._
import io.cassandra.config._
import pureconfig.{ConfigReader, Derivation}
import pureconfig.error.{CannotConvert, FailureReason}
import pureconfig.generic.semiauto._
import pureconfig.module.cats.nonEmptyListReader

package object config {

  def loadConfig[F[_], Config](
    namespace: String
  )(implicit AE: ApplicativeError[F, Throwable], CR: Derivation[ConfigReader[Config]]): F[Config] =
    AE.fromEither(
      pureconfig.ConfigSource.default
        .at(namespace)
        .load[Config]
        .leftMap(failures => new RuntimeException(failures.prettyPrint()))
    )

  def loadConfigAsStream[F[_], Config](namespace: String)(
    implicit AE: ApplicativeError[F, Throwable],
    CR: Derivation[ConfigReader[Config]]
  ): fs2.Stream[F, Config] =
    fs2.Stream.fromEither[F](
      pureconfig.ConfigSource.default
        .at(namespace)
        .load[Config]
        .leftMap(failures => new RuntimeException(failures.prettyPrint()))
    )

  implicit val cassandraHostsConfigReader: ConfigReader[NonEmptyList[InetSocketAddress]] =
    nonEmptyListReader[String].emap(hosts =>
      hosts.traverse(host =>
        Either
          .catchNonFatal(InetSocketAddress.createUnresolved(host, 9042))
          .leftMap[FailureReason](error =>
            CannotConvert("cassandra.hosts", "InetSocketAddress", error.getMessage)
          )
      )
    )

  implicit val keyspaceConfigReader: ConfigReader[Keyspace] = deriveReader[Keyspace]
  implicit val localDataCenterConfigReader: ConfigReader[LocalDataCenter] =
    deriveReader[LocalDataCenter]
  implicit val passwordConfigReader: ConfigReader[Password] = deriveReader[Password]
  implicit val sslConfigConfigReader: ConfigReader[SslConfig] = deriveReader[SslConfig]
  implicit val usernameConfigReader: ConfigReader[Username] = deriveReader[Username]
  implicit val cassandraConfigReader: ConfigReader[CassandraConfig] = deriveReader[CassandraConfig]
  implicit val hostConfigReader: ConfigReader[Host] = deriveReader[Host]
  implicit val portConfigReader: ConfigReader[Port] = deriveReader[Port]
  implicit val httpConfigReader: ConfigReader[HttpConfig] = deriveReader[HttpConfig]
  implicit val appConfigReader: ConfigReader[AppConfig] = deriveReader[AppConfig]
}
