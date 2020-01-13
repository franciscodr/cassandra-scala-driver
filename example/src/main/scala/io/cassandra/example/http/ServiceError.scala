package io.cassandra.example.http

import cats.effect.IO
import org.http4s.EntityEncoder

abstract class ServiceError(val message: String, val cause: Option[Throwable])
    extends Throwable(message) {
  cause foreach initCause
}

object ServiceError {

  implicit val entityEncoder: EntityEncoder[IO, ServiceError] =
    EntityEncoder.stringEncoder[IO].contramap(_.message)
}

case object UserNotAuthorized extends ServiceError("User is not allowed to retrieve the data", None)
case object AuthorizationHeaderNotProvided
    extends ServiceError("Unable to find an Authorization header", None)
