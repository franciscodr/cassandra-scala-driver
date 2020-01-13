package io.cassandra.example.http.auth

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import io.cassandra.example.http.{AuthorizationHeaderNotProvided, ServiceError, UserNotAuthorized}
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.server._
import org.http4s.util.CaseInsensitiveString

trait BasicAuthentication {
  val authUser: Kleisli[IO, Request[IO], Either[ServiceError, String]] = Kleisli({ request =>
    IO.delay(request.headers.foreach(h => println(s"${h.name.value}:${h.value}")))
      .flatMap(_ =>
        IO(for {
          header <- request.headers
            .get(CaseInsensitiveString("Authorization"))
            .toRight(AuthorizationHeaderNotProvided)
          user <- Either
            .cond(header.value.equalsIgnoreCase("customer"), header.value, UserNotAuthorized)
        } yield user)
      )
  })

  val onFailure: AuthedRoutes[ServiceError, IO] =
    Kleisli(request => OptionT.liftF(Forbidden(request.authInfo)))

  val middleware: AuthMiddleware[IO, String] = AuthMiddleware(authUser, onFailure)
}

object HttpRoutesWithBasicAuthentication extends BasicAuthentication {
  def apply(
    routes: PartialFunction[AuthedRequest[IO, String], IO[Response[IO]]]
  ): HttpRoutes[IO] = {
    middleware(AuthedRoutes.of(routes))
  }
}
