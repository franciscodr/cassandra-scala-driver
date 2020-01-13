package io.cassandra.example.http

import java.time.Instant

import cats.effect.IO
import cats.syntax.either._
import cats.syntax.option._
import org.http4s.{MediaType, Request}
import org.http4s.util.CaseInsensitiveString

trait HeaderExtractors {
  def extractIfModifiedSinceHeader(request: Request[IO]): Option[Instant] =
    request.headers
      .get(CaseInsensitiveString("If-Modified-Since"))
      .flatMap(header =>
        Either
          .catchNonFatal(Instant.parse(header.value))
          .fold(
            _ => None,
            instant => instant.some
          )
      )

  def extractAcceptHeader(request: Request[IO]): ContentType =
    request.headers
      .get(org.http4s.headers.Accept)
      .map(header =>
        if (header.value == MediaType.application.xml.mainType + "/" + MediaType.application.xml.subType)
          Xml
        else Json
      )
      .getOrElse(Json)
}
