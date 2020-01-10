package io.cassandra.example.http

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import io.cassandra.Session
import io.cassandra.example.model.TransactionQuery
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze._

object HttpService extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val ingestionStream = for {
      implicit0(logger: Logger[IO]) <- fs2.Stream.eval(Slf4jLogger.create[IO])
      session <- Session.buildAsStream("customer", 500)
      query <- TransactionQuery.buildAsStream(session)
      stream <- BlazeServerBuilder[IO]
        .bindHttp(8080, "0.0.0.0")
        .withHttpApp(Router("/" -> new ServiceApi(query).httpService).orNotFound)
        .serve
    } yield stream

    ingestionStream.compile.drain
      .as(ExitCode.Success)
  }
}
