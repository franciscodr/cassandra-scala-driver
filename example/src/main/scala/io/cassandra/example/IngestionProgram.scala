package io.cassandra.example

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import io.cassandra.Session
import io.cassandra.config._
import io.cassandra.example.config._
import io.cassandra.example.model.{TransactionDTO, TransactionQuery}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object IngestionProgram extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    for {
      implicit0(logger: Logger[IO]) <- fs2.Stream.eval(Slf4jLogger.create[IO])
      config <- loadConfigAsStream[IO, CassandraConfig]("example.cassandra")
      session <- Session
        .buildAsStream(config)
        .evalTap(_ => logger.info("Database session opened"))
      query <- TransactionQuery
        .buildAsStream(session)
        .evalTap(_ => logger.info("TransactionQuery object created"))
      transaction <- fs2.Stream
        .range(0, 10)
        .flatMap(_ => fs2.Stream.emits(TransactionDTO.generator.sample.get))
        .evalMap(query.insertTransaction)
    } yield transaction.wasApplied
  }.compile.drain.as(ExitCode.Success)
}
