package io.cassandra

import java.util.concurrent.CompletionStage

import cats.effect.Async

trait CatsEffectConverters {

  final class FromCompletionStage[F[_]] {
    def apply[A](completionStage: => CompletionStage[A])(implicit async: Async[F]): F[A] =
      async.async[A] { cb =>
        completionStage.handle[Unit]((value, exception) =>
          Option(exception) match {
            case None => cb(Right(value))
            case Some(e) => cb(Left(e))
          }
        )
      }
  }

  def fromCompletionStage[F[_]] = new FromCompletionStage[F]
}

object CatsEffectConverters extends CatsEffectConverters
