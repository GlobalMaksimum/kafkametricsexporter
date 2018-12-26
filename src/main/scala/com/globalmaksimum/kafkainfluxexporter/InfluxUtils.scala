package com.globalmaksimum.kafkainfluxexporter

import cats.effect._
import fs2.{Pipe2, Sink}
import cats.syntax.functor._

import scala.concurrent.duration.FiniteDuration
import org.http4s._
import org.http4s.client.Client
import org.slf4j.LoggerFactory
import org.log4s._
object InfluxUtils {
  val log = getLogger

  def createPostRequest[F[_]](body: String, influxEndPoint: Uri): Request[F] = Request[F](method = Method.POST, uri = influxEndPoint).withEntity(body)

  case class ServerResponse(status: Status, responseBody: String)

  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int, messagePrefix: String)
                         (implicit timer: Timer[IO]): IO[A] = {
    import cats.syntax.all._
    ioa.handleErrorWith { error =>
      if (maxRetries > 0)
        IO.sleep(initialDelay).map(_ => log.error(error)(s"$messagePrefix ${error.getMessage}")) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1, messagePrefix)
      else
        IO.raiseError(error)
    }
  }


  def toInflux[F[_]](uri: Uri)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): Pipe2[F, String, Client[F], Unit] = (l, r) => {
    r.flatMap { client =>
      l.map(body => createPostRequest[F](body, uri)).mapAsync(10) { post =>
        client.fetch(post) { r =>
          r.as[String].map(b => ServerResponse(r.status, b))
        }
      }.through(_.filter(!_.status.isSuccess).map(println))
    }
  }
}
