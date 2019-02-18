package com.globalmaksimum.kafkainfluxexporter

import cats.effect.{ExitCode, IO, IOApp}
import org.http4s._
import cats.syntax.functor._
import cats.syntax.apply._

import scala.concurrent.duration._
import cats.syntax.parallel._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.log4s._

import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect._
object Main extends IOApp {
  val log = getLogger

  import InfluxUtils.retryWithBackoff

  override def run(args: List[String]): IO[ExitCode] = {
    val configs = loadConfigF[IO, Configs.ProgramConfig]
    val clientFactory: fs2.Stream[IO, Client[IO]] = BlazeClientBuilder[IO](scala.concurrent.ExecutionContext.Implicits.global).withMaxTotalConnections(20).stream
    configs.flatMap{c=>
      val uriString = c.influx.auth.map(a=>s"http://${c.influx.hostPort}/write?u=${a.username}&p=${a.password}&db=${c.influx.database}&precision=ms")
        .getOrElse(s"http://${c.influx.hostPort}/write?db=${c.influx.database}&precision=ms")
      IO.fromEither(Uri.fromString(uriString)).flatMap{ influxEndPoint =>
        val groupCollector = retryWithBackoff(
          (new ConsumerGroupCollector[IO](c.bootstrapServers, 10 seconds, c.metricPrefix.getOrElse("group_offsets")))
            .program.evalTap(l => IO(log.debug(s"consumerGroupMetric:$l"))).through2(clientFactory)(InfluxUtils.toInflux(influxEndPoint)).compile.drain,
          5 seconds, Integer.MAX_VALUE, "GBM group collector failed with ")
        val metricTopicExporter = c.metricConsumer.map(mcc=>retryWithBackoff(
          (new MetricsTopicExporter[IO](mcc.topic, c.bootstrapServers, mcc.consumerGroup))
            .program.evalTap(l => IO(log.debug(s"consumerMetric:$l"))).through2(clientFactory)(InfluxUtils.toInflux(influxEndPoint)).compile.drain,
          5 seconds, Integer.MAX_VALUE, "GBM metric reporter failed with ")).getOrElse(IO.unit)
        log.debug("starting execution")
        (groupCollector, metricTopicExporter).parMapN((_, _) => ())
      }
    }.as(ExitCode.Success)
  }

}
