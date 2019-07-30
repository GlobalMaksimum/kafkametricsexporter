package com.globalmaksimum.kafkainfluxexporter


import cats.effect.{ContextShift, ExitCode, IO, IOApp}
import org.http4s._
import cats.syntax.functor._

import scala.concurrent.duration._
import io.prometheus.client.CollectorRegistry
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.metrics.prometheus.PrometheusExportService
import org.http4s.server.blaze.BlazeServerBuilder
import org.log4s._
import org.http4s.implicits._
import org.http4s.server.Router
import cats.syntax.apply._
import cats.syntax.parallel._
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext
import org.apache.kafka.common.config.SslConfigs


object Main {
  val log = getLogger

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.Implicits.global)
  implicit val timer = IO.timer(ExecutionContext.Implicits.global)

  import InfluxUtils.retryWithBackoff

  def loadConfigF: IO[Configs.ProgramConfig] = Configs.programConfig.run(ConfigFactory.load())

  def main(args: Array[String]): Unit = {
    val configs = loadConfigF
    val clientFactory: fs2.Stream[IO, Client[IO]] = BlazeClientBuilder[IO](scala.concurrent.ExecutionContext.Implicits.global).withMaxTotalConnections(20).stream
    val run = configs.flatMap { c =>
      log.info(s"read config $c")
      val influxExporter = c.influx.map { influx =>
        val uriString = influx.auth.map(a => s"http://${influx.hostPort}/write?u=${a.username}&p=${a.password}&db=${influx.database}&precision=ms")
          .getOrElse(s"http://${influx.hostPort}/write?db=${influx.database}&precision=ms")
        IO.fromEither(Uri.fromString(uriString)).flatMap { influxEndPoint =>
          val groupCollector = retryWithBackoff(
            (new ConsumerGroupCollector[IO](c.bootstrapServers, 10 seconds, c.metricPrefix.getOrElse("group_offsets"), c.kerberosEnabled.getOrElse(false)))
              .program.evalTap(l => IO(log.debug(s"consumerGroupMetric:$l"))).through2(clientFactory)(InfluxUtils.toInflux(influxEndPoint)).compile.drain,
            5 seconds, Integer.MAX_VALUE, "GBM group collector failed with ")
          val metricTopicExporter = c.metricConsumer.map(mcc => retryWithBackoff(
            (new MetricsTopicExporter[IO](mcc.topic, c.bootstrapServers, mcc.consumerGroup))
              .program.evalTap(l => IO(log.debug(s"consumerMetric:$l"))).through2(clientFactory)(InfluxUtils.toInflux(influxEndPoint)).compile.drain,
            5 seconds, Integer.MAX_VALUE, "GBM metric reporter failed with ")).getOrElse(IO.unit)
          log.debug("starting execution")
          (groupCollector, metricTopicExporter).parMapN((_, _) => ())
        }
      }.getOrElse(IO.unit)
      val prometheusExporter = c.prometheus.map { prometheus =>
        val registry = new CollectorRegistry()
        registry.register(new KafkaCollector(
          new ConsumerGroupCollector[IO](c.bootstrapServers, 10 seconds, c.metricPrefix.getOrElse("group_offsets"), c.kerberosEnabled.getOrElse(false)
          ), prometheus.prefix))
        BlazeServerBuilder[IO]
          .bindHttp(prometheus.port, "0.0.0.0")
          .withHttpApp(Router("/" -> PrometheusExportService[IO](registry).routes).orNotFound)
          .serve
          .compile
          .drain
      }.getOrElse(IO.unit)
      (influxExporter, prometheusExporter).parMapN((_, _) => ())
    }.as(ExitCode.Success)

    val exitCode = run.unsafeRunSync()
    log.warn(s"exit with $exitCode")
  }

}
