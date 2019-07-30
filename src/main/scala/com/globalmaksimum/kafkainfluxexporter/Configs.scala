package com.globalmaksimum.kafkainfluxexporter

import cats.{Functor, Monad}
import cats.data.{Kleisli, Reader}
import cats.effect.IO
import cats.instances.map
import com.typesafe.config.Config
import cats.syntax.contravariant._
import cats.syntax.functor._
import cats.syntax.compose._
import cats.syntax.traverse._
import cats.syntax.flatMap._
import cats.instances.option._

object Configs {
  type IOReader[A, B] = Kleisli[IO, A, B]

  case class Authentication(username: String, password: String)

  case class InfluxDBConfig(hostPort: String, database: String, auth: Option[Authentication])

  case class PrometheusConfig(prefix: String, port: Int)

  case class MetricConsumerConfig(consumerGroup: String, topic: String)

  case class ProgramConfig(bootstrapServers: String, influx: Option[InfluxDBConfig], prometheus: Option[PrometheusConfig], metricPrefix: Option[String],
                           metricConsumer: Option[MetricConsumerConfig], kerberosEnabled: Option[Boolean])

  def klIO[A, B](f: A => B) = Kleisli[IO, A, B](a => IO(f(a)))

  def readString(path: String): IOReader[Config, String] = klIO(c => c.getString(path))

  def readInt(path: String): IOReader[Config, Int] = klIO(c => c.getInt(path))

  def readBooleanOption(path: String): IOReader[Config, Option[Boolean]] = klIO(c => if (c.hasPath(path)) Some(c.getBoolean(path)) else None)

  def innerConfig(path: String): IOReader[Config, Config] = klIO(c => c.getConfig(path))


  def optionInnerConfig(path: String): IOReader[Config, Option[Config]] = klIO(c =>
    if (c.hasPath(path))
      Some(c.getConfig(path))
    else None
  )


  def withInner[A, B](i: IOReader[A, Option[A]])(ia: IOReader[A, B]) = i.map(_.map(ia.run)).map(_.sequence[IO, B]).flatMap(Kleisli.liftF)

  def withInnerX[A, B](i: IOReader[A, Option[A]])(ia: IOReader[A, B]) = i >>> klIO(Functor[Option].lift(ia.run)) map (_.sequence) >>= Kleisli.liftF


  val authentication: IOReader[Config, Authentication] = for {
    name <- readString("username")
    password <- readString("password")
  } yield Authentication(name, password)


  val influx: IOReader[Config, InfluxDBConfig] = for {
    hp <- readString("host-port")
    database <- readString("database")
    auth <- withInner(optionInnerConfig("auth"))(authentication)
  } yield InfluxDBConfig(hp, database, auth)


  val metricConsumer = for {
    cg <- readString("consumer-group")
    t <- readString("topic")
  } yield MetricConsumerConfig(cg, t)

  val prometheus = for {
    port <- readInt("port")
    prefix <- readString("prefix")
  } yield PrometheusConfig(prefix, port)

  val programConfig: IOReader[Config, ProgramConfig] = for {
    bootstrapServers <- readString("bootstrap-servers")
    influx <- withInner(optionInnerConfig("influx"))(influx)
    prometheus <- withInner(optionInnerConfig("prometheus"))(prometheus)
    mc <- withInner(optionInnerConfig("metric-consumer"))(metricConsumer)
    kerberosEnabled <- readBooleanOption("kerberos-enabled")
  } yield ProgramConfig(bootstrapServers, influx, prometheus, None, mc, kerberosEnabled)

}
