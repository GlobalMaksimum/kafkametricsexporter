package com.globalmaksimum.kafkainfluxexporter

import cats.data.NonEmptyList
import org.scalatest.{FlatSpec, Matchers}
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect._
import cats.effect.IO
import com.globalmaksimum.kafkainfluxexporter.Configs.Authentication
object Parameters {
  val bootstrapServers =
    "quickstart.confluent.io:19094,quickstart.confluent.io:29094,quickstart.confluent.io:39094"
  val consumerGroup = "gbmmetricsconsumer"
  val influxDB = "kafka"
  val influxHost = "localhost:8086"

  val topics = NonEmptyList.one("__GBMMetrics")
}
class ConfigsTest extends FlatSpec with Matchers {

  it should "parse example config" in {
    val result = loadConfigF[IO, Configs.ProgramConfig].unsafeRunSync()
    result.bootstrapServers should be (Parameters.bootstrapServers)
    result.metricConsumer.topic should be (Parameters.topics.head)
    result.metricConsumer.consumerGroup should be (Parameters.consumerGroup)
    result.influx.database should be (Parameters.influxDB)
    result.influx.hostPort should be (Parameters.influxHost)

    result.influx.auth shouldBe Some(Authentication("myuser","mypassword"))
  }

}
