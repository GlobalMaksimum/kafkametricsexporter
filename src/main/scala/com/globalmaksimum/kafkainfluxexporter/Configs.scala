package com.globalmaksimum.kafkainfluxexporter

object Configs {

  import pureconfig.generic.auto._

  case class Authentication(username: String, password: String)

  case class InfluxDBConfig(hostPort: String, database: String, auth: Option[Authentication])

  case class PrometheusConfig(prefix: String, port: Int)

  case class MetricConsumerConfig(consumerGroup: String, topic: String)

  case class ProgramConfig(bootstrapServers: String, influx: Option[InfluxDBConfig], prometheus: Option[PrometheusConfig], metricPrefix: Option[String],
                           metricConsumer: Option[MetricConsumerConfig], kerberosEnabled: Option[Boolean])

}
