package com.globalmaksimum.kafkainfluxexporter

import cats.effect._
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, consumerExecutionContextStream, consumerStream}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class MetricsTopicExporter[F[_]](topic: String, bootstrapServers: String, consumerGroup: String)
                                (implicit F: ConcurrentEffect[F], timer: Timer[F], contextShift: ContextShift[F]) {
  def program = {
    val consumerSettings = (executionContext: ExecutionContext) => {
      ConsumerSettings(
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer,
        executionContext = executionContext
      ).withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(bootstrapServers)
        .withEnableAutoCommit(true)
        .withAutoCommitInterval(10 seconds)
        .withGroupId(consumerGroup)
        .withProperties(
          (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name),
          ("sasl.kerberos.service.name", "kafka")
        )
    }
    for {
      executionContext <- consumerExecutionContextStream[F]
      message <- consumerStream[F].using(consumerSettings(executionContext))
        .evalTap(_.subscribeTo(topic))
        .flatMap(_.stream)
        .map(message => message.record.value())
    } yield message
  }

}
