package com.globalmaksimum.kafkainfluxexporter

import java.util

import cats.effect.{Async, Timer}
import fs2.kafka.{AdminClientFactory, AdminClientSettings, ConsumerFactory, ConsumerSettings}
import org.apache.kafka.clients.admin.AdminClient
import cats.syntax.functor._
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import fs2.Stream
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.duration._
import org.log4s._
object ConsumerGroupCollector {

  implicit class kafkaFuture2F[T](val f: KafkaFuture[T]) extends AnyVal {
    def toF[F[_]](implicit F: Async[F]): F[T] = F.async[T] { callback =>
      f.whenComplete((t: T, u: Throwable) =>
        if (u == null) callback(Right(t)) else callback(Left(u)))
    }
  }

  case class LagInfo(groupId: String, topic: String, partition: Int, groupOffset: Long, latestOffset: Long)

  val log = getLogger
}

class ConsumerGroupCollector[F[_]](bootstrapServers: String, repeatInterval: FiniteDuration,metricPrefix:String)(
  implicit F: Async[F], timer: Timer[F]) {

  import ConsumerGroupCollector._

  private def createAdminClient = AdminClientFactory.Default.create[F](
    AdminClientSettings.Default.withBootstrapServers(bootstrapServers).withProperties(
      (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name),
      ("sasl.kerberos.service.name", "kafka")
    ))

  private def createConsumer = ConsumerFactory.Default.create(ConsumerSettings(new ByteArrayDeserializer, new ByteArrayDeserializer).withBootstrapServers(bootstrapServers).withProperties(
    (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name),
    ("sasl.kerberos.service.name", "kafka")
  ))


  import scala.collection.JavaConverters._

  def groups(adminClient: AdminClient) = Stream.eval(
    adminClient.listConsumerGroups().all().toF.map(_.asScala.toIterator.map(_.groupId()))
  ).flatMap(l => Stream.fromIterator(l)).handleErrorWith { t =>
    log.error(t)("encountered with error when getting consumer groups")
    Stream.empty
  }

  import scala.collection.JavaConverters._


  def getOffset(consumer:Consumer[_,_],tp: TopicPartition): F[Long] = F.delay(consumer.endOffsets(util.Arrays.asList(tp)).values().asScala.head)

  def collectGroupOffsets(adminClient:AdminClient,group: String,consumer:Consumer[_,_]) =
    for {
      tpOffsets <- Stream.eval {
        adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().toF.map(_.asScala.toIterator)
      }.flatMap(i => Stream.fromIterator(i)).handleErrorWith { t =>
        log.error(t)(s"encountered with error when getting groups topic and offset map group ${group}")
        Stream.empty
      }
      latestOffset <- Stream.eval(getOffset(consumer,tpOffsets._1)).handleErrorWith { t =>
        log.error(t)(s"encountered with error when getting latest offset of topic:${tpOffsets._1} for group:$group")
        Stream.empty
      }
    } yield LagInfo(group, tpOffsets._1.topic(), tpOffsets._1.partition(), tpOffsets._2.offset(), latestOffset)

  def program = for {
    adminClient <- Stream.bracket(createAdminClient)(i=>F.delay(i.close()))
    consumer <- Stream.bracket(createConsumer)(i=>F.delay(i.close()))
    _ <- Stream.fixedRate(repeatInterval).evalTap(_ => F.delay(log.info("collecting group info")))
    now <- Stream(System.currentTimeMillis())
    group <- groups(adminClient)
    lag <- collectGroupOffsets(adminClient,group,consumer)
      .map(l => s"$metricPrefix,consumerGroup=${l.groupId},topic=${l.topic},partition=${l.partition} groupOffset=${l.groupOffset},latestOffset=${l.latestOffset} $now")
  } yield lag


}
