package com.globalmaksimum.kafkainfluxexporter

import java.util
import java.util.concurrent.TimeUnit

import cats.effect.{Async, ContextShift, Resource, Timer}
import fs2.kafka.{AdminClientFactory, AdminClientSettings, ConsumerFactory, ConsumerSettings}
import org.apache.kafka.clients.admin.AdminClient
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.instances.list._
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

class ConsumerGroupCollector[F[_]](bootstrapServers: String, repeatInterval: FiniteDuration, metricPrefix: String, kerberosEnabled: Boolean)(
  implicit F: Async[F], timer: Timer[F],cs: ContextShift[F]) {

  import ConsumerGroupCollector._

  val securityConfigs = Array((CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name),
    ("sasl.kerberos.service.name", "kafka"))

  private def createAdminClient = AdminClientFactory.Default.create[F] {
    val adminClient = AdminClientSettings.Default.withBootstrapServers(bootstrapServers)
    if (kerberosEnabled) adminClient.withProperties(securityConfigs: _*) else adminClient
  }

  private def createConsumer = ConsumerFactory.Default.create {
    val consumer = ConsumerSettings(new ByteArrayDeserializer, new ByteArrayDeserializer).withBootstrapServers(bootstrapServers)
    if (kerberosEnabled) consumer.withProperties(securityConfigs: _*) else consumer
  }


  import scala.collection.JavaConverters._

  def groups(adminClient: AdminClient) = Stream.eval(extractGroupsInternal(adminClient))
    .flatMap(l => Stream.fromIterator(l.iterator)).handleErrorWith { t =>
    log.error(t)("encountered with error when getting consumer groups")
    Stream.empty
  }

  private def extractGroupsInternal(adminClient: AdminClient) = {
    adminClient.listConsumerGroups().all().toF.map(_.asScala.toList.map(_.groupId()))
  }


  import scala.collection.JavaConverters._


  def getEndOffsets(consumer: Consumer[_, _], tp: TopicPartition): F[Long] = F.delay(consumer.endOffsets(util.Arrays.asList(tp)).values().asScala.head)

  def getEarliestOffsets(consumer: Consumer[_, _], tp: TopicPartition): F[Long] = F.delay(consumer.beginningOffsets(util.Arrays.asList(tp)).values().asScala.head)

  def collectGroupOffsets(adminClient: AdminClient, group: String, consumer: Consumer[_, _]) =
    for {
      tpOffsets <- Stream.eval {
        adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().toF.map(_.asScala.toIterator)
      }.flatMap(i => Stream.fromIterator(i)).handleErrorWith { t =>
        log.error(t)(s"encountered with error when getting groups topic and offset map group ${group}")
        Stream.empty
      }
      latestOffset <- Stream.eval(getEndOffsets(consumer, tpOffsets._1)).handleErrorWith { t =>
        log.error(t)(s"encountered with error when getting latest offset of topic:${tpOffsets._1} for group:$group")
        Stream.empty
      }
    } yield LagInfo(group, tpOffsets._1.topic(), tpOffsets._1.partition(), tpOffsets._2.offset(), latestOffset)

  def program = for {
    adminClient <- Stream.bracket(createAdminClient)(i => F.delay(i.close()))
    consumer <- Stream.bracket(createConsumer)(i => F.delay(i.close()))
    _ <- Stream.fixedRate(repeatInterval).evalTap(_ => F.delay(log.info("collecting group info")))
    now <- Stream(System.currentTimeMillis())
    group <- groups(adminClient)
    lag <- collectGroupOffsets(adminClient, group, consumer)
      .map(l => s"$metricPrefix,consumerGroup=${l.groupId},topic=${l.topic},partition=${l.partition} groupOffset=${l.groupOffset},latestOffset=${l.latestOffset} $now")
  } yield lag

  def collectAll =
    Resource.fromAutoCloseable(createConsumer).use { consumer =>
      Resource.fromAutoCloseable(createAdminClient).use { adminClient =>
        groups(adminClient).compile.toList.flatMap { groupsL =>
          log.info(s"collected groups $groupsL")
          val traversed = groupsL.traverse {
            group =>
              log.info("collection for group")
              val groupOffsets = collectGroupOffsets(adminClient, group, consumer).compile.toList
              log.info(s"group offset for ${group} $groupOffsets")
              groupOffsets
          }.map(_.flatMap(i => i))
          log.info("traverse called")
          traversed
        } <* cs.shift // we need context shift here otherwise it can create deadlock
      }
    } <* cs.shift


}
