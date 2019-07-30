package com.globalmaksimum.kafkainfluxexporter

import java.util.Optional

import cats.effect.{Async, ContextShift, Resource, Timer}
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.instances.list._
import fs2.Stream
import fs2.kafka.{AdminClientFactory, AdminClientSettings, ConsumerFactory, ConsumerSettings}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{Consumer, OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.requests.ListOffsetRequest
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

  case class LagInfo(groupId: String, topic: String, partition: Int, groupOffset: Long, latestOffset: Long, groupTimestamp: Long, latestTimestamp: Long)

  case class TopicSizeInfo(topic: String, partition: Int, beginningOffset: Long, endOffset: Long, beginningTimestamp: Long, endTimestamp: Long)

  val log = getLogger
}

class ConsumerGroupCollector[F[_]](bootstrapServers: String, repeatInterval: FiniteDuration, metricPrefix: String, kerberosEnabled: Boolean)(
  implicit F: Async[F], timer: Timer[F], cs: ContextShift[F]) {

  import ConsumerGroupCollector._

  val securityConfigs = Array((CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name),
    ("sasl.kerberos.service.name", "kafka"))

  private def createAdminClient: F[AdminClient] = AdminClientFactory.Default.create[F] {
    val adminClient = AdminClientSettings.Default.withBootstrapServers(bootstrapServers)
    if (kerberosEnabled) adminClient.withProperties(securityConfigs: _*) else adminClient
  }

  private def createConsumer: F[Consumer[Array[Byte], Array[Byte]]] = ConsumerFactory.Default.create {
    val consumer = ConsumerSettings(new ByteArrayDeserializer, new ByteArrayDeserializer).withBootstrapServers(bootstrapServers)
    if (kerberosEnabled) consumer.withProperties(securityConfigs: _*) else consumer
  }


  import scala.collection.JavaConverters._

  def groups(adminClient: AdminClient): Stream[F, String] = Stream.eval(extractGroupsInternal(adminClient))
    .flatMap(l => Stream.fromIterator(l.iterator)).handleErrorWith { t =>
    log.error(t)("encountered with error when getting consumer groups")
    Stream.empty
  }

  private def extractGroupsInternal(adminClient: AdminClient): F[List[String]] = for {
    allGroups <- adminClient.listConsumerGroups().all().toF
    _ <- cs.shift
  } yield allGroups.asScala.toList.map(_.groupId())


  import scala.collection.JavaConverters._

  private def getOffsets(consumer: Consumer[_, _], tps: Iterable[TopicPartition], ts: Long): F[Map[TopicPartition, OffsetAndTimestamp]] = {
    val map = tps.map(t => (t, java.lang.Long.valueOf(ts))).toMap.asJava
    F.delay(consumer.offsetsForTimes(map).asScala.toMap)
  }

  def getBeginningOffsets(consumer: Consumer[_, _], tps: Iterable[TopicPartition]): F[Map[TopicPartition, OffsetAndTimestamp]] = getOffsets(consumer, tps, 0l)
    .map(i => i.filter(t => t._2!=null))

  def getEndOffsets(consumer: Consumer[_, _], tps: Iterable[TopicPartition]): F[Map[TopicPartition, OffsetAndTimestamp]] = F.delay {
    consumer.endOffsets(tps.toList.asJavaCollection).asScala.toMap
  }.flatMap { offsets =>
    offsets.map {
      case (k, v) => (k, new OffsetAndMetadata(v, Optional.empty(), null))
    }.map(i => getOffsetTimestamp(consumer, i._1, i._2).map(l => (i._1, new OffsetAndTimestamp(i._2.offset(), l))))
      .toList.sequence.map(_.toMap)
  }

  def collectGroupOffsets(adminClient: AdminClient, group: String, consumer: Consumer[_, _], endOffsets: Map[TopicPartition, OffsetAndTimestamp]): Stream[F, LagInfo] =
    for {
      tpOffsets <- Stream.eval {
        for {
          m <- adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().toF
          _ <- cs.shift
        } yield m.asScala.iterator
      }
      topicMetaTuple <- Stream.fromIterator(tpOffsets).handleErrorWith { t =>
        log.error(t)(s"encountered with error when getting groups topic and offset map group ${group}")
        Stream.empty
      }
      topicMetaTsTuple <- Stream.eval(getOffsetTimestamp(consumer, topicMetaTuple._1, topicMetaTuple._2).map(i => (topicMetaTuple._1, topicMetaTuple._2, i)))
    } yield LagInfo(group, topicMetaTsTuple._1.topic(), topicMetaTsTuple._1.partition(), topicMetaTsTuple._2.offset(), endOffsets(topicMetaTsTuple._1).offset(), topicMetaTsTuple._3, endOffsets(topicMetaTsTuple._1).timestamp())

  private def getOffsetTimestamp(consumer: Consumer[_, _], l: TopicPartition, r: OffsetAndMetadata): F[Long] = F.point(0l)

  // TODO we should add topicinfos to output of influx
  def program = for {
    adminClient <- Stream.bracket(createAdminClient)(i => F.delay(i.close()))
    consumer <- Stream.bracket(createConsumer)(i => F.delay(i.close()))
    _ <- Stream.fixedRate(repeatInterval).evalTap(_ => F.delay(log.info("collecting group info")))
    now <- Stream(System.currentTimeMillis())
    group <- groups(adminClient)
    topicInfos <- Stream.eval(getTopicInfos(adminClient))
    endOffsets <- Stream.eval(getEndOffsets(consumer, topicInfos))
    lag <- collectGroupOffsets(adminClient, group, consumer, endOffsets)
      .map(l => s"$metricPrefix,consumerGroup=${l.groupId},topic=${l.topic},partition=${l.partition} groupOffset=${l.groupOffset},latestOffset=${l.latestOffset} $now")
  } yield lag

  def collectLagInfo: F[(List[LagInfo], List[TopicSizeInfo])] =
    Resource.fromAutoCloseable(createConsumer).use { consumer =>
      Resource.fromAutoCloseable(createAdminClient).use { adminClient =>
        getTopicInfos(adminClient).flatMap { i =>
          getBeginningOffsets(consumer, i).map2(getEndOffsets(consumer, i))((starts, ends) => (starts, ends))
        }.flatMap { case (starts, ends) =>
          cs.shift *> groups(adminClient).compile.toList.flatMap { groupsL =>
            log.debug(s"collected groups $groupsL")
            val lags = groupsL.traverse {
              group => collectGroupOffsets(adminClient, group, consumer, ends).compile.toList
            }.map(_.flatMap(i => i)) // flatten list of list
          val sizes = ends.flatMap { case (tp, end) =>
            log.debug(s"collected tp:$tp end:$end startsMap:${starts.get(tp)}")
            starts.get(tp).orElse(Some(end)).map(start => TopicSizeInfo(tp.topic(), tp.partition(), start.offset(), end.offset(), start.timestamp(), end.timestamp()))
          }.toList
            lags.map(l => (l, sizes))
          } // we need context shift here otherwise it can create deadlock
        }
      }
    }

  private def getTopicInfos(adminClient: AdminClient): F[List[TopicPartition]] =
    for {
      i <- adminClient.listTopics().names().toF
      descs <- adminClient.describeTopics(i).all().toF
      _ <- cs.shift
    } yield descs.values().asScala.flatMap(i => i.partitions().asScala.map(x => new TopicPartition(i.name(), x.partition()))).toList
}
