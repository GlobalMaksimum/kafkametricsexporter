package com.globalmaksimum.kafkainfluxexporter

import java.util

import cats.effect.IO
import io.prometheus.client.{Collector, CounterMetricFamily}
import org.log4s.getLogger


class KafkaCollector(collector: ConsumerGroupCollector[IO], prefix: String) extends Collector {
  val log = getLogger

  import scala.collection.JavaConverters._

  override def collect(): util.List[Collector.MetricFamilySamples] = {
    try {
      val (lagInfo, sizeInfo) = collector.collectLagInfo.unsafeRunSync()

      if(log.isDebugEnabled){
        lagInfo.foreach(i=>log.debug(i.toString))
        sizeInfo.foreach(i=>log.debug(i.toString))
      }

      val groupOffset = lagInfo.foldLeft(new CounterMetricFamily(
        s"kafka_consumergroup_group_offset",
        "The offset of the last consumed offset for this partition in this topic partition for this group.",
        List("cluster", "group", "topic", "partition").asJava)) { (family, l) =>
        family.addMetric(List(prefix, l.groupId, l.topic, l.partition.toString).asJava, l.groupOffset)
      }
      val groupOffsetLag = lagInfo.foldLeft(new CounterMetricFamily(
        s"kafka_consumergroup_group_lag",
        "consumer group lags",
        List("cluster", "group", "topic", "partition").asJava)) { (family, l) =>
        family.addMetric(List(prefix, l.groupId, l.topic, l.partition.toString).asJava, l.latestOffset - l.groupOffset)
      }

      val latestOffset = sizeInfo.foldLeft(new CounterMetricFamily(
        s"kafka_partition_latest_offset",
        "The latest offset available for topic partition.",
        List("cluster", "topic", "partition").asJava)) { (family, l) =>
        family.addMetric(List(prefix, l.topic, l.partition.toString).asJava, l.endOffset)
      }

      val earliestOffset = sizeInfo.foldLeft(new CounterMetricFamily(
        s"kafka_partition_earliest_offset",
        "The latest offset available for topic partition.",
        List("cluster", "topic", "partition").asJava)) { (family, l) =>
        family.addMetric(List(prefix, l.topic, l.partition.toString).asJava, l.beginningOffset)
      }


      util.Arrays.asList(List(groupOffset, groupOffsetLag, latestOffset, earliestOffset).toArray: _*)
    } catch {
      case t => log.error(t)("error")
        List.empty.asJava
    }
  }
}
