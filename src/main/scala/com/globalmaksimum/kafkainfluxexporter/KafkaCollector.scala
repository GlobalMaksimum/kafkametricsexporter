package com.globalmaksimum.kafkainfluxexporter

import java.util

import cats.effect.IO
import io.prometheus.client.{Collector, CounterMetricFamily}
import org.log4s.getLogger

import scala.concurrent.Await
import scala.concurrent.duration._
class KafkaCollector(collector: ConsumerGroupCollector[IO],prefix:String) extends Collector {
  val log = getLogger
  override def collect(): util.List[Collector.MetricFamilySamples] = {
    val grouped = collector.collectAll.map(_.groupBy(_.topic).toList)
    val scalaList = try {
      grouped.map { topicList =>
        topicList.map { case (topic, list) =>
          val labelList = util.Arrays.asList("partition", "groupId", "value")
          list.foldLeft(new CounterMetricFamily(s"$prefix$topic", "topic info", labelList)) { case (counters, lagInfo) =>
            counters.addMetric(util.Arrays.asList(lagInfo.partition.toString, lagInfo.groupId, "groupOffset"), lagInfo.groupOffset)
            counters.addMetric(util.Arrays.asList(lagInfo.partition.toString, lagInfo.groupId, "latestOffset"), lagInfo.latestOffset)
          }
        }
      }.unsafeRunSync()
    } catch {
      case t=> log.error(t)("error")
        List.empty
    }
    log.info("collection finished")
    java.util.Arrays.asList(scalaList.toArray: _*)
  }
}
