package com.globalmaksimum.kafkainfluxexporter

import cats.effect.internals.IOAppPlatform
import cats.effect.{ContextShift, IO, IOApp, Resource, Timer}
import fs2.kafka.{AdminClientFactory, AdminClientSettings, ConsumerFactory, ConsumerSettings}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object MainAdmin {

  import scala.collection.JavaConverters._

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  implicit def timer: Timer[IO] = IO.timer(ExecutionContext.global)

  def main(args: Array[String]): Unit = {
    val bootstraps = "localhost:19094,localhost:29094,localhost:39094"
    val client = AdminClientFactory.Default.create[IO] {

      AdminClientSettings.Default.withBootstrapServers(bootstraps)
    }



    /*val consumer = ConsumerFactory.Default.create[IO,Array[Byte],Array[Byte]] {
      ConsumerSettings(new ByteArrayDeserializer, new ByteArrayDeserializer).withBootstrapServers("localhost:19094,localhost:29094,localhost:39094")
    }.unsafeRunSync()*/

    /*val groups = Resource.fromAutoCloseable(client).use { adminClient =>
      IO.delay(adminClient.listConsumerGroups().all().get().asScala)
    }
    groups.unsafeRunSync()*/

    val collector = new ConsumerGroupCollector[IO](bootstraps, 10 seconds, "group_offsets", false)
    val result = collector.collectAll.unsafeRunSync()
    println("result got")


  }
}
