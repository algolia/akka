/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.net.InetAddress
import java.util.concurrent.Executors

import scala.collection.AbstractIterator
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit._
import com.typesafe.config.ConfigFactory

import io.aeron.Aeron
import io.aeron.driver.MediaDriver

object AeronStreamMaxThroughputSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  val hostname =
    if (java.lang.Boolean.getBoolean("akka.test.multi-node"))
      InetAddress.getLocalHost.getHostAddress // or InetAddress.getLocalHost.getHostName
    else
      "localhost"

  commonConfig(debugConfig(on = false).withFallback(
    ConfigFactory.parseString(s"""
       AeronStreamMaxThroughputSpec.totalMessagesFactor = 1.0
       akka {
         loglevel = INFO
         testconductor.barrier-timeout = 60s
         actor {
           provider = "akka.remote.RemoteActorRefProvider"
           serialize-creators = false
           serialize-messages = false
         }
         remote.artery.enabled = off
       }
       """)))

  def aeronPort(roleName: RoleName): Int =
    roleName match {
      case `first`  ⇒ 20511 // TODO yeah, we should have support for dynamic port assignment
      case `second` ⇒ 20512
    }

  def iterate(start: Long, end: Long): Iterator[Long] = new AbstractIterator[Long] {
    private[this] var first = true
    private[this] var acc = start
    def hasNext: Boolean = acc < end
    def next(): Long = {
      if (!hasNext) throw new NoSuchElementException("next on empty iterator")
      if (first) first = false
      else acc += 1

      acc
    }
  }
}

class AeronStreamMaxThroughputSpecMultiJvmNode1 extends AeronStreamMaxThroughputSpec
class AeronStreamMaxThroughputSpecMultiJvmNode2 extends AeronStreamMaxThroughputSpec

abstract class AeronStreamMaxThroughputSpec
  extends MultiNodeSpec(AeronStreamMaxThroughputSpec)
  with STMultiNodeSpec with ImplicitSender {

  import AeronStreamMaxThroughputSpec._

  val totalMessagesFactor = system.settings.config.getDouble("AeronStreamMaxThroughputSpec.totalMessagesFactor")

  val aeron = {
    val ctx = new Aeron.Context
    val driver = MediaDriver.launchEmbedded()
    ctx.aeronDirectoryName(driver.aeronDirectoryName)
    Aeron.connect(ctx)
  }

  lazy implicit val mat = ActorMaterializer()(system)
  import system.dispatcher

  def totalMessages(n: Long): Long = (n * totalMessagesFactor).toLong

  override def initialParticipants = roles.size

  def channel(roleName: RoleName) = {
    val a = node(roleName).address
    s"aeron:udp?endpoint=${a.host.get}:${aeronPort(roleName)}"
  }

  lazy val reporterExecutor = Executors.newFixedThreadPool(1)
  def reporter(name: String): RateReporter = {
    val r = new RateReporter(SECONDS.toNanos(1), new RateReporter.Reporter {
      override def onReport(messagesPerSec: Double, bytesPerSec: Double, totalMessages: Long, totalBytes: Long): Unit = {
        println(name + ": %.03g msgs/sec, %.03g bytes/sec, totals %d messages %d MB".format(
          messagesPerSec, bytesPerSec, totalMessages, totalBytes / (1024 * 1024)))
      }
    })
    reporterExecutor.execute(r)
    r
  }

  override def afterAll(): Unit = {
    reporterExecutor.shutdown()
    super.afterAll()
  }

  def printTotal(testName: String, total: Long, cmd: String, startTime: Long, payloadSize: Long): Unit = {
    val d = (System.nanoTime - startTime).nanos.toMillis
    println(f"== $testName: $total $cmd of size ${payloadSize} bytes took $d ms, " +
      f"${1000.0 * total / d}%.03g msg/s, ${1000.0 * total * payloadSize / d}%.03g bytes/s")
  }

  def test(testName: String, messages: Long, payloadSize: Int): Unit = {
    val receiverName = testName + "-rcv"

    runOn(second) {
      val rep = reporter(testName)
      var t0 = System.nanoTime()
      var count = 0L
      val done = TestLatch(1)
      Source.fromGraph(new AeronSource(channel(second), () ⇒ aeron))
        .runForeach { bytes ⇒
          rep.onMessage(1, bytes.length)
          count += 1
          if (count == 1) {
            t0 = System.nanoTime()
          } else if (count == messages) {
            printTotal(testName, messages, "receive", t0, payloadSize)
            done.countDown()
          }
        }.onFailure {
          case e ⇒
            e.printStackTrace
        }

      enterBarrier(receiverName + "-started")
      Await.ready(done, 60.seconds * totalMessagesFactor)
      rep.halt()
      enterBarrier(testName + "-done")
    }

    runOn(first) {
      enterBarrier(receiverName + "-started")

      val payload = ("0" * payloadSize).getBytes("utf-8")
      val t0 = System.nanoTime()
      Source.fromIterator(() ⇒ iterate(1, messages))
        .map { n ⇒
          if (n == messages) {
            printTotal(testName, messages, "send", t0, payloadSize)
          }
          payload
        }
        .runWith(new AeronSink(channel(second), () ⇒ aeron))

      enterBarrier(testName + "-done")
    }

    enterBarrier("after-" + testName)
  }

  "Max throughput of Aeron Streams" must {
    "be great for payloadSize = 100" in {
      test(
        testName = "AeronStreams-1",
        messages = totalMessages(10000000),
        payloadSize = 100)
    }

    // TODO add more tests

  }
}
