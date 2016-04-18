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
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLongArray
import org.HdrHistogram.Histogram
import akka.stream.ThrottleMode

object AeronStreamLatencySpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(debugConfig(on = false).withFallback(
    ConfigFactory.parseString(s"""
       AeronStreamLatencySpec.totalMessagesFactor = 1.0
       AeronStreamLatencySpec.repeatCount = 1
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
      case `first`  ⇒ 20521 // TODO yeah, we should have support for dynamic port assignment
      case `second` ⇒ 20522
    }

  final case class TestSettings(
    testName: String,
    messageRate: Int, // msg/s
    payloadSize: Int,
    repeat: Int)

}

class AeronStreamLatencySpecMultiJvmNode1 extends AeronStreamLatencySpec
class AeronStreamLatencySpecMultiJvmNode2 extends AeronStreamLatencySpec

abstract class AeronStreamLatencySpec
  extends MultiNodeSpec(AeronStreamLatencySpec)
  with STMultiNodeSpec with ImplicitSender {

  import AeronStreamLatencySpec._

  val totalMessagesFactor = system.settings.config.getDouble("AeronStreamLatencySpec.totalMessagesFactor")
  val repeatCount = system.settings.config.getInt("AeronStreamLatencySpec.repeatCount")

  val aeron = {
    val ctx = new Aeron.Context
    val driver = MediaDriver.launchEmbedded()
    ctx.aeronDirectoryName(driver.aeronDirectoryName)
    Aeron.connect(ctx)
  }

  lazy implicit val mat = ActorMaterializer()(system)
  import system.dispatcher

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

  def printTotal(testName: String, payloadSize: Long, histogram: Histogram): Unit = {
    println(s"=== $testName: Histogram of RTT latencies in microseconds.")
    histogram.outputPercentileDistribution(System.out, 1000.0)
  }

  val scenarios = List(
    TestSettings(
      testName = "LatencyAeronStreams-rate-100-size-100",
      messageRate = 100,
      payloadSize = 100,
      repeat = repeatCount),
    TestSettings(
      testName = "LatencyAeronStreams-rate-1000-size-100",
      messageRate = 1000,
      payloadSize = 100,
      repeat = repeatCount),
    TestSettings(
      testName = "LatencyAeronStreams-rate-10000-size-100",
      messageRate = 10000,
      payloadSize = 100,
      repeat = repeatCount),
    TestSettings(
      testName = "AeronStreams-rate-1000-size-1k",
      messageRate = 1000,
      payloadSize = 1000,
      repeat = repeatCount))

  def test(testSettings: TestSettings): Unit = {
    import testSettings._

    runOn(first) {
      val payload = ("0" * payloadSize).getBytes("utf-8")
      val totalMessages = (10 * messageRate * totalMessagesFactor).toInt
      val sendTimes = new AtomicLongArray(totalMessages)
      val histogram = new Histogram(SECONDS.toNanos(10), 3)

      val rep = reporter(testName)
      val barrier = new CyclicBarrier(2)
      val count = new AtomicInteger
      Source.fromGraph(new AeronSource(channel(first), aeron))
        .runForeach { bytes ⇒
          if (bytes.length != payloadSize) throw new IllegalArgumentException("Invalid message")
          rep.onMessage(1, payloadSize)
          val c = count.incrementAndGet()
          val d = System.nanoTime() - sendTimes.get(c - 1)
          if (c % (totalMessages / 10) == 0)
            println(s"# receive offset $c => ${d / 1000} µs") // FIXME
          histogram.recordValue(d)
          if (c == totalMessages) {
            printTotal(testName, bytes.length, histogram)
            barrier.await() // this is always the last party
          }
        }

      for (n ← 1 to repeat) {
        histogram.reset()
        count.set(0)

        Source(1 to totalMessages)
          .throttle(messageRate, 1.second, math.max(messageRate / 10, 1), ThrottleMode.Shaping)
          .map { n ⇒
            if (n % (totalMessages / 10) == 0)
              println(s"# send offset $n") // FIXME
            sendTimes.set(n - 1, System.nanoTime())
            payload
          }
          .runWith(new AeronSink(channel(second), aeron))

        barrier.await((totalMessages / messageRate) + 5, SECONDS)
      }

      rep.halt()
    }

    enterBarrier("after-" + testName)
  }

  "Latency of Aeron Streams" must {

    "start echo" in {
      runOn(second) {
        // just echo back on
        Source.fromGraph(new AeronSource(channel(second), aeron))
          .runWith(new AeronSink(channel(first), aeron))
      }
      enterBarrier("echo-started")
    }

    for (s ← scenarios) {
      s"be low for ${s.testName}, at ${s.messageRate} msg/s, payloadSize = ${s.payloadSize}" in test(s)
    }

    // TODO add more tests

  }
}
