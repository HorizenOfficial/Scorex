package scorex.core.network.dns

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import scorex.core.network.PeerSynchronizer.ReceivableMessages.LookupResponse
import scorex.core.network.dns.DnsClient.ReceivableMessages.LookupRequest
import scorex.core.network.dns.model.{DnsClientInput, DnsSeederDomain}
import scorex.core.network.dns.strategy.Strategy.{LeastNodeQuantity, MaxNodeQuantity, ThresholdNodeQuantity}

import java.net.InetAddress
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Try

class DnsClientSpec extends AnyFlatSpec {
  private val fakeURLOne = new DnsSeederDomain("fake-url.com")
  private val fakeIpSeqOne = Seq(
    InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3"), InetAddress.getByName("127.0.0.4")
  )

  private val fakeURLTwo = new DnsSeederDomain("fake-url2.com")
  private val fakeIpSeqTwo = Seq(InetAddress.getByName("127.0.1.1"))

  private val fakeURLThree = new DnsSeederDomain("fake-url3.com")
  private val fakeIpSeqThree = Seq(InetAddress.getByName("127.0.1.1"), InetAddress.getByName("127.0.1.2"))

  private val mockLookupFunction = (url: DnsSeederDomain) => url match {
    case `fakeURLOne` => Try(fakeIpSeqOne)
    case `fakeURLTwo` => Try(fakeIpSeqTwo)
    case `fakeURLThree` => Try(fakeIpSeqThree)
    case _ => throw new IllegalArgumentException("Unexpected url in test")
  }

  "A DnsClient" should "reply with the least number of IPs" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    implicit val timeout: Timeout = Timeout(5 seconds)

    val dnsClientParams = DnsClientInput(
      Seq(fakeURLOne, fakeURLTwo, fakeURLThree),
      mockLookupFunction
    )

    val dnsClient: ActorRef = DnsClientRef(dnsClientParams)

    // Act
    val futureResponse = (dnsClient ? LookupRequest(LeastNodeQuantity())).mapTo[LookupResponse]
    val response: LookupResponse = Await.result(futureResponse, 5 seconds)

    // Assert
    response.ipv4Addresses.size should be(fakeIpSeqOne.size)

    system.terminate()
  }

  "A DnsClient" should "reply with the max number of IPs" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    implicit val timeout: Timeout = Timeout(5 seconds)

    val dnsClientParams = DnsClientInput(
      Seq(fakeURLOne, fakeURLTwo, fakeURLThree),
      mockLookupFunction
    )

    val dnsClient: ActorRef = DnsClientRef(dnsClientParams)

    // Act
    val futureResponse = (dnsClient ? LookupRequest(MaxNodeQuantity())).mapTo[LookupResponse]
    val response: LookupResponse = Await.result(futureResponse, 5 seconds)

    // Assert
    response.ipv4Addresses.size should be(fakeIpSeqOne.size + fakeIpSeqTwo.size + fakeIpSeqThree.size)

    system.terminate()
  }

  "A DnsClient" should "reply with a threshold number of IPs" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    implicit val timeout: Timeout = Timeout(5 seconds)

    val nodesThreshold = 3

    val dnsClientParams = DnsClientInput(
      Seq(fakeURLOne, fakeURLTwo, fakeURLThree),
      mockLookupFunction
    )

    val dnsClient: ActorRef = DnsClientRef(dnsClientParams)

    // Act
    val futureResponse = (dnsClient ? LookupRequest(ThresholdNodeQuantity(nodesThreshold))).mapTo[LookupResponse]
    val response: LookupResponse = Await.result(futureResponse, 5 seconds)

    // Assert
    response.ipv4Addresses.size should (be <= fakeIpSeqOne.size and be >= nodesThreshold)

    system.terminate()
  }

  "A DnsClient" should "reply with a threshold number of IPs bis" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    implicit val timeout: Timeout = Timeout(5 seconds)

    val nodesThreshold = 5

    val dnsClientParams = DnsClientInput(
      Seq(fakeURLOne, fakeURLTwo, fakeURLThree),
      mockLookupFunction
    )

    val dnsClient: ActorRef = DnsClientRef(dnsClientParams)

    // Act
    val futureResponse = (dnsClient ? LookupRequest(ThresholdNodeQuantity(nodesThreshold))).mapTo[LookupResponse]
    val response: LookupResponse = Await.result(futureResponse, 5 seconds)

    // Assert
    response.ipv4Addresses.size should be(fakeIpSeqOne.size + fakeIpSeqTwo.size)

    system.terminate()
  }
}
