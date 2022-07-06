package scorex.flow

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp.{Bind, Bound, Message => _}
import akka.testkit.TestProbe
import scorex.core.app.ScorexContext
import scorex.core.network.NetworkController.ReceivableMessages.EmptyPeerDatabase
import scorex.core.network._
import scorex.core.network.dns.DnsClientRef
import scorex.core.network.dns.model.{DnsClientInput, DnsSeederDomain}
import scorex.core.network.message._
import scorex.core.network.peer.PeerManager.ReceivableMessages.GetAllPeers
import scorex.core.network.peer._
import scorex.core.settings.ScorexSettings
import scorex.core.utils.LocalTimeProvider
import scorex.network.NetworkTests

import java.net.{InetAddress, InetSocketAddress}
import scala.util.Try

class DnsLookupFlow extends NetworkTests {
  type Data = Map[InetSocketAddress, PeerInfo]

  import scala.concurrent.ExecutionContext.Implicits.global

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
  private val featureSerializers = Map(LocalAddressPeerFeature.featureId -> LocalAddressPeerFeatureSerializer)

  it should "perform a dns lookup and fill the internal peer database with new received IPs" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val probe = TestProbe("probe")(system)
    implicit val defaultSender: ActorRef = probe.testActor

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val scorexSettings = settings.copy(network = settings.network.copy(bindAddress = nodeAddr))
    val (networkControllerRef, peerManager) = createNetworkController(scorexSettings, tcpManagerProbe)

    // Act
    networkControllerRef ! EmptyPeerDatabase()

    // Assert
    Thread.sleep(1000)
    peerManager ! GetAllPeers
    val data = probe.expectMsgClass(classOf[Data])
    data.size should be(fakeIpSeqOne.size)

    system.terminate()
  }

  it should "fill peer database with dns response when PeerSynchronizer is instantiated" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val probe = TestProbe("probe")(system)
    implicit val defaultSender: ActorRef = probe.testActor

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val scorexSettings = settings.copy(network = settings.network.copy(bindAddress = nodeAddr))
    val (_, peerManager) = createNetworkController(scorexSettings, tcpManagerProbe)

    // Assert
    Thread.sleep(1000)
    peerManager ! GetAllPeers
    val data = probe.expectMsgClass(classOf[Data])
    data.size should be(fakeIpSeqOne.size)

    system.terminate()
  }

  private def createNetworkController(settings: ScorexSettings, tcpManagerProbe: TestProbe, upnp: Option[UPnPGateway] = None)(implicit system: ActorSystem) = {
    val timeProvider = LocalTimeProvider
    val externalAddr = settings.network.declaredAddress
      .orElse(upnp.map(u => new InetSocketAddress(u.externalAddress, settings.network.bindAddress.getPort)))

    val peersSpec: PeersSpec = new PeersSpec(featureSerializers, settings.network.maxPeerSpecObjects)
    val messageSpecs = Seq(GetPeersSpec, peersSpec)
    val scorexContext = ScorexContext(messageSpecs, Seq.empty, upnp, timeProvider, externalAddr)

    val dnsClientInput = new DnsClientInput(Seq(fakeURLOne, fakeURLTwo), mockLookupFunction)
    val dnsClientRef = DnsClientRef(dnsClientInput)

    val peerManagerRef = PeerManagerRef(settings, scorexContext)

    val networkControllerRef: ActorRef = NetworkControllerRef(
      "networkController", settings.network,
      peerManagerRef, scorexContext, tcpManagerProbe.testActor)

    val peerSynchronizer: ActorRef = PeerSynchronizerRef("PeerSynchronizer",
      networkControllerRef, peerManagerRef, dnsClientRef, settings.network, featureSerializers)

    tcpManagerProbe.expectMsg(Bind(networkControllerRef, settings.network.bindAddress, options = Nil))

    tcpManagerProbe.send(networkControllerRef, Bound(settings.network.bindAddress))
    (networkControllerRef, peerManagerRef)
  }
}