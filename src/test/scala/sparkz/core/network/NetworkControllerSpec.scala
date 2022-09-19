package sparkz.core.network

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp
import akka.io.Tcp.{Bind, Bound, Connect, Connected, Message => TcpMessage}
import akka.testkit.TestProbe
import akka.util.ByteString
import akka.io.Tcp.{Message => _, _}
import org.scalatest.EitherValues._
import org.scalatest.OptionValues._
import org.scalatest.TryValues._
import org.scalatest.matchers.should.Matchers
import sparkz.core.app.{SparkzContext, Version}
import sparkz.core.network.NetworkController.ReceivableMessages.Internal.ConnectionToPeer
import sparkz.core.network.NetworkController.ReceivableMessages.{GetConnectedPeers, GetPeersStatus}
import sparkz.core.network.message._
import sparkz.core.network.peer.PeerManager.ReceivableMessages.AddOrUpdatePeer
import sparkz.core.network.peer._
import sparkz.core.settings.SparkzSettings
import sparkz.core.utils.LocalTimeProvider

import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

class NetworkControllerSpec extends NetworkTests {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val featureSerializers = Map(LocalAddressPeerFeature.featureId -> LocalAddressPeerFeatureSerializer)

  "A NetworkController" should "send local address on handshake when peer and node address are in localhost" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings, tcpManagerProbe)
    val testPeer = new TestPeer(settings, networkControllerRef, tcpManagerProbe)

    val peerAddr = new InetSocketAddress("127.0.0.1", 5678)
    val nodeAddr = new InetSocketAddress("127.0.0.1", settings.network.bindAddress.getPort)
    testPeer.connectAndExpectSuccessfulMessages(peerAddr, nodeAddr, Tcp.ResumeReading)

    val handshakeFromNode = testPeer.receiveHandshake
    handshakeFromNode.peerSpec.declaredAddress shouldBe empty
    val localAddressFeature = extractLocalAddrFeat(handshakeFromNode)
    localAddressFeature.value should be(nodeAddr)

    system.terminate()
  }

  it should "send local address on handshake when the peer are in local network" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings, tcpManagerProbe)
    val testPeer = new TestPeer(settings, networkControllerRef, tcpManagerProbe)

    val nodeAddr = new InetSocketAddress("127.0.0.1", settings.network.bindAddress.getPort)
    testPeer.connectAndExpectSuccessfulMessages(new InetSocketAddress("192.168.0.1", 5678), nodeAddr, Tcp.ResumeReading)

    val handshakeFromNode = testPeer.receiveHandshake
    handshakeFromNode.peerSpec.declaredAddress shouldBe empty
    val localAddressFeature = extractLocalAddrFeat(handshakeFromNode)
    localAddressFeature.value should be(nodeAddr)

    system.terminate()
  }

  it should "not send local address on handshake when the peer is external" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings, tcpManagerProbe)
    val testPeer = new TestPeer(settings, networkControllerRef, tcpManagerProbe)

    val nodeAddr = new InetSocketAddress("127.0.0.1", settings.network.bindAddress.getPort)
    testPeer.connectAndExpectSuccessfulMessages(new InetSocketAddress("88.77.66.55", 5678), nodeAddr, Tcp.ResumeReading)

    val handshakeFromNode = testPeer.receiveHandshake
    handshakeFromNode.peerSpec.declaredAddress shouldBe empty
    val localAddressFeature = extractLocalAddrFeat(handshakeFromNode)
    localAddressFeature shouldBe empty

    testPeer.sendHandshake(None, None)
    system.terminate()
  }

  it should "send declared address when node and peer are public" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val bindAddress = new InetSocketAddress("88.77.66.55", 12345)
    val settings2 = settings.copy(network = settings.network.copy(bindAddress = bindAddress))
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings2, tcpManagerProbe)
    val testPeer = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)

    testPeer.connectAndExpectSuccessfulMessages(new InetSocketAddress("88.77.66.55", 5678), bindAddress, Tcp.ResumeReading)

    val handshakeFromNode = testPeer.receiveHandshake
    handshakeFromNode.peerSpec.declaredAddress.value should be(bindAddress)
    val localAddressFeature = extractLocalAddrFeat(handshakeFromNode)
    localAddressFeature shouldBe empty

    testPeer.sendHandshake(None, None)
    system.terminate()
  }

  it should "send known public peers" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val settings2 = settings.copy(network = settings.network.copy(bindAddress = nodeAddr))
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings2, tcpManagerProbe)
    val testPeer1 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    val peer1Addr = new InetSocketAddress("88.77.66.55", 5678)
    testPeer1.connectAndExpectSuccessfulMessages(peer1Addr, nodeAddr, Tcp.ResumeReading)
    testPeer1.receiveHandshake
    testPeer1.sendHandshake(Some(peer1Addr), None)
    testPeer1.receiveGetPeers
    testPeer1.sendPeers(Seq.empty)

    val peer2Addr = new InetSocketAddress("88.77.66.56", 5678)
    val testPeer2 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    testPeer2.connectAndExpectSuccessfulMessages(peer2Addr, nodeAddr, Tcp.ResumeReading)
    testPeer2.receiveHandshake
    testPeer2.sendHandshake(Some(peer2Addr), None)

    testPeer1.sendGetPeers
    testPeer1.receivePeers.flatMap(_.declaredAddress) should contain theSameElementsAs Seq(peer1Addr, peer2Addr)

    system.terminate()
  }

  it should "send known local peers" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val settings2 = settings.copy(network = settings.network.copy(bindAddress = nodeAddr))
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings2, tcpManagerProbe)

    val testPeer1 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    val peer1DecalredAddr = new InetSocketAddress("88.77.66.55", 5678)
    val peer1LocalAddr = new InetSocketAddress("192.168.1.55", 5678)
    testPeer1.connectAndExpectSuccessfulMessages(peer1LocalAddr, nodeAddr, Tcp.ResumeReading)
    testPeer1.receiveHandshake
    testPeer1.sendHandshake(Some(peer1DecalredAddr), Some(peer1LocalAddr))
    testPeer1.receiveGetPeers
    testPeer1.sendPeers(Seq.empty)

    val peer2DeclaredAddr = new InetSocketAddress("88.77.66.56", 5678)
    val peer2LocalAddr = new InetSocketAddress("192.168.1.56", 5678)
    val testPeer2 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    testPeer2.connectAndExpectSuccessfulMessages(peer2LocalAddr, nodeAddr, Tcp.ResumeReading)
    testPeer2.receiveHandshake
    testPeer2.sendHandshake(Some(peer2DeclaredAddr), Some(peer2LocalAddr))

    testPeer1.sendGetPeers
    testPeer1.receivePeers.flatMap(_.localAddressOpt) should contain theSameElementsAs Seq(peer1LocalAddr, peer2LocalAddr)

    system.terminate()
  }

  it should "send close message when max connections threshold is reached" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val settings2 = settings.copy(network = settings.network.copy(bindAddress = nodeAddr, maxConnections = 1))
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings2, tcpManagerProbe)

    val testPeer1 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    val peer1DecalredAddr = new InetSocketAddress("88.77.66.55", 5678)
    val peer1LocalAddr = new InetSocketAddress("192.168.1.55", 5678)
    testPeer1.connectAndExpectSuccessfulMessages(peer1LocalAddr, nodeAddr, Tcp.ResumeReading)
    testPeer1.receiveHandshake
    testPeer1.sendHandshake(Some(peer1DecalredAddr), Some(peer1LocalAddr))
    testPeer1.receiveGetPeers
    testPeer1.sendPeers(Seq.empty)

    val peer2LocalAddr = new InetSocketAddress("192.168.1.56", 5678)
    val testPeer2 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    testPeer2.connectAndExpectMessage(peer2LocalAddr, nodeAddr, Tcp.Close)

    system.terminate()
  }

  it should "not send known local address of peer when node is not in local network" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val settings2 = settings.copy(network = settings.network.copy(bindAddress = nodeAddr))
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings2, tcpManagerProbe)

    val testPeer1 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    val peer1DecalredAddr = new InetSocketAddress("88.77.66.55", 5678)
    val peer1LocalAddr = new InetSocketAddress("192.168.1.55", 5678)
    testPeer1.connectAndExpectSuccessfulMessages(peer1LocalAddr, nodeAddr, Tcp.ResumeReading)
    testPeer1.receiveHandshake
    testPeer1.sendHandshake(Some(peer1DecalredAddr), Some(peer1LocalAddr))
    testPeer1.receiveGetPeers
    testPeer1.sendPeers(Seq.empty)

    val peer2DeclaredAddr = new InetSocketAddress("88.77.66.56", 5678)
    val testPeer2 = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    testPeer2.connectAndExpectSuccessfulMessages(peer2DeclaredAddr, nodeAddr, Tcp.ResumeReading)
    testPeer2.receiveHandshake
    testPeer2.sendHandshake(Some(peer2DeclaredAddr), None)

    testPeer2.sendGetPeers
    testPeer2.receivePeers should not contain peer1LocalAddr

    system.terminate()
  }

  it should "not connect to itself" in {
    implicit val system = ActorSystem()

    val tcpManagerProbe = TestProbe()

    val nodeAddr = new InetSocketAddress("127.0.0.1", settings.network.bindAddress.getPort)
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings, tcpManagerProbe)
    val testPeer = new TestPeer(settings, networkControllerRef, tcpManagerProbe)

    val peerLocalAddress = new InetSocketAddress("192.168.1.2", settings.network.bindAddress.getPort)

    testPeer.connectAndExpectSuccessfulMessages(new InetSocketAddress("192.168.1.2", 5678), nodeAddr, Tcp.ResumeReading)

    val handshakeFromNode = testPeer.receiveHandshake
    val nodeLocalAddress = extractLocalAddrFeat(handshakeFromNode).value
    testPeer.sendHandshake(None, Some(peerLocalAddress))
    testPeer.sendPeers(Seq(getPeerInfo(nodeLocalAddress).peerSpec))

    testPeer.sendGetPeers
    val peers = testPeer.receivePeers

    peers.flatMap(_.address) should contain theSameElementsAs Seq(peerLocalAddress)
    system.terminate()
  }

  it should "update last-seen on getting message from peer" in {
    implicit val system = ActorSystem()
    val tcpManagerProbe = TestProbe()
    val p = TestProbe("p")(system)

    val nodeAddr = new InetSocketAddress("88.77.66.55", 12345)
    val settings2 = settings.copy(network = settings.network.copy(bindAddress = nodeAddr))
    val (networkControllerRef: ActorRef, _) = createNetworkController(settings2, tcpManagerProbe)

    val testPeer = new TestPeer(settings2, networkControllerRef, tcpManagerProbe)
    val peerAddr = new InetSocketAddress("88.77.66.55", 5678)

    testPeer.connectAndExpectSuccessfulMessages(peerAddr, nodeAddr, Tcp.ResumeReading)
    testPeer.receiveHandshake
    testPeer.sendHandshake(Some(peerAddr), None)

    p.send(networkControllerRef, GetConnectedPeers)
    val data0 = p.expectMsgClass(classOf[Seq[ConnectedPeer]])
    val ls0 = data0(0).lastMessage

    Thread.sleep(1000)
    testPeer.sendGetPeers() // send a message to see node's status update then

    p.send(networkControllerRef, GetConnectedPeers)
    val data = p.expectMsgClass(classOf[Seq[ConnectedPeer]])
    val ls = data(0).lastMessage
    ls should not be ls0

    p.send(networkControllerRef, GetPeersStatus)
    val status = p.expectMsgClass(classOf[PeersStatus])
    status.lastIncomingMessage shouldBe ls

    system.terminate()
  }

  it should "skip connection attempt if the only connection in PeerManager is marked as unconfirmed" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    val tcpManagerProbe = TestProbe()

    val (networkControllerRef, peerManager) = createNetworkController(settings, tcpManagerProbe)

    val peerAddressOne = new InetSocketAddress("88.77.66.55", 12345)
    val peerInfoOne = getPeerInfo(peerAddressOne)

    val activeConnections = Map.empty[InetSocketAddress, ConnectedPeer]
    var unconfirmedConnections = Set.empty[InetSocketAddress]

    unconfirmedConnections += peerAddressOne

    // Act
    peerManager ! AddOrUpdatePeer(peerInfoOne)
    networkControllerRef ! ConnectionToPeer(activeConnections, unconfirmedConnections)

    // Assert
    tcpManagerProbe.expectNoMessage()

    system.terminate()
  }

  it should "select the only candidate existing in the PeerManager since there are neither active or unconfirmed connections" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    val tcpManagerProbe = TestProbe()

    val (networkControllerRef, peerManager) = createNetworkController(settings, tcpManagerProbe)

    val peerAddressOne = new InetSocketAddress("88.77.66.55", 12345)
    val peerInfoOne = getPeerInfo(peerAddressOne)

    val activeConnections = Map.empty[InetSocketAddress, ConnectedPeer]
    val unconfirmedConnections = Set.empty[InetSocketAddress]

    // Act
    peerManager ! AddOrUpdatePeer(peerInfoOne)
    networkControllerRef ! ConnectionToPeer(activeConnections, unconfirmedConnections)

    // Assert
    tcpManagerProbe.expectMsgPF() {
      case ok@Connect(remoteAddress, _, _, _, _) if remoteAddress == peerAddressOne => ok
      case _ => fail("Unexpected message received")
    }

    system.terminate()
  }

  it should "skip the peer connection attempt since the only peer in the PeerManager is already connected" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    val tcpManagerProbe = TestProbe()

    val (networkControllerRef, peerManagerRef) = createNetworkController(settings, tcpManagerProbe)

    val peerAddressOne = new InetSocketAddress("88.77.66.55", 12345)
    val peerInfoOne = getPeerInfo(peerAddressOne)

    var activeConnections = Map.empty[InetSocketAddress, ConnectedPeer]
    val unconfirmedConnections = Set.empty[InetSocketAddress]

    activeConnections += peerAddressOne -> ConnectedPeer(
      ConnectionId(peerAddressOne, peerAddressOne, Incoming),
      networkControllerRef,
      1,
      Some(peerInfoOne)
    )

    // Act
    peerManagerRef ! AddOrUpdatePeer(peerInfoOne)
    networkControllerRef ! ConnectionToPeer(activeConnections, unconfirmedConnections)

    // Assert
    tcpManagerProbe.expectNoMessage()

    system.terminate()
  }

  it should "connect to the second peer since is not an active or unconfirmed connection" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    val tcpManagerProbe = TestProbe()

    val (networkControllerRef, peerManagerRef) = createNetworkController(settings, tcpManagerProbe)

    val peerAddressOne = new InetSocketAddress("88.77.66.55", 12345)
    val peerInfoOne = getPeerInfo(peerAddressOne)
    val peerAddressTwo = new InetSocketAddress("55.66.77.88", 11223)
    val peerInfoTwo = getPeerInfo(peerAddressTwo)

    var activeConnections = Map.empty[InetSocketAddress, ConnectedPeer]
    val unconfirmedConnections = Set.empty[InetSocketAddress]

    activeConnections += peerAddressOne -> ConnectedPeer(
      ConnectionId(peerAddressOne, peerAddressOne, Incoming),
      networkControllerRef,
      1,
      Some(peerInfoOne)
    )

    // Act
    peerManagerRef ! AddOrUpdatePeer(peerInfoOne)
    peerManagerRef ! AddOrUpdatePeer(peerInfoTwo)
    networkControllerRef ! ConnectionToPeer(activeConnections, unconfirmedConnections)

    // Assert
    tcpManagerProbe.expectMsgPF() {
      case ok@Connect(remoteAddress, _, _, _, _) if remoteAddress == peerAddressTwo => ok
      case _ => fail("Unexpected message received")
    }

    system.terminate()
  }

  it should "connect to the second peer using the local address peer feature" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()
    val tcpManagerProbe = TestProbe()

    val (networkControllerRef, peerManagerRef) = createNetworkController(settings, tcpManagerProbe)

    val peerAddressOne = new InetSocketAddress("88.77.66.55", 12345)
    val peerInfoOne = getPeerInfo(peerAddressOne)
    val peerAddressTwo = new InetSocketAddress("55.66.77.88", 11223)
    val featureSeq = Seq(LocalAddressPeerFeature(peerAddressTwo))
    val peerInfoTwo = getPeerInfo(peerAddressTwo, featureSeq = featureSeq)

    var activeConnections = Map.empty[InetSocketAddress, ConnectedPeer]
    val unconfirmedConnections = Set.empty[InetSocketAddress]

    activeConnections += peerAddressOne -> ConnectedPeer(
      ConnectionId(peerAddressOne, peerAddressOne, Incoming),
      networkControllerRef,
      1,
      Some(peerInfoOne)
    )

    // Act
    peerManagerRef ! AddOrUpdatePeer(peerInfoOne)
    peerManagerRef ! AddOrUpdatePeer(peerInfoTwo)
    networkControllerRef ! ConnectionToPeer(activeConnections, unconfirmedConnections)

    // Assert
    tcpManagerProbe.expectMsgPF() {
      case ok@Connect(remoteAddress, _, _, _, _) if remoteAddress == peerAddressTwo => ok
      case _ => fail("Unexpected message received")
    }

    system.terminate()
  }

  private def extractLocalAddrFeat(handshakeFromNode: Handshake): Option[InetSocketAddress] = {
    handshakeFromNode.peerSpec.localAddressOpt
  }

  /**
    * Create NetworkControllerActor
    */
  private def createNetworkController(settings: SparkzSettings, tcpManagerProbe: TestProbe)(implicit system: ActorSystem) = {
    val timeProvider = LocalTimeProvider
    val externalAddr = settings.network.declaredAddress

    val peersSpec: PeersSpec = new PeersSpec(featureSerializers, settings.network.maxPeerSpecObjects)
    val messageSpecs = Seq(GetPeersSpec, peersSpec)
    val sparkzContext = SparkzContext(messageSpecs, Seq.empty, timeProvider, externalAddr)

    val peerManagerRef = PeerManagerRef(settings, sparkzContext)

    val networkControllerRef: ActorRef = NetworkControllerRef(
      "networkController", settings.network,
      peerManagerRef, sparkzContext, tcpManagerProbe.testActor)

    val peerSynchronizer: ActorRef = PeerSynchronizerRef("PeerSynchronizer",
      networkControllerRef, peerManagerRef, settings.network, featureSerializers)


    tcpManagerProbe.expectMsg(Bind(networkControllerRef, settings.network.bindAddress, options = Nil))

    tcpManagerProbe.send(networkControllerRef, Bound(settings.network.bindAddress))
    (networkControllerRef, peerManagerRef)
  }
}

/**
  * Helper class that emulates peers
  */
class TestPeer(settings: SparkzSettings, networkControllerRef: ActorRef, tcpManagerProbe: TestProbe)
              (implicit ec: ExecutionContext) extends Matchers {

  private val timeProvider = LocalTimeProvider
  private val featureSerializers = Map(LocalAddressPeerFeature.featureId -> LocalAddressPeerFeatureSerializer)
  private val handshakeSerializer = new HandshakeSpec(featureSerializers, Int.MaxValue)
  private val peersSpec = new PeersSpec(featureSerializers, settings.network.maxPeerSpecObjects)
  private val messageSpecs = Seq(GetPeersSpec, peersSpec)
  private val messagesSerializer = new MessageSerializer(messageSpecs, settings.network.magicBytes, settings.network.messageLengthBytesLimit)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private var connectionHandler: ActorRef = _

  /**
    * Connect peer to node
    *
    * @param peerAddr - peer address
    * @param nodeAddr - node address
    */
  def connectAndExpectSuccessfulMessages(peerAddr: InetSocketAddress, nodeAddr: InetSocketAddress, expectedMessage: TcpMessage): Unit = {
    tcpManagerProbe.send(networkControllerRef, Connected(peerAddr, nodeAddr))

    connectionHandler = tcpManagerProbe.expectMsgPF() {
      case Tcp.Register(handler, _, _) => handler
      case Tcp.Close => tcpManagerProbe.ref
    }

    tcpManagerProbe.expectMsg(expectedMessage)
  }

  def connectAndExpectMessage(peerAddr: InetSocketAddress, nodeAddr: InetSocketAddress, expectedMessage: TcpMessage): Unit = {
    tcpManagerProbe.send(networkControllerRef, Connected(peerAddr, nodeAddr))

    tcpManagerProbe.expectMsg(expectedMessage)
  }

  /**
    * Send handshake message to node
    *
    * @param declaredAddress
    * @param localAddress
    * @return
    */
  def sendHandshake(declaredAddress: Option[InetSocketAddress], localAddress: Option[InetSocketAddress]): Tcp.ResumeReading.type = {
    val localFeature: Seq[PeerFeature] = localAddress.map(LocalAddressPeerFeature(_)).toSeq
    val features = localFeature :+ SessionIdPeerFeature(settings.network.magicBytes)
    val handshakeToNode = Handshake(PeerSpec(settings.network.agentName,
      Version(settings.network.appVersion), "test",
      declaredAddress, features), timeProvider.time())

    tcpManagerProbe.send(connectionHandler, Tcp.Received(ByteString(handshakeSerializer.toBytes(handshakeToNode))))
    tcpManagerProbe.expectMsg(Tcp.ResumeReading)
  }

  /**
    * Receive handshake message from node
    *
    * @return Success with handshake message if received valid handshake message and Fail for invalid message
    */
  def receiveHandshake: Handshake = {
    tcpManagerProbe.expectMsgPF() {
      case Tcp.Write(data, e) =>
        handshakeSerializer.parseBytes(data.toByteBuffer.array)
    }
  }

  /**
    * Send GetPeers message to node
    */
  def sendGetPeers(): Unit = {
    val msg = Message[Unit](GetPeersSpec, Right(Unit), None)
    sendMessage(msg)
  }

  /**
    * Receive GetPeer message from node
    *
    * @return
    */
  def receiveGetPeers: Message[_] = {
    val message = receiveMessage
    message.spec.messageCode should be(GetPeersSpec.messageCode)
    message
  }

  /**
    * Receive sequence of peer addresses from node
    */
  def receivePeers: Seq[PeerSpec] = {
    val message = receiveMessage
    message.spec.messageCode should be(PeersSpec.messageCode)
    peersSpec.parseBytes(message.input.left.value)
  }

  /**
    * Send sequence of peer addresses to node
    */
  def sendPeers(peers: Seq[PeerSpec]): Unit = {
    val msg = Message(peersSpec, Right(peers), None)
    sendMessage(msg)
  }

  /**
    * Send message to node
    *
    * @param msg
    */
  def sendMessage(msg: Message[_]): Unit = {
    val byteString = messagesSerializer.serialize(msg)
    tcpManagerProbe.send(connectionHandler, Tcp.Received(byteString))
    tcpManagerProbe.expectMsg(Tcp.ResumeReading)
  }

  /**
    * Receive message from node
    *
    */
  def receiveMessage: Message[_] = {
    tcpManagerProbe.expectMsgPF() {
      case Tcp.Write(b, _) =>
        messagesSerializer.deserialize(b, None).success.value.value
    }
  }

}
