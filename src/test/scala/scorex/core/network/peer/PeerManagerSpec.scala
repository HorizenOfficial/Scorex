package scorex.core.network.peer

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import scorex.core.app.ScorexContext
import scorex.core.network.NetworkController.ReceivableMessages.EmptyPeerDatabase
import scorex.core.network.peer.PeerManager.ReceivableMessages.RemovePeer
import scorex.network.NetworkTests

import java.net.InetSocketAddress
import scala.language.postfixOps

class PeerManagerSpec extends NetworkTests {

  import scorex.core.network.peer.PeerManager.ReceivableMessages.{AddOrUpdatePeer, GetAllPeers}

  type Data = Map[InetSocketAddress, PeerInfo]
  private val DefaultPort = 27017

  it should "ignore adding self as a peer" in {
    implicit val system: ActorSystem = ActorSystem()
    val p = TestProbe("p")(system)
    implicit val defaultSender: ActorRef = p.testActor


    val selfAddress = settings.network.bindAddress
    val scorexContext = ScorexContext(Seq.empty, Seq.empty, None, timeProvider, Some(selfAddress))
    val peerManager = PeerManagerRef(settings, scorexContext)(system)
    val peerInfo = getPeerInfo(selfAddress)

    peerManager ! AddOrUpdatePeer(peerInfo)
    peerManager ! GetAllPeers
    val data = p.expectMsgClass(classOf[Data])

    data.keySet should not contain selfAddress
    system.terminate()
  }

  it should "added peer be returned in GetAllPeers" in {
    implicit val system: ActorSystem = ActorSystem()
    val p = TestProbe("p")(system)
    implicit val defaultSender: ActorRef = p.testActor

    val scorexContext = ScorexContext(Seq.empty, Seq.empty, None, timeProvider, None)
    val peerManager = PeerManagerRef(settings, scorexContext)(system)
    val peerAddress = new InetSocketAddress("1.1.1.1", DefaultPort)
    val peerInfo = getPeerInfo(peerAddress)

    peerManager ! AddOrUpdatePeer(peerInfo)
    peerManager ! GetAllPeers

    val data = p.expectMsgClass(classOf[Data])
    data.keySet should contain(peerAddress)
    system.terminate()
  }

  it should "send an EmptyPeerDatabase message" in {
    implicit val system: ActorSystem = ActorSystem()
    val p = TestProbe("p")(system)
    implicit val defaultSender: ActorRef = p.testActor

    val scorexContext = ScorexContext(Seq.empty, Seq.empty, None, timeProvider, None)
    val peerManager = PeerManagerRef(settings, scorexContext)(system)
    val peerAddress = new InetSocketAddress("1.1.1.1", DefaultPort)
    val peerInfo = getPeerInfo(peerAddress)

    peerManager ! AddOrUpdatePeer(peerInfo)
    peerInfo.peerSpec.address.foreach(address => peerManager ! RemovePeer(address))

    p.expectMsg(EmptyPeerDatabase)
    system.terminate()
  }

  it should "not send an EmptyPeerDatabase message if peer database not yet empty" in {
    implicit val system: ActorSystem = ActorSystem()
    val p = TestProbe("p")(system)
    implicit val defaultSender: ActorRef = p.testActor

    val scorexContext = ScorexContext(Seq.empty, Seq.empty, None, timeProvider, None)
    val peerManager = PeerManagerRef(settings, scorexContext)(system)

    val peerAddressOne = new InetSocketAddress("1.1.1.1", DefaultPort)
    val peerInfoOne = getPeerInfo(peerAddressOne)
    val peerAddressTwo = new InetSocketAddress("1.1.1.2", DefaultPort)
    val peerInfoTwo = getPeerInfo(peerAddressTwo)

    peerManager ! AddOrUpdatePeer(peerInfoOne)
    peerManager ! AddOrUpdatePeer(peerInfoTwo)
    peerInfoOne.peerSpec.address.foreach(address => peerManager ! RemovePeer(address))

    p.expectNoMessage()
    system.terminate()
  }

}
