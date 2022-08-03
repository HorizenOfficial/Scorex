package scorex.network

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActor, TestProbe}
import scorex.ObjectGenerators
import scorex.core.app.Version
import scorex.core.network.NetworkController.ReceivableMessages.{GetFilteredConnectedPeers, PenalizePeer, RegisterMessageSpecs, SendToNetwork}
import scorex.core.network.message.Message.MessageCode
import scorex.core.network.message.{GetPeersSpec, Message, ModifiersSpec, PeersSpec}
import scorex.core.network.peer.PeerInfo
import scorex.core.network.peer.PenaltyType.{NonDeliveryPenalty, SpamPenalty}
import scorex.core.network._
import scorex.core.settings.{NetworkSettings, ScorexSettings}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

@SuppressWarnings(Array(
  "org.wartremover.warts.Null",
  "org.wartremover.warts.TraversableOps",
  "org.wartremover.warts.OptionPartial"))
class RequestTrackerSpecification extends NetworkTests with ObjectGenerators {

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContext = actorSystem.dispatchers.lookup("scorex.executionContext")

  private val deliveryTimeout: FiniteDuration = 2.seconds
  private val requestMessageCode: MessageCode = GetPeersSpec.messageCode
  private val responseMessageCode: MessageCode = PeersSpec.messageCode
  private val connectedPeer = connectedPeerGen(null).sample.get
    .copy(peerInfo = Some(PeerInfo(PeerSpec("unknown", Version.initial, "unknown", None, Seq()), 0L, Some(Outgoing))))

  private def withTestedActors(test: (ActorRef, TestProbe, TestProbe, NetworkSettings) => Unit): Unit = {
    val (requestTracker, networkControllerProbe, peerSynchronizerProbe, networkSettings) = prepareTestData()
    test(requestTracker, networkControllerProbe, peerSynchronizerProbe, networkSettings)
  }

  "Request Tracker" should "forward registerMessageSpec to network controller, replacing ref to itself" in {
    withTestedActors { (requestTracker, networkControllerProbe, peerSynchronizerProbe, _) =>
      val registerSpecMessage = RegisterMessageSpecs(Seq(), peerSynchronizerProbe.ref)
      requestTracker ! registerSpecMessage
      networkControllerProbe.expectMsg(RegisterMessageSpecs(Seq(), requestTracker))
    }
  }

  it should "use actor specified in registerMessageSpec for forwarding" in {
    withTestedActors { (requestTracker, networkControllerProbe, peerSynchronizerProbe, _) =>
      val registerSpecMessage = RegisterMessageSpecs(Seq(), peerSynchronizerProbe.ref)
      requestTracker ! registerSpecMessage
      val messageToBeForwarded = Message(new ModifiersSpec(1), Left(null), None)
      networkControllerProbe.expectMsg(RegisterMessageSpecs(Seq(), requestTracker))

      requestTracker tell (messageToBeForwarded, networkControllerProbe.ref)
      peerSynchronizerProbe.expectMsg(messageToBeForwarded)

      requestTracker ! messageToBeForwarded
      networkControllerProbe.expectMsg(messageToBeForwarded)
    }
  }

  it should "penalize peer for not delivering response" in {
    withTestedActors { (requestTracker, networkControllerProbe, peerSynchronizerProbe, _) =>
      val msg = Message(GetPeersSpec, Left(null), None)
      val request = SendToNetwork(msg, SendToRandom)

      //register
      val registerSpecMessage = RegisterMessageSpecs(Seq(), peerSynchronizerProbe.ref)
      requestTracker ! registerSpecMessage
      networkControllerProbe.expectMsg(RegisterMessageSpecs(Seq(), requestTracker))

      //request
      networkControllerProbe.setAutoPilot(
        (sender: ActorRef, msg: Any) => {
          msg match {
            case GetFilteredConnectedPeers(_,_) => sender ! Seq(connectedPeer)
            case _ =>
          }
          TestActor.KeepRunning
        }
      )
      requestTracker ! request
      networkControllerProbe.expectMsg(GetFilteredConnectedPeers(SendToRandom, GetPeersSpec.protocolVersion))
      networkControllerProbe.expectMsg(request.copy(sendingStrategy = SendToPeer(connectedPeer)))

      //penalty
      networkControllerProbe.expectMsg(deliveryTimeout + 1.seconds, PenalizePeer(connectedPeer.connectionId.remoteAddress, NonDeliveryPenalty))
    }
  }


  it should "not penalize when response is received" in {
    withTestedActors { (requestTracker, networkControllerProbe, peerSynchronizerProbe, _) =>
      val msg = Message(GetPeersSpec, Left(null), None)
      val request = SendToNetwork(msg, SendToRandom)
      val response = Message(new PeersSpec(Map(), 1), Left(null), Some(connectedPeer))

      //register
      val registerSpecMessage = RegisterMessageSpecs(Seq(), peerSynchronizerProbe.ref)
      requestTracker ! registerSpecMessage
      networkControllerProbe.expectMsg(RegisterMessageSpecs(Seq(), requestTracker))

      //request
      networkControllerProbe.setAutoPilot(
        (sender: ActorRef, msg: Any) => {
          msg match {
            case GetFilteredConnectedPeers(_,_) => sender ! Seq(connectedPeer)
            case _ =>
          }
          TestActor.KeepRunning
        }
      )
      requestTracker ! request
      networkControllerProbe.expectMsg(GetFilteredConnectedPeers(SendToRandom, GetPeersSpec.protocolVersion))
      networkControllerProbe.expectMsg(request.copy(sendingStrategy = SendToPeer(connectedPeer)))

      //response
      requestTracker ! response
      peerSynchronizerProbe.expectMsg(response)

      //no penalty
      networkControllerProbe.expectNoMessage(deliveryTimeout + 1.seconds)
    }
  }

  it should "penalize peer for spamming" in {
    withTestedActors { (requestTracker, networkControllerProbe, peerSynchronizerProbe, _) =>
      val response = Message(new PeersSpec(Map(), 1), Left(null), Some(connectedPeer))

      //register
      val registerSpecMessage = RegisterMessageSpecs(Seq(), peerSynchronizerProbe.ref)
      requestTracker ! registerSpecMessage
      networkControllerProbe.expectMsg(RegisterMessageSpecs(Seq(), requestTracker))

      //response without request
      requestTracker ! response

      //spam penalty
      networkControllerProbe.expectMsg(PenalizePeer(connectedPeer.connectionId.remoteAddress, SpamPenalty))
      peerSynchronizerProbe.expectNoMessage
    }
  }

  private def prepareTestData(): (ActorRef, TestProbe, TestProbe, NetworkSettings) = {
    val networkControllerProbe = TestProbe()
    val peerSynchronizerProbe = TestProbe()
    val networkSettings = ScorexSettings.read(None).network.copy(
      penalizeNonDelivery = true
    )
    val requestTracker = RequestTrackerRef(networkControllerProbe.ref, requestMessageCode, responseMessageCode, deliveryTimeout, networkSettings.penalizeNonDelivery)

    (requestTracker, networkControllerProbe, peerSynchronizerProbe, networkSettings)
  }

}
