package sparkz.core.network

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import sparkz.core.network.NetworkController.ReceivableMessages.{PenalizePeer, RegisterMessageSpecs, SendToNetwork}
import sparkz.core.network.message.{GetPeersSpec, Message, MessageSpec, PeersSpec}
import sparkz.core.network.peer.{PeerInfo, PenaltyType}
import sparkz.core.network.peer.PeerManager.ReceivableMessages.{AddPeersIfEmpty, SeenPeers}
import sparkz.core.settings.NetworkSettings
import scorex.util.ScorexLogging
import shapeless.syntax.typeable._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Responsible for discovering and sharing new peers.
  */
class PeerSynchronizer(val networkControllerRef: ActorRef,
                       peerManager: ActorRef,
                       settings: NetworkSettings,
                       featureSerializers: PeerFeature.Serializers)
                      (implicit ec: ExecutionContext) extends Actor with Synchronizer with ScorexLogging {

  private val peersSpec = new PeersSpec(featureSerializers, settings.maxPeerSpecObjects)

  protected override val msgHandlers: PartialFunction[(MessageSpec[_], _, ConnectedPeer), Unit] = {
    case (_: PeersSpec, peers: Seq[PeerSpec]@unchecked, sourcePeer: ConnectedPeer) if peers.cast[Seq[PeerSpec]].isDefined =>
      addNewPeers(peers, sourcePeer)

    case (spec, _, remote) if spec.messageCode == GetPeersSpec.messageCode =>
      gossipPeers(remote)
  }

  override def preStart: Unit = {
    super.preStart()

    networkControllerRef ! RegisterMessageSpecs(Seq(GetPeersSpec, peersSpec), self)

    val msg = Message[Unit](GetPeersSpec, Right(Unit), None)
    val stn = SendToNetwork(msg, SendToRandom)
    context.system.scheduler.scheduleWithFixedDelay(2.seconds, settings.getPeersInterval, networkControllerRef, stn)
  }

  override def receive: Receive = {

    // data received from a remote peer
    case Message(spec, Left(msgBytes), Some(source)) => parseAndHandle(spec, msgBytes, source)

    // fall-through method for reporting unhandled messages
    case nonsense: Any => log.warn(s"PeerSynchronizer: got unexpected input $nonsense from ${sender()}")
  }

  override protected def penalizeMaliciousPeer(peer: ConnectedPeer): Unit = {
    networkControllerRef ! PenalizePeer(peer.connectionId.remoteAddress, PenaltyType.PermanentPenalty)
  }

  /**
    * Handles adding new peers to the peer database if they were previously unknown
    *
    * @param peers sequence of peer specs describing a remote peers details
    */
  private def addNewPeers(peers: Seq[PeerSpec], sourcePeer: ConnectedPeer): Unit = {
    peerManager ! AddPeersIfEmpty(peers, sourcePeer)
  }

  /**
    * Handles gossiping about the locally known peer set to a given remote peer
    *
    * @param remote the remote peer to be informed of our local peers
    */
  private def gossipPeers(remote: ConnectedPeer): Unit = {
    implicit val timeout: Timeout = Timeout(settings.syncTimeout.getOrElse(5.seconds))

    (peerManager ? SeenPeers(settings.maxPeerSpecObjects))
      .mapTo[Seq[PeerInfo]]
      .foreach { peers =>
        val msg = Message(peersSpec, Right(peers.map(_.peerSpec)), None)
        networkControllerRef ! SendToNetwork(msg, SendToPeer(remote))
      }
  }
}

object PeerSynchronizerRef {
  def props(networkControllerRef: ActorRef, peerManager: ActorRef, settings: NetworkSettings,
            featureSerializers: PeerFeature.Serializers)(implicit system: ActorSystem, ec: ExecutionContext): Props = {
    val wrappedNetworkController = RequestTrackerRef(
      networkControllerRef,
      GetPeersSpec.messageCode,
      PeersSpec.messageCode,
      settings.getPeersInterval / 2,
      settings.penalizeNonDelivery
    )
    Props(new PeerSynchronizer(wrappedNetworkController, peerManager, settings, featureSerializers))
  }

  def apply(networkControllerRef: ActorRef, peerManager: ActorRef, settings: NetworkSettings,
            featureSerializers: PeerFeature.Serializers)(implicit system: ActorSystem, ec: ExecutionContext): ActorRef =
    system.actorOf(props(networkControllerRef, peerManager, settings, featureSerializers))

  def apply(name: String, networkControllerRef: ActorRef, peerManager: ActorRef, settings: NetworkSettings,
            featureSerializers: PeerFeature.Serializers)(implicit system: ActorSystem, ec: ExecutionContext): ActorRef =
    system.actorOf(props(networkControllerRef, peerManager, settings, featureSerializers), name)
}