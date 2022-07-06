package scorex.core.network

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import scorex.core.network.NetworkController.ReceivableMessages.{PenalizePeer, RegisterMessageSpecs, SendToNetwork}
import scorex.core.network.PeerSynchronizer.ReceivableMessages.{GetNewPeers, LookupResponse}
import scorex.core.network.dns.DnsClient.ReceivableMessages.LookupRequest
import scorex.core.network.dns.strategy.Strategy.LeastNodeQuantity
import scorex.core.network.message.{GetPeersSpec, Message, MessageSpec, PeersSpec}
import scorex.core.network.peer.PeerManager.ReceivableMessages.{AddOrUpdatePeer, AddPeerIfEmpty, SeenPeers}
import scorex.core.network.peer.{PeerInfo, PenaltyType}
import scorex.core.settings.NetworkSettings
import scorex.util.ScorexLogging
import shapeless.syntax.typeable._

import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Responsible for discovering and sharing new peers.
  */
class PeerSynchronizer(val networkControllerRef: ActorRef,
                       peerManager: ActorRef,
                       dnsClientRef: ActorRef,
                       settings: NetworkSettings,
                       featureSerializers: PeerFeature.Serializers)
                      (implicit ec: ExecutionContext) extends Actor with Synchronizer with ScorexLogging {

  private val peersSpec = new PeersSpec(featureSerializers, settings.maxPeerSpecObjects)

  protected override val msgHandlers: PartialFunction[(MessageSpec[_], _, ConnectedPeer), Unit] = {
    case (_: PeersSpec, peers: Seq[PeerSpec]@unchecked, _) if peers.cast[Seq[PeerSpec]].isDefined =>
      addNewPeers(peers)

    case (spec, _, remote) if spec.messageCode == GetPeersSpec.messageCode =>
      gossipPeers(remote)
  }

  override def preStart: Unit = {
    super.preStart()

    networkControllerRef ! RegisterMessageSpecs(Seq(GetPeersSpec, peersSpec), self)

    context.system.eventStream.subscribe(self, classOf[GetNewPeers])

    val msg = Message[Unit](GetPeersSpec, Right(Unit), None)
    val stn = SendToNetwork(msg, SendToRandom)
    context.system.scheduler.scheduleWithFixedDelay(2.seconds, settings.getPeersInterval, networkControllerRef, stn)

    performFirstDnsSeedersLookup()
  }

  private def performFirstDnsSeedersLookup(): Unit = {
    self ! GetNewPeers()
  }

  override def receive: Receive = {

    // data received from a remote peer
    case Message(spec, Left(msgBytes), Some(source)) => parseAndHandle(spec, msgBytes, source)

    // TODO: it could be a good idea to pass the peers left in the db or some other criteria
    // TODO: to decide the lookup strategy
    case GetNewPeers() =>
      dnsClientRef ! LookupRequest(LeastNodeQuantity())

    case LookupResponse(ipv4Addresses, _) =>
      val defaultPort = settings.bindAddress.getPort
      ipv4Addresses.foreach(
        address => peerManager ! AddOrUpdatePeer(PeerInfo.fromAddress(new InetSocketAddress(address, defaultPort)))
      )

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
  private def addNewPeers(peers: Seq[PeerSpec]): Unit = {
    peers.foreach(peerSpec => peerManager ! AddPeerIfEmpty(peerSpec))
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

object PeerSynchronizer {
  object ReceivableMessages {
    case class GetNewPeers()

    case class LookupResponse(ipv4Addresses: Seq[InetAddress], ipv6Addresses: Seq[InetAddress])
  }
}

object PeerSynchronizerRef {
  def props(networkControllerRef: ActorRef, peerManager: ActorRef, dnsClientRef: ActorRef, settings: NetworkSettings,
            featureSerializers: PeerFeature.Serializers)(implicit ec: ExecutionContext): Props =
    Props(new PeerSynchronizer(networkControllerRef, peerManager, dnsClientRef, settings, featureSerializers))

  def apply(networkControllerRef: ActorRef, peerManager: ActorRef, dnsClientRef: ActorRef, settings: NetworkSettings,
            featureSerializers: PeerFeature.Serializers)(implicit system: ActorSystem, ec: ExecutionContext): ActorRef =
    system.actorOf(props(networkControllerRef, peerManager, dnsClientRef, settings, featureSerializers))

  def apply(name: String, networkControllerRef: ActorRef, peerManager: ActorRef, dnsClientRef: ActorRef, settings: NetworkSettings,
            featureSerializers: PeerFeature.Serializers)(implicit system: ActorSystem, ec: ExecutionContext): ActorRef =
    system.actorOf(props(networkControllerRef, peerManager, dnsClientRef, settings, featureSerializers), name)
}