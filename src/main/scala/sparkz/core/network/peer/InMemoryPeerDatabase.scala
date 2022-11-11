package sparkz.core.network.peer

import scorex.util.ScorexLogging
import sparkz.core.network.ConnectedPeer
import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PenaltyType.DisconnectPenalty
import sparkz.core.settings.NetworkSettings
import sparkz.core.utils.TimeProvider

import java.net.{InetAddress, InetSocketAddress}
import scala.collection.mutable
import scala.concurrent.duration._

/**
  * In-memory peer database implementation supporting temporal blacklisting.
  */
final class InMemoryPeerDatabase(
                                  settings: NetworkSettings,
                                  timeProvider: TimeProvider,
                                  bucketManager: BucketManager,
                                  blacklist: mutable.Map[InetAddress, TimeProvider.Time],
                                  penaltyBook: mutable.Map[InetAddress, (Int, Long)]
                                )
  extends PeerDatabase with ScorexLogging {

  private val safeInterval = settings.penaltySafeInterval.toMillis

  override def get(peer: InetSocketAddress): Option[PeerInfo] = bucketManager.getPeer(peer) match {
    case Some(peerBucketValue) => Some(peerBucketValue.peerInfo)
    case _ => None
  }

  /**
   * Adds a peer to the in-memory database ignoring the configurable limit.
   * Used for high-priority peers, like peers from config file or connected peers
   */
  override def addOrUpdateKnownPeer(peerInfo: PeerInfo, source: Option[ConnectedPeer]): Unit = {
    if (!peerInfo.peerSpec.declaredAddress.exists(x => isBlacklisted(x.getAddress))) {
      source match {
        case Some(source) => bucketManager.addNewPeer(PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true))
        case None => bucketManager.makeTried(peerInfo.peerSpec.address.getOrElse(throw new IllegalArgumentException()))
      }
    }
  }

  override def addOrUpdateKnownPeers(peersInfo: Seq[PeerInfo], source: Option[ConnectedPeer]): Unit = {
    val validPeers = peersInfo.filterNot {_.peerSpec.declaredAddress.exists(x => isBlacklisted(x.getAddress))}
    validPeers.foreach(peer => addOrUpdateKnownPeer(peer, source))
  }

  override def addToBlacklist(socketAddress: InetSocketAddress,
                              penaltyType: PenaltyType): Unit = {
    remove(socketAddress)
    Option(socketAddress.getAddress).foreach { address =>
      penaltyBook -= address
      if (!blacklist.keySet.contains(address))
        blacklist += address -> (timeProvider.time() + penaltyDuration(penaltyType))
      else log.warn(s"${address.toString} is already blacklisted")
    }
  }

  override def removeFromBlacklist(address: InetAddress): Unit = {
    log.info(s"$address removed from blacklist")
    blacklist -= address
  }

  override def remove(address: InetSocketAddress): Unit = {
    bucketManager.removePeer(address)
  }

  override def allPeers: Map[InetSocketAddress, PeerInfo] = bucketManager.getTriedPeers ++ bucketManager.getNewPeers

  override def blacklistedPeers: Seq[InetAddress] = blacklist
    .map { case (address, bannedTill) =>
      checkBanned(address, bannedTill)
      address
    }
    .toSeq

  override def isEmpty: Boolean = bucketManager.isEmpty

  override def isBlacklisted(address: InetAddress): Boolean =
    blacklist.get(address).exists(checkBanned(address, _))

  def isBlacklisted(address: InetSocketAddress): Boolean =
    Option(address.getAddress).exists(isBlacklisted)

  /**
    * Registers a new penalty in the penalty book.
    *
    * @return - `true` if penalty threshold is reached, `false` otherwise.
    */
  override def peerPenaltyScoreOverThreshold(socketAddress: InetSocketAddress, penaltyType: PenaltyType): Boolean =
    Option(socketAddress.getAddress).exists { address =>
      val (newPenaltyScore, penaltyTs) = penaltyBook.get(address) match {
        case Some((penaltyScoreAcc, lastPenaltyTs)) =>
          val currentTime = timeProvider.time()
          if (currentTime - lastPenaltyTs - safeInterval > 0 || penaltyType.isPermanent)
            (penaltyScoreAcc + penaltyType.penaltyScore, timeProvider.time())
          else
            (penaltyScoreAcc, lastPenaltyTs)
        case None =>
          (penaltyType.penaltyScore, timeProvider.time())
      }
      if (newPenaltyScore > settings.penaltyScoreThreshold)
        true
      else {
        penaltyBook += address -> (newPenaltyScore -> penaltyTs)
        false
      }
    }

  /**
    * Currently accumulated penalty score for a given address.
    */
  def penaltyScore(address: InetAddress): Int =
    penaltyBook.getOrElse(address, (0, 0L))._1

  def penaltyScore(socketAddress: InetSocketAddress): Int =
    Option(socketAddress.getAddress).map(penaltyScore).getOrElse(0)

  private def checkBanned(address: InetAddress, bannedTill: Long): Boolean = {
    val stillBanned = timeProvider.time() < bannedTill
    if (!stillBanned) removeFromBlacklist(address)
    stillBanned
  }

  private def penaltyDuration(penalty: PenaltyType): Long =
    penalty match {
      case PenaltyType.NonDeliveryPenalty | PenaltyType.MisbehaviorPenalty | PenaltyType.SpamPenalty | _: DisconnectPenalty =>
        settings.temporalBanDuration.toMillis
      case PenaltyType.PermanentPenalty =>
        (360 * 10).days.toMillis
    }

  override def randomPeersSubset: Map[InetSocketAddress, PeerInfo] = bucketManager.getRandomPeers
}
