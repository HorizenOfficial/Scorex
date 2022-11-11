package sparkz.core.network.peer

import sparkz.core.network.SourcePeer
import sparkz.core.network.peer.BucketManager.Exception.PeerNotFoundException
import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PeerBucketStorage._

import java.net.InetSocketAddress
import scala.util.Random

class BucketManager(private val newBucket: NewPeerBucketStorage, private val triedBucket: TriedPeerBucketStorage) {
  def addNewPeer(peerBucketValue: PeerBucketValue): Unit = {
    newBucket.add(peerBucketValue)
  }

  def makeTried(address: InetSocketAddress): Unit = {
    val peerToBeMovedInTried = newBucket
      .getStoredPeerByAddress(address)
      .getOrElse(
        throw PeerNotFoundException(s"Cannot move peer $address to tried table because it doesn't exist in new")
      )
    newBucket.remove(address)

    if (triedBucket.bucketPositionIsAlreadyTaken(peerToBeMovedInTried)) {
      val (bucket, bucketPosition) = triedBucket.getPeerIndexes(peerToBeMovedInTried)
      val peerToBeRemovedOption = triedBucket.getStoredPeerByIndexes(bucket, bucketPosition)
      val peerToBeRemoved = peerToBeRemovedOption.getOrElse(throw new IllegalArgumentException())
      val addressToBeRemoved = peerToBeRemoved.peerInfo.peerSpec.address.getOrElse(throw new IllegalArgumentException())
      triedBucket.remove(addressToBeRemoved)

      addNewPeer(peerToBeRemoved)
    }

    triedBucket.add(peerToBeMovedInTried)
  }

  def removePeer(address: InetSocketAddress): Unit = {
    newBucket.remove(address)
    triedBucket.remove(address)
  }

  def isEmpty: Boolean = triedBucket.isEmpty && newBucket.isEmpty

  def getNewPeers: Map[InetSocketAddress, PeerInfo] = newBucket.getPeers

  def getTriedPeers: Map[InetSocketAddress, PeerInfo] = triedBucket.getPeers

  def getRandomPeers: Map[InetSocketAddress, PeerInfo] = {
    val pickFromNewBucket = new Random().nextBoolean()

    if ((pickFromNewBucket && newBucket.nonEmpty) || triedBucket.isEmpty)
      newBucket.getPeers
    else
      triedBucket.getPeers
  }

  def getPeer(peerAddress: InetSocketAddress): Option[PeerBucketValue] = {
    triedBucket.getStoredPeerByAddress(peerAddress) match {
      case peer: Some[PeerBucketValue] => peer
      case _ => newBucket.getStoredPeerByAddress(peerAddress)
    }
  }
}

object BucketManager {
  case class BucketManagerConfig(newBucketConfig: BucketConfig, triedBucketConfig: BucketConfig, nKey: Int)

  case class PeerBucketValue(peerInfo: PeerInfo, source: SourcePeer, isNew: Boolean)

  case object Exception {
    case class PeerNotFoundException(msg: String) extends Exception(msg)
  }
}