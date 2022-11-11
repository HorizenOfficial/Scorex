package sparkz.core.network.peer

import com.google.common.primitives.{Bytes, Ints}
import scorex.crypto.hash.Blake2b256
import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PeerBucketStorage.{BucketConfig, BucketHashContent, NOT_FOUND_PEER_INDEXES}
import sparkz.core.storage.ScheduledStoragePersister.Persistable
import sparkz.core.utils.TimeProvider.Time
import sparkz.core.utils.{NetworkAddressWrapper, TimeProvider}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.{Success, Try}

abstract class PeerBucketStorage[T <: BucketHashContent](
                                  private val bucketConfig: BucketConfig,
                                  private val nKey: Int,
                                  private val timeProvider: TimeProvider,
                                  private val isNewPeer: Boolean
                                ) {
  private val (buckets, positionsInBucket, bucketSubgroup) = BucketConfig.unapply(bucketConfig).getOrElse(throw new IllegalArgumentException())
  private val table: mutable.Map[(Int, Int), (PeerBucketValue, Time)] = mutable.Map.empty[(Int, Int), (PeerBucketValue, Time)]
  private val reverseTable: mutable.Map[InetSocketAddress, (Int, Int)] = mutable.Map.empty[InetSocketAddress, (Int, Int)]

  protected[peer] def getBucket(hashContent: T): Int = {
    val hash1Content = Bytes.concat(Ints.toByteArray(nKey), hashContent.getHashAsBytes)
    val h: Array[Byte] = Blake2b256.hash(hash1Content)
    val hInt = ByteBuffer.wrap(h).getInt

    val hash2Content = Bytes.concat(Ints.toByteArray(nKey), getAddressGroup(hashContent.getAddress), Ints.toByteArray(hInt % bucketSubgroup))
    val hash2: Array[Byte] = Blake2b256.hash(hash2Content)

    ByteBuffer.wrap(hash2).getInt % buckets
  }

  private def getAddressGroup(peerAddress: InetSocketAddress): Array[Byte] = {
    val na = new NetworkAddressWrapper(peerAddress)
    var nClass = NetworkAddressWrapper.NET_IPV6
    var nStartByte = 0
    var nBits = 16

    if (na.isLocal) {
      nClass = 255
      nBits = 0
    } else if (!na.isRoutable) {
      nClass = NetworkAddressWrapper.NET_UNROUTABLE
      nBits = 0
    } else if (na.hasLinkedIPv4) {
      nClass = NetworkAddressWrapper.NET_IPV4
    } else if (na.isHeNet) {
      nBits = 36
    }
    else
      nBits = 32

    var result: Array[Byte] = Array(nClass.toByte)
    while (nBits >= 16) {
      result = result ++ na.getSubNum(nStartByte)
      nStartByte += 1
      nBits -= 16
    }
    result
  }

  protected[peer] def getBucketPosition(nBucket: Int, peerAddress: InetSocketAddress): Int = {
    val hashContent = Bytes.concat(
      Ints.toByteArray(nKey),
      Ints.toByteArray(if (isNewPeer) 1 else 0),
      Ints.toByteArray(nBucket),
      peerAddress.getHostString.getBytes,
      Ints.toByteArray(peerAddress.getPort)
    )
    val hash1 = Blake2b256.hash(hashContent)
    ByteBuffer.wrap(hash1).getInt % positionsInBucket
  }

  def add(peerBucketValue: PeerBucketValue): Unit = {
    peerBucketValue.peerInfo.peerSpec.address.foreach(address => {
      val hashContent = getHashContent(peerBucketValue)
      val (bucket, bucketPosition) = getPeerIndexes(hashContent)
      table += (bucket, bucketPosition) -> (peerBucketValue, timeProvider.time())
      reverseTable += address -> (bucket, bucketPosition)
    })
  }

  protected def getHashContent(peerBucketValue: PeerBucketValue): T

  def remove(address: InetSocketAddress): Unit = {
    val peerIndexes = tryGetIndexesFromPeerAddress(address)

    if (peerIndexes != NOT_FOUND_PEER_INDEXES) {
      table -= ((peerIndexes._1, peerIndexes._2))
      reverseTable -= address
    }
  }

  def bucketPositionIsAlreadyTaken(peerBucketValue: PeerBucketValue): Boolean = {
    val hashContent = getHashContent(peerBucketValue)
    val (bucket, bucketPosition) = getPeerIndexes(hashContent)

    table.contains((bucket, bucketPosition))
  }

  def contains(address: InetSocketAddress): Boolean = {
    tryGetIndexesFromPeerAddress(address) != NOT_FOUND_PEER_INDEXES
  }

  def getStoredPeerByAddress(address: InetSocketAddress): Option[PeerBucketValue] = {
    tryGetIndexesFromPeerAddress(address) match {
      case indexes if indexes != NOT_FOUND_PEER_INDEXES => Some(table(indexes._1, indexes._2)._1)
      case _ => None
    }
  }

  def getStoredPeerByIndexes(bucket: Int, bucketPosition: Int): Option[PeerBucketValue] = {
    Try(table(bucket, bucketPosition)) match {
      case Success((peerBucketValue, _)) => Some(peerBucketValue)
      case _ => None
    }
  }

  def getPeerIndexes(peerBucketValue: PeerBucketValue): (Int, Int) = {
    val peerHashContent = getHashContent(peerBucketValue)
    getPeerIndexes(peerHashContent)
  }

  def getPeerIndexes(peerHashContent: T): (Int, Int) = {
    val bucket = getBucket(peerHashContent)
    val bucketPosition = getBucketPosition(bucket, peerHashContent.getAddress)
    (bucket, bucketPosition)
  }

  private def tryGetIndexesFromPeerAddress(address: InetSocketAddress): (Int, Int) = {
    Try(reverseTable(address)) match {
      case Success(indexes) => indexes
      case _ => NOT_FOUND_PEER_INDEXES
    }
  }

  def isEmpty: Boolean = table.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def getPeers: Map[InetSocketAddress, PeerInfo] =
    table
      .values
      .map { p => (p._1.peerInfo.peerSpec.address.getOrElse(throw new IllegalArgumentException()), p._1.peerInfo) }
      .toMap

  def getPeersAsBucketValue: Seq[PeerBucketValue] = table.values.map(value => value._1).toSeq

  def clear(): Unit = {
    table.clear()
    reverseTable.clear()
  }
}

object PeerBucketStorage {
  val NOT_FOUND_PEER_INDEXES: (Int, Int) = (-1, -1)
  case class BucketConfig(buckets: Int, bucketPositions: Int, bucketSubgroups: Int)

  trait BucketHashContent {
    def getAddress: InetSocketAddress

    def getHashAsBytes: Array[Byte]
  }

  case class NewPeerBucketHashContent(address: InetSocketAddress, sourceAddress: InetSocketAddress) extends BucketHashContent {
    override def getHashAsBytes: Array[Byte] = Bytes.concat(
      address.getHostString.getBytes,
      Ints.toByteArray(address.getPort),
      sourceAddress.getHostString.getBytes,
      Ints.toByteArray(sourceAddress.getPort)
    )

    override def getAddress: InetSocketAddress = address
  }

  case class TriedPeerBucketHashContent(address: InetSocketAddress) extends BucketHashContent {

    override def getHashAsBytes: Array[Byte] = Bytes.concat(address.getHostString.getBytes, Ints.toByteArray(address.getPort))

    override def getAddress: InetSocketAddress = address
  }

  case class NewPeerBucketStorage(bucketConfig: BucketConfig, nKey: Int, timeProvider: TimeProvider)
    extends PeerBucketStorage[NewPeerBucketHashContent](bucketConfig, nKey, timeProvider, true)
    with Persistable {

    override protected def getHashContent(peerBucketValue: PeerBucketValue): NewPeerBucketHashContent = {
      val address = peerBucketValue.peerInfo.peerSpec.address.getOrElse(throw new IllegalArgumentException())
      NewPeerBucketHashContent(address, peerBucketValue.source.connectionId.remoteAddress)
    }

    override val STORAGE_LABEL: String = "NewBucketPeers.dat"
  }

  case class TriedPeerBucketStorage(bucketConfig: BucketConfig, nKey: Int, timeProvider: TimeProvider)
    extends PeerBucketStorage[TriedPeerBucketHashContent](bucketConfig, nKey, timeProvider, false)
    with Persistable {

    override protected def getHashContent(peerBucketValue: PeerBucketValue): TriedPeerBucketHashContent = {
      val address = peerBucketValue.peerInfo.peerSpec.address.getOrElse(throw new IllegalArgumentException())
      TriedPeerBucketHashContent(address)
    }

    override val STORAGE_LABEL: String = "TriedBucketPeers.dat"
  }
}