package sparkz.core.network.peer

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{doReturn, spy}
import sparkz.core.network.peer.BucketManager.Exception.PeerNotFoundException
import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PeerBucketStorage.{BucketConfig, NewPeerBucketStorage, TriedPeerBucketStorage}
import sparkz.core.network.{ConnectedPeer, ConnectionId, Incoming, NetworkTests}

import java.net.InetSocketAddress

class BucketManagerTest extends NetworkTests {
  private val bucketConf = BucketConfig(256, 64, 8)
  private val nKey = 1234
  private val newBucket = NewPeerBucketStorage(bucketConf, nKey, timeProvider)
  private val triedBucket = TriedPeerBucketStorage(bucketConf, nKey, timeProvider)

  "BucketManager" should "be empty when created" in {
    // Arrange
    val bucketManager = new BucketManager(newBucket, triedBucket)

    // Assert
    bucketManager.isEmpty shouldBe true
    bucketManager.getTriedPeers shouldBe Map.empty
  }

  it should "persist a new peer added and being able to retrieve it" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val peerBucketValue = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val bucketManager = new BucketManager(newBucket, triedBucket)

    // Act
    bucketManager.addNewPeer(peerBucketValue)

    // Assert
    bucketManager.isEmpty shouldBe false

    val retrievedPeer = bucketManager.getPeer(peerAddress)
    retrievedPeer match {
      case Some(storedPeer) => peerBucketValue shouldBe storedPeer
      case _ => fail("")
    }

    system.terminate()
  }

  it should "move the peer when established a connection to it" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val peerBucketValue = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val bucketManager = new BucketManager(newBucket, triedBucket)
    bucketManager.addNewPeer(peerBucketValue)

    // Act
    bucketManager.makeTried(peerAddress)

    // Assert
    bucketManager.isEmpty shouldBe false

    val retrievedTriedPeer = bucketManager.getTriedPeers(peerAddress)
    retrievedTriedPeer shouldBe peerInfo
    val retrievedNewPeer = bucketManager.getNewPeers
    retrievedNewPeer.isEmpty shouldBe true

    system.terminate()
  }

  it should "re-add a peer's address into new" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val peerBucketValue = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val bucketManager = new BucketManager(newBucket, triedBucket)
    bucketManager.addNewPeer(peerBucketValue)
    bucketManager.makeTried(peerAddress)

    // Act
    bucketManager.addNewPeer(peerBucketValue)

    // Assert
    bucketManager.isEmpty shouldBe false

    val triedPeers = bucketManager.getTriedPeers
    val newPeers = bucketManager.getNewPeers
    triedPeers.size shouldBe 1
    newPeers.size shouldBe 1
    triedPeers.contains(peerAddress) shouldBe true
    newPeers.contains(peerAddress) shouldBe true

    system.terminate()
  }

  it should "remove all occurrences of the same peer's address" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val peerBucketValue = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val bucketManager = new BucketManager(newBucket, triedBucket)
    bucketManager.addNewPeer(peerBucketValue)
    bucketManager.makeTried(peerAddress)
    bucketManager.addNewPeer(peerBucketValue)

    // Act
    bucketManager.removePeer(peerAddress)

    // Assert
    bucketManager.isEmpty shouldBe true

    system.terminate()
  }

  it should "raise a PeerNotFoundException if makeTried method is called but the peer does not exist in the new table" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val bucketManager = new BucketManager(newBucket, triedBucket)

    // Act
    val exception = intercept[PeerNotFoundException] {
      bucketManager.makeTried(peerAddress)
    }

    // Assert
    exception.getMessage shouldBe s"Cannot move peer $peerAddress to tried table because it doesn't exist in new"

    system.terminate()
  }

  it should "correctly move peers from tried back to new without deleting them" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val bucket = 1
    val bucketPosition = 1
    val bucketConfig = BucketConfig(10, 10, 10)
    val newBucketMock = spy(NewPeerBucketStorage(bucketConfig, nKey, timeProvider))
    val triedBucketMock = spy(TriedPeerBucketStorage(bucketConfig, nKey, timeProvider))

    doReturn(bucket).when(triedBucketMock).getBucket(any())
    doReturn(bucketPosition).when(triedBucketMock).getBucketPosition(any(), any())

    val peerAddress1 = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo1 = getPeerInfo(peerAddress1)
    val sourcePeer = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val peer1 = PeerBucketValue(peerInfo1, sourcePeer.toSourcePeer, isNew = true)

    val peerAddress2 = new InetSocketAddress("88.77.66.55", 1234)
    val peerInfo2 = getPeerInfo(peerAddress2)
    val peer2 = PeerBucketValue(peerInfo2, sourcePeer.toSourcePeer, isNew = true)

    val bucketManager = new BucketManager(newBucketMock, triedBucketMock)

    // Act
    bucketManager.addNewPeer(peer1)
    bucketManager.addNewPeer(peer2)
    bucketManager.makeTried(peerAddress1)
    bucketManager.makeTried(peerAddress2)

    // Assert
    bucketManager.isEmpty shouldBe false

    val newPeers = bucketManager.getNewPeers
    val triedPeers = bucketManager.getTriedPeers

    newPeers shouldBe Map(peerAddress1 -> peerInfo1)
    triedPeers shouldBe Map(peerAddress2 -> peerInfo2)

    system.terminate()
  }
}
