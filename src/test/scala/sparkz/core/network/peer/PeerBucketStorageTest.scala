package sparkz.core.network.peer

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PeerBucketStorage.{BucketConfig, NewPeerBucketStorage, TriedPeerBucketStorage}
import sparkz.core.network.{ConnectedPeer, ConnectionId, Incoming, NetworkTests}

import java.net.InetSocketAddress

class PeerBucketStorageTest extends NetworkTests {
  private val buckets = 256
  private val bucketPositions = 64
  private val bucketSubgroups = 8
  private val nKey = 1234

  "Both new and tried buckets" should "should be empty when created" in {
    // Arrange
    val bucketConfig = BucketConfig(buckets, bucketPositions, bucketSubgroups)
    val tried = TriedPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val newB = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)

    // Assert
    tried.isEmpty shouldBe true
    newB.isEmpty shouldBe true

    tried.getPeers shouldBe Map.empty
    newB.getPeers shouldBe Map.empty
  }

  they should "persist the peer when one is added" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val bucketConfig = BucketConfig(buckets, bucketPositions, bucketSubgroups)
    val tried = TriedPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val newB = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val newPeer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val triedPeer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = false)

    // Act
    newB.add(newPeer)
    tried.add(triedPeer)

    // Assert
    newB.isEmpty shouldBe false
    tried.isEmpty shouldBe false

    val newPeers = newB.getPeers
    val triedPeers = tried.getPeers
    newPeer.peerInfo.peerSpec.address match {
      case Some(address) => newPeers.contains(address) shouldBe true
      case _ => fail("Expected newPeer to have a valid address")
    }
    triedPeer.peerInfo.peerSpec.address match {
      case Some(address) => triedPeers.contains(address) shouldBe true
      case _ => fail("Expected newPeer to have a valid address")
    }

    system.terminate()
  }

  they should "delete existing peers" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val bucketConfig = BucketConfig(buckets, bucketPositions, bucketSubgroups)
    val tried = TriedPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val newB = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val newPeer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val triedPeer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = false)
    newB.add(newPeer)
    tried.add(triedPeer)

    // Act
    newPeer.peerInfo.peerSpec.address.foreach(address => newB.remove(address))
    triedPeer.peerInfo.peerSpec.address.foreach(address => tried.remove(address))

    // Assert
    newB.isEmpty shouldBe true
    tried.isEmpty shouldBe true

    newB.getPeers shouldBe Map.empty
    tried.getPeers shouldBe Map.empty

    system.terminate()
  }

  they should "do nothing when removing a non existing peer" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val bucketConfig = BucketConfig(buckets, bucketPositions, bucketSubgroups)
    val tried = TriedPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val newB = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val peerAddress = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo = getPeerInfo(peerAddress)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val newPeer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
    val triedPeer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = false)

    // Act
    newPeer.peerInfo.peerSpec.address.foreach(address => newB.remove(address))
    triedPeer.peerInfo.peerSpec.address.foreach(address => tried.remove(address))

    // Assert
    newB.isEmpty shouldBe true
    tried.isEmpty shouldBe true

    newB.getPeers shouldBe Map.empty
    tried.getPeers shouldBe Map.empty

    system.terminate()
  }

  they should "return properly when contains method is called" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val bucketConfig = BucketConfig(buckets, bucketPositions, bucketSubgroups)
    val newB = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)
    val peerAddress1 = new InetSocketAddress("55.66.77.88", 1234)
    val peerInfo1 = getPeerInfo(peerAddress1)
    val peerAddress2 = new InetSocketAddress("88.77.66.55", 1234)
    val peerInfo2 = getPeerInfo(peerAddress2)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    val newPeer1 = PeerBucketValue(peerInfo1, source.toSourcePeer, isNew = true)
    val newPeer2 = PeerBucketValue(peerInfo2, source.toSourcePeer, isNew = true)
    val fakeAddress = new InetSocketAddress("55.88.77.66", 1234)

    // Act
    newB.add(newPeer1)
    newB.add(newPeer2)

    // Assert
    newB.isEmpty shouldBe false
    newB.contains(peerAddress1) shouldBe true
    newB.contains(peerAddress2) shouldBe true
    newB.contains(fakeAddress) shouldBe false

    system.terminate()
  }
}
