package sparkz.core.network.peer

import org.scalatest.Assertion
import sparkz.ObjectGenerators
import sparkz.core.network.NetworkTests
import sparkz.core.network.peer.PeerBucketStorage.{BucketConfig, NewPeerBucketStorage, TriedPeerBucketStorage}

import java.net.InetSocketAddress
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

@SuppressWarnings(Array(
  "org.wartremover.warts.Null",
  "org.wartremover.warts.OptionPartial"
))
class InMemoryPeerDatabaseSpec extends NetworkTests with ObjectGenerators {

  private val peerAddress1 = new InetSocketAddress("1.1.1.1", 27017)
  private val peerAddress2 = new InetSocketAddress("2.2.2.2", 27017)
  private val storedPeersLimit = 10
  private val nKey = 1234
  private val bucketConfig: BucketConfig = BucketConfig(buckets = 10, bucketPositions = 10, bucketSubgroups = 10)
  private val triedBucket: TriedPeerBucketStorage = TriedPeerBucketStorage(bucketConfig, nKey, timeProvider)
  private val newBucket: NewPeerBucketStorage = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)
  private val bucketManager: BucketManager = new BucketManager(newBucket, triedBucket)
  private val sourcePeer = connectedPeerGen(null).sample.get

  private def withDb(test: InMemoryPeerDatabase => Assertion): Assertion =
    test(new InMemoryPeerDatabase(
      settings.network.copy(storedPeersLimit = storedPeersLimit, penaltySafeInterval = 1.seconds),
      timeProvider,
      bucketManager,
      mutable.Map.empty,
      mutable.Map.empty))

  "new DB" should "be empty" in {
    withDb { db =>
      db.isEmpty shouldBe true
      db.blacklistedPeers.isEmpty shouldBe true
      db.allPeers.isEmpty shouldBe true
      db.allPeers.isEmpty shouldBe true
    }
  }

  it should "be non-empty after adding a peer" in {
    withDb { db =>
      db.addOrUpdateKnownPeer(getPeerInfo(peerAddress1), Some(sourcePeer))
      db.isEmpty shouldBe false
      db.blacklistedPeers.isEmpty shouldBe true
      db.allPeers.isEmpty shouldBe false
    }
  }

  it should "return a peer after adding a peer" in {
    withDb { db =>
      val peerInfo = getPeerInfo(peerAddress1)

      db.addOrUpdateKnownPeer(peerInfo, Some(sourcePeer))
      db.allPeers shouldBe Map(peerAddress1 -> peerInfo)
    }
  }

  it should "return an updated peer after updating a peer" in {
    withDb { db =>
      val peerInfo = getPeerInfo(peerAddress1, Some("initialName"))
      db.addOrUpdateKnownPeer(peerInfo, Some(sourcePeer))
      val newPeerInfo = getPeerInfo(peerAddress1, Some("updatedName"))
      db.addOrUpdateKnownPeer(newPeerInfo, Some(sourcePeer))

      db.allPeers shouldBe Map(peerAddress1 -> newPeerInfo)
    }
  }

  it should "return a blacklisted peer after blacklisting" in {
    withDb { db =>
      db.addOrUpdateKnownPeer(getPeerInfo(peerAddress1), Some(sourcePeer))
      db.addOrUpdateKnownPeer(getPeerInfo(peerAddress2), Some(sourcePeer))
      db.addToBlacklist(peerAddress1, PenaltyType.PermanentPenalty)

      db.isBlacklisted(peerAddress1.getAddress) shouldBe true
      db.isBlacklisted(peerAddress2.getAddress) shouldBe false
      db.blacklistedPeers shouldBe Seq(peerAddress1.getAddress)
    }
  }

  it should "the blacklisted peer be absent in knownPeers" in {
    withDb { db =>
      val peerInfo1 = getPeerInfo(peerAddress1)
      db.addOrUpdateKnownPeer(peerInfo1, Some(sourcePeer))
      db.addOrUpdateKnownPeer(getPeerInfo(peerAddress2), Some(sourcePeer))
      db.addToBlacklist(peerAddress2, PenaltyType.PermanentPenalty)

      db.allPeers shouldBe Map(peerAddress1 -> peerInfo1)
    }
  }

  it should "remove peers from db correctly" in {
    withDb { db =>
      db.addOrUpdateKnownPeer(getPeerInfo(peerAddress1), Some(sourcePeer))
      db.isEmpty shouldBe false
      db.blacklistedPeers.isEmpty shouldBe true
      db.allPeers.isEmpty shouldBe false

      db.remove(peerAddress1)

      db.isEmpty shouldBe true
    }
  }

  it should "blacklist immediately when a permanent penalty is applied" in {
    withDb { db =>
      db.peerPenaltyScoreOverThreshold(peerAddress1, PenaltyType.SpamPenalty) shouldBe false
      db.peerPenaltyScoreOverThreshold(peerAddress1, PenaltyType.PermanentPenalty) shouldBe true
    }
  }

  it should "not apply another penalty within a safe interval" in {
    withDb { db =>
      db.peerPenaltyScoreOverThreshold(peerAddress1, PenaltyType.SpamPenalty)
      db.penaltyScore(peerAddress1) shouldBe PenaltyType.SpamPenalty.penaltyScore
      db.peerPenaltyScoreOverThreshold(peerAddress1, PenaltyType.MisbehaviorPenalty)
      db.penaltyScore(peerAddress1) shouldBe PenaltyType.SpamPenalty.penaltyScore
    }
  }

  it should "not update penalty timestamp on consecutive violations during safeInterval" in {
    withDb { db =>
      // penalty interval is 1 second
      // 1. ~0.0 -> first penalty  - applied
      // 2. ~0.0 -> second penalty - not applied
      // 3. ~0.6 -> third penalty  - not applied and not updating timestamp
      // 4. ~1.2 -> fourth penalty - applied
      val address = new InetSocketAddress("192.168.31.1", 7280)
      val penalty = PenaltyType.SpamPenalty
      // ==1==
      db.peerPenaltyScoreOverThreshold(address, penalty)
      db.penaltyScore(address) shouldBe penalty.penaltyScore
      // ==2==
      db.peerPenaltyScoreOverThreshold(address, penalty)
      db.penaltyScore(address) shouldBe penalty.penaltyScore
      // ==3==
      Thread.sleep(600)
      db.peerPenaltyScoreOverThreshold(address, penalty)
      db.penaltyScore(address) shouldBe penalty.penaltyScore
      // ==4==
      Thread.sleep(600)
      db.peerPenaltyScoreOverThreshold(address, penalty)
      db.penaltyScore(address) shouldBe penalty.penaltyScore * 2
    }
  }

  it should "penalize a peer by banning it" in {
    withDb { db =>
      val address = new InetSocketAddress("192.168.31.1", 7280)
      val penaltyType = PenaltyType.DisconnectPenalty(settings.network)

      val penalizeWithBan = db.peerPenaltyScoreOverThreshold(address, penaltyType)

      penalizeWithBan shouldBe true
    }
  }

}
