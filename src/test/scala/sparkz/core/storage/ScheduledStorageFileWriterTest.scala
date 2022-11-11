package sparkz.core.storage

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.mockito.MockitoSugar.mock
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PeerBucketStorage.{BucketConfig, NewPeerBucketStorage}
import sparkz.core.network.{ConnectedPeer, ConnectionId, Incoming, NetworkTests}
import sparkz.core.storage.ScheduledActor.ScheduledActorConfig
import sparkz.core.storage.ScheduledStorageFilePersister.ScheduledStorageFilePersisterConfig
import sparkz.core.storage.ScheduledStoragePersister.ScheduledStoragePersisterConfig
import sparkz.core.storage.StoragePersisterContainer.StoragePersisterContainerConfig
import sparkz.core.utils.TimeProvider

import java.io.File
import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.Random

class ScheduledStorageFileWriterTest extends NetworkTests with BeforeAndAfterAll with BeforeAndAfterEach {
  private val tempDir = Files.createTempDirectory("temp-directory")
  private var tempFile: Option[Path] = None
  private val random = new Random()
  private val MAX_ELEMENTS_TO_INSERT = 10000

  override protected def afterAll(): Unit = {
    tempDir.toFile.deleteOnExit()
  }

  override protected def beforeEach(): Unit = {
    tempFile = Some(Files.createTempFile(tempDir, "tempFile", ""))
  }

  override protected def afterEach(): Unit = {
    tempFile.foreach(path => Files.deleteIfExists(path))
  }

  private implicit val executionContext: ExecutionContext = mock[ExecutionContext]

  "The ScheduledPeerBucketWriter" should "persist and restore the peers in a bucket" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val nKey = 1234

    val bucketConfig = BucketConfig(1024, 512, 64)
    val newB = NewPeerBucketStorage(bucketConfig, nKey, timeProvider)

    val writerConfig = tempFile match {
      case Some(file) =>
        val scheduledStorageWriterConfig = ScheduledStoragePersisterConfig(
          ScheduledActorConfig(1.minutes, 10.minutes)
        )
        ScheduledStorageFilePersisterConfig(tempDir.toFile, file.getFileName.toString, scheduledStorageWriterConfig)
      case None => fail("")
    }

    val storageWriter = new ScheduledPeerBucketPersister(newB, writerConfig)
    val source = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), TestProbe().ref, 0L, None)
    for (_ <- 1 to MAX_ELEMENTS_TO_INSERT) {
      val peerAddress = new InetSocketAddress(s"${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}", random.nextInt(35000))
      val peerInfo = getPeerInfo(peerAddress)
      val peer = PeerBucketValue(peerInfo, source.toSourcePeer, isNew = true)
      newB.add(peer)
    }
    val expectedPeersSize = newB.getPeers.size
    expectedPeersSize should be > 0

    // Act
    storageWriter.persist()
    newB.clear()

    val peersSizeAfterCleaning = newB.getPeers.size
    peersSizeAfterCleaning shouldBe 0

    storageWriter.restore()

    // Assert
    newB.getPeers.size shouldBe expectedPeersSize

    system.terminate()
  }

  "The ScheduledMapWriter" should "persist and restore the peers in a map" in {
    // Arrange
    implicit val system: ActorSystem = ActorSystem()

    val blacklist = mutable.Map.empty[InetAddress, TimeProvider.Time]
    val penaltyBook = mutable.Map.empty[InetAddress, (Int, Long)]
    val config = tempFile match {
      case Some(file) =>
        val scheduledStorageWriterConfig = ScheduledStoragePersisterConfig(
          ScheduledActorConfig(1.minutes, 10.minutes)
        )
        ScheduledStorageFilePersisterConfig(tempDir.toFile, file.getFileName.toString, scheduledStorageWriterConfig)
      case None => fail("")
    }

    val blacklistWriter = new ScheduledMapPersister(blacklist, config)
    val penaltyWriter = new ScheduledMapPersister(penaltyBook, config)

    for (_ <- 1 to MAX_ELEMENTS_TO_INSERT) {
      val peerAddress = new InetSocketAddress(s"${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}", random.nextInt(35000)).getAddress
      blacklist += peerAddress -> timeProvider.time()
      penaltyBook += peerAddress -> (random.nextInt(), random.nextLong())
    }
    val expectedBlacklistedSize = blacklist.size
    expectedBlacklistedSize shouldBe MAX_ELEMENTS_TO_INSERT

    val expectedPenaltyBookSize = blacklist.size
    expectedPenaltyBookSize shouldBe MAX_ELEMENTS_TO_INSERT

    // Act
    blacklistWriter.persist()
    penaltyWriter.persist()

    blacklist.clear()
    penaltyBook.clear()

    blacklist.size shouldBe 0
    penaltyBook.size shouldBe 0

    blacklistWriter.restore()
    penaltyWriter.restore()

    // Assert
    blacklist.size shouldBe MAX_ELEMENTS_TO_INSERT
    penaltyBook.size shouldBe MAX_ELEMENTS_TO_INSERT

    system.terminate()
  }
}
