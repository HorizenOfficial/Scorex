package sparkz.core.network

import akka.actor.{ActorContext, ActorSystem}
import akka.testkit.TestProbe
import org.mockito.MockitoSugar.mock
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import sparkz.core.consensus.History.{Fork, HistoryComparisonResult, Unknown}
import sparkz.core.settings.NetworkSettings
import sparkz.core.utils.NetworkTimeProvider
import sparkz.core.utils.TimeProvider.Time

import java.net.InetSocketAddress
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class SyncTrackerSpec extends AnyPropSpec with Matchers {
  private val system = ActorSystem()
  private implicit val executionContext: ExecutionContext = mock[ExecutionContext]
  private val probe = TestProbe("peerActor")(system)
  private val networkSettings = mock[NetworkSettings]
  private val timeProvider = mock[NetworkTimeProvider]
  private val context = mock[ActorContext]

  private val address = new InetSocketAddress(666)
  private val connectedPeer = ConnectedPeer(ConnectionId(address, address, Outgoing), probe.ref, 10000, None)

  property("calling updateStatus adds correctly the data into the statusTracker") {
    // Arrange
    val syncTracker = new SyncTracker(probe.ref, context, networkSettings, timeProvider)

    val statusField = classOf[SyncTracker].getDeclaredField("statuses")
    statusField.setAccessible(true)

    // Act
    syncTracker.updateStatus(connectedPeer, Fork)
    val statusAfterInsertion = statusField.get(syncTracker).asInstanceOf[mutable.Map[ConnectedPeer, HistoryComparisonResult]]

    // Assert
    statusAfterInsertion.size should be(1)
  }

  property("calling the clear status correctly removes the data from the statusTracker") {
    // Arrange
    val syncTracker = new SyncTracker(probe.ref, context, networkSettings, timeProvider)

    val statusField = classOf[SyncTracker].getDeclaredField("statuses")
    statusField.setAccessible(true)
    val lastTimestampField = classOf[SyncTracker].getDeclaredField("lastSyncSentTime")
    lastTimestampField.setAccessible(true)

    // Act
    syncTracker.updateStatus(connectedPeer, Fork)
    syncTracker.updateLastSyncSentTime(connectedPeer)

    syncTracker.clearStatus(address)
    val statusAfterDeletion = statusField.get(syncTracker).asInstanceOf[mutable.Map[ConnectedPeer, HistoryComparisonResult]]
    val timestampAfterDeletion = lastTimestampField.get(syncTracker).asInstanceOf[mutable.Map[ConnectedPeer, Time]]

    // Assert
    statusAfterDeletion.size should be(0)
    timestampAfterDeletion.size should be(0)
  }

  property("syncTracker should start tracking a peer once the handshake message has been received") {
    // Arrange
    val syncTracker = new SyncTracker(probe.ref, context, networkSettings, timeProvider)

    val lastSyncSentTimeField = classOf[SyncTracker].getDeclaredField("lastSyncSentTime")
    lastSyncSentTimeField.setAccessible(true)

    val lastSyncSentTime = lastSyncSentTimeField.get(syncTracker).asInstanceOf[mutable.Map[ConnectedPeer, Time]]

    // Act
    lastSyncSentTime should be(Map.empty)
    syncTracker.updateStatus(connectedPeer, Unknown)

    // Assert
    lastSyncSentTime.contains(connectedPeer) should be(true)
  }
}
