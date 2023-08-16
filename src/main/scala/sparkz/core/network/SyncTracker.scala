package sparkz.core.network

import java.net.InetSocketAddress
import akka.actor.{ActorContext, ActorRef, Cancellable}
import sparkz.core.consensus.History
import sparkz.core.network.NodeViewSynchronizer.Events.{BetterNeighbourAppeared, NoBetterNeighbour}
import sparkz.core.network.NodeViewSynchronizer.ReceivableMessages.SendLocalSyncInfo
import sparkz.core.settings.NetworkSettings
import sparkz.core.utils.TimeProvider
import sparkz.util.SparkzLogging

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * SyncTracker caches the peers' statuses (i.e. whether they are ahead or behind this node)
  */
class SyncTracker(nvsRef: ActorRef,
                  context: ActorContext,
                  networkSettings: NetworkSettings,
                  timeProvider: TimeProvider)(implicit ec: ExecutionContext) extends SparkzLogging {

  import History._
  import sparkz.core.utils.TimeProvider.Time

  private var schedule: Option[Cancellable] = None

  private val statuses = mutable.Map[ConnectedPeer, HistoryComparisonResult]()
  private val lastSyncSentTime = mutable.Map[ConnectedPeer, Time]()

  private var lastSyncInfoSentTime: Time = 0L

  private var stableSyncRegime = false

  def scheduleSendSyncInfo(): Unit = {
    schedule foreach {
      _.cancel()
    }

    val syncTask = context.system.scheduler.scheduleWithFixedDelay(2.seconds, minInterval(),nvsRef, SendLocalSyncInfo)
    schedule = Some(syncTask)
  }

  def maxInterval(): FiniteDuration =
    if (stableSyncRegime) networkSettings.syncStatusRefreshStable
    else networkSettings.syncStatusRefresh

  def minInterval(): FiniteDuration =
    if (stableSyncRegime) networkSettings.syncIntervalStable
    else networkSettings.syncInterval

  def updateStatus(peer: ConnectedPeer, status: HistoryComparisonResult): Unit = {
    val seniorsBefore = numOfSeniors()
    statuses += peer -> status

    if (isFirstTimeTrackingPeer(peer)) {
      updateLastSyncSentTime(peer)
    }

    val seniorsAfter = numOfSeniors()

    // todo: we should also send NoBetterNeighbour signal when all the peers around are not seniors initially
    if (seniorsBefore > 0 && seniorsAfter == 0) {
      log.info("Syncing is done, switching to stable regime")
      stableSyncRegime = true
      scheduleSendSyncInfo()
      context.system.eventStream.publish(NoBetterNeighbour)
    }
    if (seniorsBefore == 0 && seniorsAfter > 0) {
      context.system.eventStream.publish(BetterNeighbourAppeared)
    }
  }

  private def isFirstTimeTrackingPeer(peer: ConnectedPeer) = {
    !lastSyncSentTime.contains(peer)
  }

  //todo: combine both?
  def clearStatus(remote: InetSocketAddress): Unit = {
    statuses.find(_._1.connectionId.remoteAddress == remote) match {
      case Some((peer, _)) => statuses -= peer
      case None => log.warn(s"Trying to clear status for $remote, but it is not found")
    }

    lastSyncSentTime.find(_._1.connectionId.remoteAddress == remote) match {
      case Some((peer, _)) => lastSyncSentTime -= peer
      case None => log.warn(s"Trying to clear last sync time for $remote, but it is not found")
    }
  }

  def updateLastSyncSentTime(peer: ConnectedPeer): Unit = {
    val currentTime = timeProvider.time()
    lastSyncSentTime(peer) = currentTime
    lastSyncInfoSentTime = currentTime
  }

  def elapsedTimeSinceLastSync(): Long = timeProvider.time() - lastSyncInfoSentTime

  private def outdatedPeers(): Seq[ConnectedPeer] =
    lastSyncSentTime.filter(t => (timeProvider.time() - t._2).millis > maxInterval()).keys.toSeq

  @nowarn def peersByStatus: Map[HistoryComparisonResult, Iterable[ConnectedPeer]] =
    statuses.groupBy(_._2).mapValues(_.keys).toMap

  private def numOfSeniors(): Int = statuses.count(_._2 == Older)

  /**
    * Return the peers to which this node should send a sync signal, including:
    * outdated peers, if any, otherwise, all the peers with unknown status plus a random peer with
    * `Older` status.
    * Updates lastSyncSentTime for all returned peers as a side effect
    */
  def peersToSyncWith(): Seq[ConnectedPeer] = {
    val outdated = outdatedPeers()
    val peers =
      if (outdated.nonEmpty) outdated
      else {
        val unknowns = statuses.filter(_._2 == Unknown).keys.toIndexedSeq
        val forks = statuses.filter(_._2 == Fork).keys.toIndexedSeq
        val elders = statuses.filter(_._2 == Older).keys.toIndexedSeq
        val nonOutdated =
          (if (elders.nonEmpty) elders(scala.util.Random.nextInt(elders.size)) +: unknowns else unknowns) ++ forks
        nonOutdated.filter(p => (timeProvider.time() - lastSyncSentTime.getOrElse(p, 0L)).millis >= minInterval())
      }

    peers.foreach(updateLastSyncSentTime)
    peers
  }

}
