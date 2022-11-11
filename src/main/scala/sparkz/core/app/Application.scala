package sparkz.core.app

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import scorex.util.ScorexLogging
import sparkz.core.api.http.{ApiErrorHandler, ApiRejectionHandler, ApiRoute, CompositeHttpService}
import sparkz.core.network._
import sparkz.core.network.message._
import sparkz.core.network.peer.PeerBucketStorage.{BucketConfig, NewPeerBucketStorage, TriedPeerBucketStorage}
import sparkz.core.network.peer.{BucketManager, InMemoryPeerDatabase, PeerManagerRef}
import sparkz.core.settings.{NetworkSettings, SparkzSettings}
import sparkz.core.storage.ScheduledActor.ScheduledActorConfig
import sparkz.core.storage.ScheduledStorageFilePersister.ScheduledStorageFilePersisterConfig
import sparkz.core.storage.ScheduledStoragePersister.ScheduledStoragePersisterConfig
import sparkz.core.storage.StoragePersisterContainer.StoragePersisterContainerConfig
import sparkz.core.storage.{ScheduledMapPersister, ScheduledPeerBucketPersister, ScheduledStoragePersister, StoragePersisterContainer}
import sparkz.core.transaction.Transaction
import sparkz.core.utils.NetworkTimeProvider
import sparkz.core.utils.TimeProvider.Time
import sparkz.core.{NodeViewHolder, PersistentNodeViewModifier}

import java.net.{InetAddress, InetSocketAddress}
import java.security.SecureRandom
import scala.collection.mutable
import scala.concurrent.ExecutionContext

trait Application extends ScorexLogging {

  import sparkz.core.network.NetworkController.ReceivableMessages.ShutdownNetwork

  type TX <: Transaction
  type PMOD <: PersistentNodeViewModifier
  type NVHT <: NodeViewHolder[TX, PMOD]

  //settings
  implicit val settings: SparkzSettings

  //api
  val apiRoutes: Seq[ApiRoute]

  implicit def exceptionHandler: ExceptionHandler = ApiErrorHandler.exceptionHandler

  implicit def rejectionHandler: RejectionHandler = ApiRejectionHandler.rejectionHandler

  private val networkSettings: NetworkSettings = settings.network
  protected implicit lazy val actorSystem: ActorSystem = ActorSystem(networkSettings.agentName)
  implicit val executionContext: ExecutionContext = actorSystem.dispatchers.lookup("sparkz.executionContext")

  protected val features: Seq[PeerFeature]
  protected val additionalMessageSpecs: Seq[MessageSpec[_]]
  private val featureSerializers: PeerFeature.Serializers = features.map(f => f.featureId -> f.serializer).toMap

  private lazy val basicSpecs = {
    val invSpec = new InvSpec(networkSettings.maxInvObjects)
    val requestModifierSpec = new RequestModifierSpec(networkSettings.maxInvObjects)
    val modifiersSpec = new ModifiersSpec(networkSettings.maxModifiersSpecMessageSize)
    Seq(
      GetPeersSpec,
      new PeersSpec(featureSerializers, networkSettings.maxPeerSpecObjects),
      invSpec,
      requestModifierSpec,
      modifiersSpec
    )
  }

  val nodeViewHolderRef: ActorRef
  val nodeViewSynchronizer: ActorRef

  /** API description in openapi format in YAML or JSON */
  val swaggerConfig: String

  val timeProvider = new NetworkTimeProvider(settings.ntp)

  //an address to send to peers
  lazy val externalSocketAddress: Option[InetSocketAddress] = {
    networkSettings.declaredAddress
  }

  val sparkzContext: SparkzContext = SparkzContext(
    messageSpecs = basicSpecs ++ additionalMessageSpecs,
    features = features,
    timeProvider = timeProvider,
    externalNodeAddress = externalSocketAddress
  )

  private val nKey: Int = new SecureRandom().nextInt()
  private val newBucketConfig: BucketConfig = BucketConfig(buckets = 1024, bucketPositions = 64, bucketSubgroups = 64)
  private val triedBucketConfig: BucketConfig = BucketConfig(buckets = 256, bucketPositions = 64, bucketSubgroups = 8)

  private def createPeerDatabaseDataStructures: (
    NewPeerBucketStorage, TriedPeerBucketStorage, mutable.Map[InetAddress, Time], mutable.Map[InetAddress, (Int, Time)]
    ) = {
    val triedBucket: TriedPeerBucketStorage = TriedPeerBucketStorage(triedBucketConfig, nKey, timeProvider)
    val newBucket: NewPeerBucketStorage = NewPeerBucketStorage(newBucketConfig, nKey, timeProvider)
    val blacklist = mutable.Map.empty[InetAddress, Time]
    val penaltyBook = mutable.Map.empty[InetAddress, (Int, Time)]

    (newBucket, triedBucket, blacklist, penaltyBook)
  }

  private def createStoragePersisters(triedBucket: TriedPeerBucketStorage, newBucket: NewPeerBucketStorage): Seq[ScheduledStoragePersister[_]] = {
    val scheduledStorageWriterConfig = ScheduledStoragePersisterConfig(
      ScheduledActorConfig(networkSettings.storageBackupDelay, networkSettings.storageBackupInterval)
    )
    val blacklistWriterConfig = ScheduledStorageFilePersisterConfig(settings.dataDir, "BlacklistPeers.dat", scheduledStorageWriterConfig)
    val blacklistPersister = new ScheduledMapPersister[InetAddress, Time](blacklist, blacklistWriterConfig)

    val penaltyWriterConfig = ScheduledStorageFilePersisterConfig(settings.dataDir, "PenaltyBook.dat", scheduledStorageWriterConfig)
    val penaltyBookPersister = new ScheduledMapPersister[InetAddress, (Int, Long)](penaltyBook, penaltyWriterConfig)

    val triedWriterConfig = ScheduledStorageFilePersisterConfig(settings.dataDir, triedBucket.STORAGE_LABEL, scheduledStorageWriterConfig)
    val triedBucketPersister = new ScheduledPeerBucketPersister(triedBucket, triedWriterConfig)

    val newWriterConfig = ScheduledStorageFilePersisterConfig(settings.dataDir, newBucket.STORAGE_LABEL, scheduledStorageWriterConfig)
    val newBucketPersister = new ScheduledPeerBucketPersister(newBucket, newWriterConfig)
    Seq(blacklistPersister, penaltyBookPersister, triedBucketPersister, newBucketPersister)
  }

  private val (newBucket, triedBucket, blacklist, penaltyBook) = createPeerDatabaseDataStructures
  val bucketManager: BucketManager = new BucketManager(newBucket, triedBucket)

  private val peerDatabase = new InMemoryPeerDatabase(
    networkSettings,
    sparkzContext.timeProvider,
    bucketManager,
    blacklist,
    penaltyBook
  )

  private val storagePersisterContainer = new StoragePersisterContainer(
    createStoragePersisters(triedBucket, newBucket),
    StoragePersisterContainerConfig(settings.dataDir)
  )
  storagePersisterContainer.restoreAllStorages()

  val peerManagerRef: ActorRef = PeerManagerRef(settings, sparkzContext, peerDatabase)

  val networkControllerRef: ActorRef = NetworkControllerRef(
    "networkController", networkSettings, peerManagerRef, sparkzContext)

  val peerSynchronizer: ActorRef = PeerSynchronizerRef("PeerSynchronizer",
    networkControllerRef, peerManagerRef, networkSettings, featureSerializers)

  lazy val combinedRoute: Route = CompositeHttpService(actorSystem, apiRoutes, settings.restApi, swaggerConfig).compositeRoute

  def run(): Unit = {
    require(networkSettings.agentName.length <= Application.ApplicationNameLimit)

    log.debug(s"Available processors: ${Runtime.getRuntime.availableProcessors}")
    log.debug(s"Max memory available: ${Runtime.getRuntime.maxMemory}")
    log.debug(s"RPC is allowed at ${settings.restApi.bindAddress.toString}")

    val bindAddress = settings.restApi.bindAddress

    Http().newServerAt(bindAddress.getAddress.getHostAddress, bindAddress.getPort).bindFlow(combinedRoute)

    //on unexpected shutdown
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        log.error("Unexpected shutdown")
        stopAll()
      }
    })
  }

  def stopAll(): Unit = synchronized {
    log.info("Stopping network services")
    networkControllerRef ! ShutdownNetwork

    log.info("Stopping actors (incl. block generator)")
    actorSystem.terminate().onComplete { _ =>
      log.info("Exiting from the app...")
      System.exit(0)
    }
  }
}

object Application {

  val ApplicationNameLimit: Int = 50
}
