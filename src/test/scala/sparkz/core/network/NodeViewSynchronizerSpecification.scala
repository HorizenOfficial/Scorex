package sparkz.core.network

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestProbe}
import sparkz.ObjectGenerators
import sparkz.core.NodeViewHolder.ReceivableMessages.{GetNodeViewChanges, TransactionsFromRemote}
import sparkz.core.consensus.History.Unknown
import sparkz.core.network.NetworkController.ReceivableMessages.{PenalizePeer, RegisterMessageSpecs, SendToNetwork}
import sparkz.core.network.NodeViewSynchronizer.ReceivableMessages._
import sparkz.core.network.message._
import sparkz.core.network.peer.PenaltyType
import sparkz.core.network.peer.PenaltyType.MisbehaviorPenalty
import sparkz.core.serialization.SparkzSerializer
import sparkz.core.settings.SparkzSettings
import sparkz.core.transaction.Transaction
import sparkz.core.utils.NetworkTimeProvider
import sparkz.core.{ModifierTypeId, NodeViewModifier}
import sparkz.util.ModifierId
import sparkz.util.serialization.{Reader, Writer}

import java.net.InetSocketAddress
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class NodeViewSynchronizerSpecification extends NetworkTests with TestImplementations with ObjectGenerators {

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContext = actorSystem.dispatchers.lookup("sparkz.executionContext")
  private val modifiersSpec = new ModifiersSpec(1024 * 1024)
  private val requestModifierSpec = new RequestModifierSpec(settings.network.maxInvObjects)
  private val syncSpec = new SyncInfoMessageSpec[TestSyncInfo](new SparkzSerializer[TestSyncInfo] {
    override def serialize(obj: TestSyncInfo, w: Writer): Unit = {}

    override def parse(r: Reader): TestSyncInfo = new TestSyncInfo
  })
  private val invSpec = new InvSpec(settings.network.maxInvObjects)
  private val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(settings)
  private val peerProbe = TestProbe()
  private val peer = ConnectedPeer(ConnectionId(new InetSocketAddress(10), new InetSocketAddress(11), Incoming), peerProbe.ref, 0L, None)
  private val messageSerializer = new MessageSerializer(Seq(modifiersSpec, invSpec, requestModifierSpec, syncSpec),
                                                        settings.network.magicBytes,
                                                        settings.network.messageLengthBytesLimit)


  "NodeViewSynchronizer" should "not penalize peer for sending valid modifiers" in {
    val transaction = TestTransaction(1, 1)
    val txBytes = TestTransactionSerializer.toBytes(transaction)

    synchronizer ! roundTrip(Message(modifiersSpec, Right(ModifiersData(Transaction.ModifierTypeId, Map(transaction.id -> txBytes))), Some(peer)))
    viewHolder.expectMsg(TransactionsFromRemote(Seq(transaction)))
    networkController.expectNoMessage()
  }

  it should "penalize peer for sending malformed modifiers" in {
    val transaction = TestTransaction(1, 1)
    val txBytes = TestTransactionSerializer.toBytes(transaction) ++ Array[Byte](0x01, 0x02)

    synchronizer ! roundTrip(Message(modifiersSpec, Right(ModifiersData(Transaction.ModifierTypeId, Map(transaction.id -> txBytes))), Some(peer)))
    viewHolder.expectMsg(TransactionsFromRemote(Seq(transaction)))
    networkController.expectMsg(PenalizePeer(peer.connectionId.remoteAddress, MisbehaviorPenalty))
  }

  it should "request transactions if handlingTransactionsEnabled is true" in {

    val networkSettings = settings.copy(
        network = settings.network.copy(
          handlingTransactionsEnabled = true
        )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)
    setupHistoryAndMempoolReaders(synchronizer)

    val transaction = TestTransaction(1, 1)
    val invMsg = Message(invSpec, Right(InvData(Transaction.ModifierTypeId, Seq(transaction.id ))), Some(peer))

    synchronizer ! roundTrip(invMsg)

    viewHolder.expectNoMessage()
    networkController.expectNoMessage()
    peerProbe.receiveOne(0.seconds) match {
      case Message(mod: MessageSpec[_], data: Right[_, _], _) =>
        assert(mod.messageCode == RequestModifierSpec.MessageCode)
        assert(data == Right(InvData(Transaction.ModifierTypeId, Vector(transaction.id))))
      case _ => fail("Wrong message received by peer connection handler")
    }
  }

  it should "not request transactions if handlingTransactionsEnabled is false" in {

    val networkSettings = settings.copy(
      network = settings.network.copy(
        handlingTransactionsEnabled = false
      )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)

    setupHistoryAndMempoolReaders(synchronizer)

    val transaction = TestTransaction(1, 1)
    val invMsg = Message(invSpec, Right(InvData(Transaction.ModifierTypeId, Seq(transaction.id))), Some(peer))

    synchronizer ! roundTrip(invMsg)
    viewHolder.expectNoMessage()
    networkController.expectNoMessage()
    peerProbe.expectNoMessage()
  }


  it should "return transactions if handlingTransactionsEnabled is true" in {

    val networkSettings = settings.copy(
      network = settings.network.copy(
        handlingTransactionsEnabled = true
      )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)
    setupHistoryAndMempoolReaders(synchronizer)

    val transaction = TestTransaction(1, 1)

    val mempool = new TestMempool {
      override def modifierById(modifierId: ModifierId): Option[TestTransaction] = None
      override def getAll(ids: Seq[ModifierId]): Seq[TestTransaction] = Seq(transaction)
    }

    synchronizer ! ChangedMempool(mempool)

    val invMsg = Message(requestModifierSpec, Right(InvData(Transaction.ModifierTypeId, Seq(transaction.id ))),  Some(peer))

    synchronizer ! roundTrip(invMsg)

    viewHolder.expectNoMessage()
    networkController.expectNoMessage()
    peerProbe.receiveOne(0.seconds) match {
      case Message(mod: MessageSpec[_], _: Either[_, ModifiersData], _) =>
        assert(mod.messageCode == ModifiersSpec.MessageCode)
      case p => fail(s"Wrong message received by peer connection handler $p")
    }
  }

  it should "not return transactions if handlingTransactionsEnabled is false" in {

    val networkSettings = settings.copy(
      network = settings.network.copy(
        handlingTransactionsEnabled = false
      )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)
    setupHistoryAndMempoolReaders(synchronizer)

    val transaction = TestTransaction(1, 1)

    val mempool = new TestMempool {
      override def modifierById(modifierId: ModifierId): Option[TestTransaction] = None
      override def getAll(ids: Seq[ModifierId]): Seq[TestTransaction] = Seq(transaction)
    }

    synchronizer ! ChangedMempool(mempool)

    val invMsg = Message(requestModifierSpec, Right(InvData(Transaction.ModifierTypeId, Seq(transaction.id))), Some(peer))

    synchronizer ! roundTrip(invMsg)

    viewHolder.expectNoMessage()
    networkController.expectNoMessage()
    peerProbe.expectNoMessage()
  }

  it should "broadcast transactions when SuccessfulTransaction if handlingTransactionsEnabled is true" in {

    val networkSettings = settings.copy(
      network = settings.network.copy(
        handlingTransactionsEnabled = true
      )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)

    val transaction = TestTransaction(1, 1)
    synchronizer ! SuccessfulTransaction(transaction)

    viewHolder.expectNoMessage()
    peerProbe.expectNoMessage()
    networkController.receiveOne(0.seconds) match {

      case SendToNetwork(msg: Message[_],  _: SendingStrategy) =>
        assert(msg.spec.messageCode == InvSpec.MessageCode)
        assert(msg.data == Success(InvData(Transaction.ModifierTypeId, Seq(transaction.id))))
      case p => fail(s"Wrong message received by peer connection handler: $p")
    }

  }

  it should "not broadcast transactions  when SuccessfulTransaction if handlingTransactionsEnabled is false" in {

    val networkSettings = settings.copy(
      network = settings.network.copy(
        handlingTransactionsEnabled = false
      )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)

    val transaction = TestTransaction(1, 1)
    synchronizer ! SuccessfulTransaction(transaction)

    viewHolder.expectNoMessage()
    peerProbe.expectNoMessage()
    networkController.expectNoMessage()
  }

  it should "penalize peer when FailedTransaction if handlingTransactionsEnabled is true" in {

    val networkSettings = settings.copy(
      network = settings.network.copy(
        handlingTransactionsEnabled = true
      )
    )
    val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)

    val transaction = TestTransaction(1, 1)
    synchronizer ! FailedTransaction(transaction.id, new Exception(), true)

    viewHolder.expectNoMessage()
    peerProbe.expectNoMessage()
    networkController.receiveOne(0.seconds) match {
      case PenalizePeer(remoteAddress: InetSocketAddress, penalty: PenaltyType) =>
        assert(remoteAddress == peer.connectionId.remoteAddress)
        assert(penalty == PenaltyType.MisbehaviorPenalty)
      case p => fail(s"Wrong message received by peer connection handler $p")
    }
  }

  it should "not penalize peer when FailedTransaction if handlingTransactionsEnabled is false" in {

      val networkSettings = settings.copy(
        network = settings.network.copy(
          handlingTransactionsEnabled = false
        )
      )
      val (synchronizer, networkController, viewHolder) = createNodeViewSynchronizer(networkSettings)

      val transaction = TestTransaction(1, 1)
      synchronizer ! FailedTransaction(transaction.id, new Exception(), true)

      viewHolder.expectNoMessage()
      peerProbe.expectNoMessage()
      networkController.expectNoMessage()
    }

  it should "when HandshakedPeer message is received, statusTracker should add the peer" in {
    // Arrange
    val nodeViewSynchronizerRef = createNodeViewSynchronizerAsTestActorRef(settings)
    val nodeViewSynchronizer = nodeViewSynchronizerRef.underlyingActor

    val statusTrackerField = classOf[NodeViewSynchronizer[_, _, _, _, _, _]].getDeclaredField("statusTracker")
    statusTrackerField.setAccessible(true)
    val statusTracker = statusTrackerField.get(nodeViewSynchronizer).asInstanceOf[SyncTracker]

    // Act
    nodeViewSynchronizerRef ! HandshakedPeer(peer)

    // Assert
    val peersStatus = statusTracker.peersByStatus
    peersStatus.nonEmpty should be(true)
    peersStatus(Unknown) should be(Set(peer))
  }

  def setupHistoryAndMempoolReaders(synchronizer: ActorRef): Unit = {
    val history = new TestHistory
    val mempool = new TestMempool {
      override def modifierById(modifierId: ModifierId): Option[TestTransaction] = None
    }

    synchronizer ! ChangedHistory(history)
    synchronizer ! ChangedMempool(mempool)

  }

  def roundTrip(msg: Message[_]): Message[_] = {
    messageSerializer.deserialize(messageSerializer.serialize(msg), msg.source) match {
      case Failure(e) => throw e
      case Success(value) => value match {
        case Some(value) => value
        case None => fail("roundTrip test method unexpected behavior")
      }
    }
  }

  private def createNodeViewSynchronizer(settings: SparkzSettings): (ActorRef, TestProbe, TestProbe) = {
    val networkControllerProbe = TestProbe()
    val viewHolderProbe = TestProbe()
    val timeProvider = new NetworkTimeProvider(settings.ntp)

    val modifierSerializers: Map[ModifierTypeId, SparkzSerializer[_ <: NodeViewModifier]] =
      Map(Transaction.ModifierTypeId -> TestTransactionSerializer)

    val nodeViewSynchronizerRef = actorSystem.actorOf(Props(
      new NodeViewSynchronizer[TestTransaction,
          TestSyncInfo,
          TestSyncInfoMessageSpec.type,
          TestModifier,
          TestHistory,
          TestMempool
        ]
      (
        networkControllerProbe.ref,
        viewHolderProbe.ref,
        TestSyncInfoMessageSpec,
        settings.network,
        timeProvider,
        modifierSerializers
      ) {
        override val deliveryTracker: DeliveryTracker = new DeliveryTracker(context.system, settings.network, self) {
          override def status(modifierId: ModifierId): ModifiersStatus = ModifiersStatus.Requested
          override private[network] def clearStatusForModifier(id: ModifierId, oldStatus: ModifiersStatus): Unit = {}
          override def setInvalid(modifierId: ModifierId): Option[ConnectedPeer] = Some(peer)
        }
      }
    ))
    networkControllerProbe.expectMsgType[RegisterMessageSpecs](5000.millis)
    viewHolderProbe.expectMsgType[GetNodeViewChanges](5000.millis)

    (nodeViewSynchronizerRef, networkControllerProbe, viewHolderProbe)
  }

  private def createNodeViewSynchronizerAsTestActorRef(settings: SparkzSettings): TestActorRef[NodeViewSynchronizer[_, _, _, _, _, _]] = {
    val networkControllerProbe = TestProbe()
    val viewHolderProbe = TestProbe()
    val timeProvider = new NetworkTimeProvider(settings.ntp)

    val modifierSerializers: Map[ModifierTypeId, SparkzSerializer[_ <: NodeViewModifier]] =
      Map(Transaction.ModifierTypeId -> TestTransactionSerializer)

    TestActorRef(Props(
      new NodeViewSynchronizer[TestTransaction,
        TestSyncInfo,
        TestSyncInfoMessageSpec.type,
        TestModifier,
        TestHistory,
        TestMempool
      ]
      (
        networkControllerProbe.ref,
        viewHolderProbe.ref,
        TestSyncInfoMessageSpec,
        settings.network,
        timeProvider,
        modifierSerializers
      ) {
        override val deliveryTracker: DeliveryTracker = new DeliveryTracker(context.system, settings.network, self) {
          override def status(modifierId: ModifierId): ModifiersStatus = ModifiersStatus.Requested

          override private[network] def clearStatusForModifier(id: ModifierId, oldStatus: ModifiersStatus): Unit = {}

          override def setInvalid(modifierId: ModifierId): Option[ConnectedPeer] = Some(peer)
        }
      }
    ))
  }
}
