package sparkz.core.network


import sparkz.core.{ModifierTypeId, PersistentNodeViewModifier}
import sparkz.core.consensus.History.{ModifierIds, Older}
import sparkz.core.consensus.{History, HistoryReader, ModifierSemanticValidity, SyncInfo}
import sparkz.core.network.message.SyncInfoMessageSpec
import sparkz.core.serialization.SparkzSerializer
import sparkz.core.transaction.{BoxTransaction, MemoryPool}
import sparkz.core.transaction.account.PublicKeyNoncedBox
import sparkz.core.transaction.box.Box.Amount
import sparkz.core.transaction.box.BoxUnlocker
import sparkz.core.transaction.box.proposition.{PublicKey25519Proposition, PublicKey25519PropositionSerializer}
import sparkz.util.ModifierId
import sparkz.util.serialization.{Reader, Writer}

import scala.util.Try

trait TestImplementations {

  class TestMempool extends MemoryPool[TestTransaction, TestMempool] {
    override def put(tx: TestTransaction): Try[TestMempool] = ???
    override def put(txs: Iterable[TestTransaction]): Try[TestMempool] = ???
    override def putWithoutCheck(txs: Iterable[TestTransaction]): TestMempool = ???
    override def remove(tx: TestTransaction): TestMempool = ???
    override def filter(condition: TestTransaction => Boolean): TestMempool = ???
    override def modifierById(modifierId: ModifierId): Option[TestTransaction] = ???
    override def getAll(ids: Seq[ModifierId]): Seq[TestTransaction] = ???
    override def size: Int = ???
    override def take(limit: Int): Iterable[TestTransaction] = ???
    override type NVCT = this.type
  }

  class TestModifier(val id: ModifierId) extends PersistentNodeViewModifier {
    override def parentId: ModifierId = ???

    override val modifierTypeId: ModifierTypeId = ModifierTypeId @@ (0: Byte)
    override type M = this.type

    override def serializer: SparkzSerializer[TestModifier.this.type] = ???
  }

  class TestHistory extends HistoryReader[TestModifier, TestSyncInfo] {
    override def isEmpty: Boolean = ???
    override def applicableTry(modifier: TestModifier): Try[Unit] = ???
    override def modifierById(modifierId: sparkz.util.ModifierId): Option[TestModifier] = ???
    override def isSemanticallyValid(modifierId: sparkz.util.ModifierId): ModifierSemanticValidity = ???
    override def openSurfaceIds(): Seq[sparkz.util.ModifierId] = ???
    override def continuationIds(info: TestSyncInfo, size: Int): ModifierIds = Seq()
    override def syncInfo: TestSyncInfo = new TestSyncInfo
    override def compare(other: TestSyncInfo): History.HistoryComparisonResult = Older
    override type NVCT = this.type
  }

  case class TestSyncInfo() extends SyncInfo {
    override def startingPoints: ModifierIds = Seq()

    override type M = TestSyncInfo

    override def serializer: SparkzSerializer[TestSyncInfo] = TestSyncInfoSerializer
  }

  object TestSyncInfoSerializer extends SparkzSerializer[TestSyncInfo] {
    override def serialize(obj: TestSyncInfo, w: Writer): Unit = {}

    override def parse(r: Reader): TestSyncInfo = TestSyncInfo()
  }

  object TestSyncInfoMessageSpec extends SyncInfoMessageSpec[TestSyncInfo](TestSyncInfoSerializer)

  case class TestTransaction(override val fee: Long, override val timestamp: Long) extends BoxTransaction[PublicKey25519Proposition, TestBox] {
    override type M = TestTransaction
    override val unlockers: Iterable[BoxUnlocker[PublicKey25519Proposition]] = Seq()
    override val newBoxes: Iterable[TestBox] = Seq()
    override val messageToSign: Array[Byte] = Array()
    override def serializer: SparkzSerializer[TestTransaction] = ???
  }

  object TestTransactionSerializer extends SparkzSerializer[TestTransaction] {
    override def serialize(m: TestTransaction, w: Writer): Unit = {
      w.putULong(m.fee)
      w.putULong(m.timestamp)
    }
    override def parse(r: Reader): TestTransaction = {
      val fee = r.getULong()
      val timestamp = r.getULong()
      TestTransaction(fee, timestamp)
    }
  }

  case class TestBox(override val proposition: PublicKey25519Proposition) extends PublicKeyNoncedBox[PublicKey25519Proposition] {
    override type M = TestBox
    override def serializer: SparkzSerializer[TestBox] = TestSerializer
    override val nonce: Long = 1
    override val value: Amount = 1
  }

  object TestSerializer extends SparkzSerializer[TestBox] {
    override def serialize(obj: TestBox, w: Writer): Unit = {
      PublicKey25519PropositionSerializer.serialize(obj.proposition, w)
      w.putLong(obj.nonce)
      w.putULong(obj.value)
    }
    override def parse(r: Reader): TestBox = TestBox(PublicKey25519PropositionSerializer.parse(r))
  }

}
