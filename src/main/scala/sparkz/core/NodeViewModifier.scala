package sparkz.core

import com.typesafe.config.ConfigFactory
import sparkz.core.serialization.BytesSerializable
import sparkz.core.transaction.Transaction
import sparkz.util.SparkzEncoding

import scala.util.Try

sealed trait NodeViewModifier extends BytesSerializable with SparkzEncoding {
  self =>

  val modifierTypeId: ModifierTypeId

  //todo: check statically or dynamically output size
  def id: sparkz.util.ModifierId

  def encodedId: String = encoder.encodeId(id)

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: NodeViewModifier => (that.id == id) && (that.modifierTypeId == modifierTypeId)
    case _ => false
  }
}

trait EphemerealNodeViewModifier extends NodeViewModifier

/**
  * It is supposed that all the modifiers (offchain transactions, blocks, blockheaders etc)
  * have identifiers of the some length fixed with the ModifierIdSize constant
  */
object NodeViewModifier {
  private val DefaultIdSize: Byte = 32 // in bytes

  val ModifierIdSize: Int = Try(ConfigFactory.load().getConfig("app").getInt("modifierIdSize")).getOrElse(DefaultIdSize)
}

/**
  * Persistent node view modifier is a part of replicated log of state transformations.
  * The log should be deterministic and ordered to get a deterministic state after its
  * application to the genesis state and thus every modifier should have parent - modifier,
  * which application should precede this modifier application
  */
trait PersistentNodeViewModifier extends NodeViewModifier {
  /**
    * Id modifier, which should be applied to the node view before this modifier
    */
  def parentId: sparkz.util.ModifierId
}


trait TransactionsCarryingPersistentNodeViewModifier[TX <: Transaction]
  extends PersistentNodeViewModifier {

  def transactions: Seq[TX]
}
