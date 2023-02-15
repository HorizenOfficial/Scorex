package sparkz.core.transaction

import sparkz.core.consensus.ContainsModifiers
import sparkz.core.NodeViewComponent
import sparkz.util.ModifierId
import sparkz.core.NodeViewComponent

/**
  * Unconfirmed transactions pool
  *
  * @tparam TX -type of transaction the pool contains
  */
trait MempoolReader[TX <: Transaction] extends NodeViewComponent with ContainsModifiers[TX] {

  //getters
  override def modifierById(modifierId: ModifierId): Option[TX]

  @deprecated("use modifierById instead", "2018-08-14")
  def getById(id: ModifierId): Option[TX] = modifierById(id)

  def contains(id: ModifierId): Boolean

  //get ids from Seq, not presenting in mempool
  def notIn(ids: Seq[ModifierId]): Seq[ModifierId] = ids.filter(id => !contains(id))

  def getAll(ids: Seq[ModifierId]): Seq[TX]

  def size: Int

  def take(limit: Int): Iterable[TX]

}
