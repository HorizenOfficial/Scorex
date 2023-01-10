package sparkz.core.persistence

import sparkz.core.network.peer.BucketManager.PeerBucketValue
import sparkz.core.network.peer.PeerBucketStorage
import sparkz.core.persistence.StorageFilePersister.StorageFilePersisterConfig

import java.io._
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * This abstract class represents an actor that is responsible to persist and recover data
  *
  * @tparam D The type of the data stored
  */
abstract class StoragePersister[D] {

  /**
    * Custom method that returns the data to persist as byte array
    *
    * @return The data to be stored as byte array
    */
  protected def getDataToPersist: Array[Byte]

  /**
    * Custom method that returns the specific output stream the class wants to use to write the data
    *
    * @return The output stream to write the data
    */
  protected def getOutputStream: OutputStream

  /**
    * Custom method to retrieve data to restore from the specified source
    *
    * @return The stored data as an array of bytes
    */
  protected def tryGetSourceDataToRestore: Try[Array[Byte]]

  /**
    * Custom method to restore the data into the data structure
    */
  def restore(): Unit

  /**
    * Persist data using the custom output stream
    */
  def persist(): Unit = {
    val dataToPersist: Array[Byte] = getDataToPersist
    val bos = new BufferedOutputStream(getOutputStream)
    bos.write(dataToPersist)
    bos.close()
  }

  /**
    * Retrieve the data previously stored and return them
    *
    * @return The data previously stored as an Option
    */
  def recoverStoredData: Option[D] = {
    tryGetSourceDataToRestore match {
      case Success(bytes) =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val value = ois.readObject
        ois.close()
        Some(value.asInstanceOf[D])
      case Failure(_) => None
    }
  }
}

/**
  * This abstract class writes and reads from file
  *
  * @param config The configuration
  * @tparam T The type of the data structure holding the data to persist
  * @tparam D The type of the data stored
  */
abstract class StorageFilePersister[T, D](config: StorageFilePersisterConfig)
  extends StoragePersister[D] {

  private val absolutePath = Paths.get(config.directoryPath.getPath, config.fileName)

  override protected def getOutputStream: OutputStream = new FileOutputStream(absolutePath.toFile)

  override protected def tryGetSourceDataToRestore: Try[Array[Byte]] = Try(Files.readAllBytes(absolutePath))
}

object StorageFilePersister {
  case class StorageFilePersisterConfig(directoryPath: File, fileName: String)
}

/**
  * This class is responsible to persist any PeerBucketStorage
  *
  * @param peerBucketStorage The PeerBucketStorage data structure to backup
  * @param config            The configuration
  * @tparam T The type of the data structure holding the data to persist
  */
class PeerBucketPersister[T <: PeerBucketStorage[_]](
                                                      private val peerBucketStorage: T,
                                                      private val config: StorageFilePersisterConfig
                                                    )
  extends StorageFilePersister[T, Seq[PeerBucketValue]](config) {

  override protected def getDataToPersist: Array[Byte] = {
    val peers = peerBucketStorage.getPeersAsBucketValue

    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(peers)
    oos.close()
    stream.toByteArray
  }

  override def restore(): Unit = {
    val storedData = recoverStoredData
    storedData match {
      case Some(peersList) => peersList.foreach(peer => peerBucketStorage.add(peer))
      case None =>
    }
  }
}

/**
  * This class is responsible to persist any Map
  *
  * @param storage The map data structure to backup
  * @param config  The configuration
  * @tparam K The type of the map's keys
  * @tparam V The type of the map's values
  */
class MapPersister[K, V](
                          storage: mutable.Map[K, V],
                          config: StorageFilePersisterConfig
                        )
  extends StorageFilePersister[mutable.Map[K, V], mutable.Map[K, V]](config) {

  override protected def getDataToPersist: Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(storage)
    oos.close()
    stream.toByteArray
  }

  override def restore(): Unit = {
    val storedData = recoverStoredData
    storedData match {
      case Some(entries) => entries.foreach(entry => storage += entry._1 -> entry._2)
      case None =>
    }
  }
}