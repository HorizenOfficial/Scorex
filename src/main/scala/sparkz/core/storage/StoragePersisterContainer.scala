package sparkz.core.storage

import sparkz.core.storage.StoragePersisterContainer.StoragePersisterContainerConfig

import java.io.File
import java.nio.file.Files

class StoragePersisterContainer(
                                 private val scheduledStoragePersister: Seq[ScheduledStoragePersister[_]],
                                 private val config: StoragePersisterContainerConfig
                               ) {
  initDirectoryForAllStorages()

  private def initDirectoryForAllStorages(): Unit = {
    Files.createDirectories(config.directoryPath.toPath)
  }

  def restoreAllStorages(): Unit = scheduledStoragePersister.foreach(persister => persister.restore())
}

object StoragePersisterContainer {
  case class StoragePersisterContainerConfig(directoryPath: File)
}