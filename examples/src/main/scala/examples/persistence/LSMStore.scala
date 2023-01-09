package examples.persistence

import java.io._
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.Level
import java.util.{Comparator, NoSuchElementException}
import com.google.common.collect.Iterators
import examples.persistence.LSMStore.{ShardLayout, isJournalFile, journalFileToNum}
import examples.persistence.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


protected[persistence] object LSMStore {

  val fileJournalPrefix = "journal"
  val fileShardPrefix = "shard-"
  val updateHeaderSize = +8 + 4 + 4 + 4 + 4 + 4 + 1

  val shardLayoutLog = "shardLayoutLog"

  type ShardLayout = util.TreeMap[K, List[LogFileUpdate]]


  protected[persistence] def isJournalFile(f: File): Boolean =
    f.getName.matches(LSMStore.fileJournalPrefix + "-[0-9]+")

  protected[persistence] def journalFileToNum(f: File): Long =
    f.getName().substring(LSMStore.fileJournalPrefix.size).toLong

}

/**
  * Log-Structured Merge Store
  */

@SuppressWarnings(
  Array(
    "org.wartremover.warts.TraversableOps",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.PublicInference",
    "org.wartremover.warts.OptionPartial"
  )
)
class LSMStore(
                val dir: File,
                val keySize: Int = 32,
                val executor: Executor = Executors.newCachedThreadPool(),
                val taskSchedulerDisabled: Boolean = false,
                val maxJournalEntryCount: Int = 100000,
                val maxShardUnmergedCount: Int = 4,
                val splitSize: Int = 100 * 1024,
                val keepVersions: Int = 0,
                val maxFileSize: Long = 64 * 1024 * 1024,

                val fileAccess: FileAccess = FileAccess.SAFE
              ) extends Store {


  val lock = new ReentrantReadWriteLock()

  protected[persistence] var journalNotDistributed: List[LogFileUpdate] = Nil
  protected[persistence] val journalCache = new util.TreeMap[K, V]
  //  protected[persistence] var journalLastVersionID: Option[VersionID] = None

  protected[persistence] def journalLastVersionID: Option[VersionID] = journalRollback.headOption.map(_.versionID)

  protected[persistence] def journalCurrentFileNum(): Long = fileHandles.keySet
    .filter(_ < 0).reduceOption(_ min _).getOrElse(-1L)

  //TODO mutable buffer, or queue?
  protected[persistence] var journalRollback: List[LogFileUpdate] = Nil

  protected[persistence] val fileHandles = mutable.HashMap[Long, Any]()

  protected[persistence] var shards = new ShardLayout()

  protected[persistence] val shardRollback =
    new mutable.LinkedHashMap[VersionID, ShardLayout]

  {
    assert(maxFileSize < 1024 * 1024 * 1024, "maximal file size must be < 1GB")

    //some initialization logic
    if (!dir.exists() || !dir.isDirectory)
      throw new IllegalArgumentException("dir does not exist, or is not an directory")

    // load all shard linked lists
    val shardNums = listFiles()
      .filter(isShardFile(_))
      .map(shardFileToNum(_))

    val journalNumbers = listFiles()
      .filter(isJournalFile(_))
      .map(journalFileToNum(_)) //add file number
      .sorted

    //open journal file handles
    journalNumbers.foreach { p =>
      val f = numToFile(p)
      fileHandles(p) = fileAccess.open(f.getPath)
    }

    //open shard file handles
    shardNums.foreach { fileNum =>
      val f = numToFile(fileNum)
      fileHandles(fileNum) = fileAccess.open(f.getPath)
    }

    //replay Shard Layout Log and restore shards
    val slogFile = new File(dir, LSMStore.shardLayoutLog)
    if (slogFile.exists()) {
      val slogInF = new FileInputStream(slogFile)
      val slogIn = new DataInputStream(new BufferedInputStream(slogInF))

      val slog = new ArrayBuffer[ShardSpec]()
      while (slogIn.available() > 0) {
        slog += deserializeShardSpec(slogIn)
      }
      slogIn.close()

      //restore shard layouts
      for (s <- slog;
           //check all files for this shard exists
           if (s.shards.map(_.fileNum).forall(fileHandles.contains(_)))
      ) {
        val t = new ShardLayout()
        for (e <- s.shards) {
          val updates = loadUpdates(numToFile(e.fileNum), e.fileNum)
          val linked = linkTogether(versionID = e.versionID, updates)
          t.put(e.startKey, linked)
        }
        shardRollback.put(s.versionID, t)
      }


      if (shardRollback.nonEmpty)
        shards = shardRollback.last._2.clone().asInstanceOf[ShardLayout]
    }

    val j = loadJournal(journalNumbers.sorted.reverse)
    j.headOption.foreach { h => assert(h.versionID != h.prevVersionID) }

    //find newest entry in journal present which is also present in shard
    //and cut journal, so it does not contain old entries
    journalNotDistributed = j.takeWhile(u => !shardRollback.contains(u.versionID)).toList
    //restore journal cache
    keyValues(journalNotDistributed, dropTombstones = false)
      .foreach(a => journalCache.put(a._1, a._2))

    journalRollback = j.toList

    if (journalRollback.nonEmpty)
      taskCleanup(deleteFiles = false)
  }


  /** read files and reconstructs linked list of updates */
  protected[persistence] def loadJournal(files: Iterable[Long]): mutable.Buffer[LogFileUpdate] = {
    //read files into structure
    val updates = files
      .flatMap(fileNum => loadUpdates(file = numToFile(fileNum), fileNum = fileNum))
      .toSeq

    //add some indexes
    val cur = updates
      .filter(u => u.versionID != u.prevVersionID) //do not include marker entries into index
      .map(u => (u.versionID, u)).toMap

    //start from last update and reconstruct list
    val list = new ArrayBuffer[LogFileUpdate]
    var b = updates.lastOption.map(u => cur(u.versionID))
    while (b.isDefined) {
      list += b.get
      b = cur.get(b.get.prevVersionID)
    }

    list
  }

  protected[persistence] def journalDeleteFilesButNewest(): Unit = {
    val knownNums = journalNotDistributed.map(_.fileNum).toSet
    val filesToDelete = journalListSortedFiles()
      .drop(1) //do not delete first file
      .filterNot(f => knownNums.contains(journalFileToNum(f))) //do not delete known files
    filesToDelete.foreach { f =>
      val deleted = f.delete()
      assert(deleted)
    }
  }

  /** creates new journal file, if no file is opened */
  protected def initJournal(): Unit = {
    assert(lock.isWriteLockedByCurrentThread)
    if (journalNotDistributed != Nil)
      return;

    //create new journal
    journalStartNewFile()
    journalNotDistributed = Nil
  }

  protected[persistence] def journalStartNewFile(): Long = {
    assert(lock.isWriteLockedByCurrentThread)
    val fileNum = journalCurrentFileNum() - 1L
    val journalFile = numToFile(fileNum)
    assert(!journalFile.exists())

    fileNum
  }

  protected[persistence] def loadUpdates(file: File, fileNum: Long): Iterable[LogFileUpdate] = {
    val updates = new ArrayBuffer[LogFileUpdate]()

    val fin = new FileInputStream(file)
    val din = new DataInputStream(new BufferedInputStream(fin))
    var offset = 0

    while (offset < fin.getChannel.size()) {
      //verify checksum
      val checksum = din.readLong()

      //read data
      val updateSize = din.readInt()
      val data = new Array[Byte](updateSize)
      val buf = ByteBuffer.wrap(data)
      din.read(data, 8 + 4, updateSize - 8 - 4)
      //put data size back so checksum is preserved
      buf.putInt(8, updateSize)

      //verify checksum
      if (checksum != Utils.checksum(data)) {
        throw new DataCorruptionException("Wrong data checksum")
      }

      //read data from byte buffer
      buf.position(12)
      val keyCount = buf.getInt()
      val keySize2 = buf.getInt()
      assert(keySize2 == keySize)
      val versionIDSize = buf.getInt()
      val prevVersionIDSize = buf.getInt()

      val versionID = new ByteArrayWrapper(versionIDSize)
      val prevVersionID = new ByteArrayWrapper(prevVersionIDSize)
      val isMerged = buf.get() == 1

      val verPos = LSMStore.updateHeaderSize + keySize * keyCount + 8 * keyCount
      if (versionIDSize > 0)
        System.arraycopy(data, verPos, versionID.data, 0, versionIDSize)
      if (prevVersionIDSize > 0)
        System.arraycopy(data, verPos + versionIDSize, prevVersionID.data, 0, prevVersionIDSize)

      updates += LogFileUpdate(
        versionID = versionID,
        prevVersionID = prevVersionID,
        merged = isMerged,
        fileNum = fileNum,
        offset = offset,
        keyCount = keyCount)
      offset += updateSize
    }
    din.close()
    updates
  }


  protected[persistence] def numToFile(n: Long): File = {
    val prefix = if (n < 0) LSMStore.fileJournalPrefix else LSMStore.fileShardPrefix
    new File(dir, prefix + n)
  }

  protected[persistence] def journalListSortedFiles(): Seq[File] = {
    listFiles()
      .filter(isJournalFile(_))
      .sortBy(journalFileToNum(_))
  }

  protected[persistence] def shardFileToNum(f: File): Long =
    f.getName().substring(LSMStore.fileShardPrefix.size).toLong


  protected[persistence] def isShardFile(f: File): Boolean =
    f.getName.matches(LSMStore.fileShardPrefix + "[0-9]+")

  protected def listFiles(): Array[File] = {
    var files: Array[File] = dir.listFiles()
    if (files == null)
      files = new Array[File](0)
    files
  }

  protected[persistence] def shardListSortedFiles(): Seq[File] = {
    listFiles()
      .filter(isShardFile(_))
      .sortBy(shardFileToNum(_))
      .reverse
  }

  protected[persistence] def shardNewFileNum(): Long = shardListSortedFiles()
    .headOption.map(shardFileToNum(_))
    .getOrElse(0L) + 1


  /** expand shard tails into linked lists */
  protected[persistence] def linkTogether(versionID: VersionID, updates: Iterable[LogFileUpdate]): List[LogFileUpdate] = {
    //index to find previous versions
    val m = updates.map { u => (u.versionID, u) }.toMap

    var ret = new ArrayBuffer[LogFileUpdate]()
    var r = m.get(versionID)
    while (r.isDefined) {
      ret += r.get
      r = m.get(r.get.prevVersionID)
    }

    ret.toList
  }

  protected[persistence] def createEmptyShard(): Long = {
    val fileNum = shardNewFileNum()
    val shardFile = new File(dir, LSMStore.fileShardPrefix + fileNum)
    assert(!shardFile.exists())
    fileNum
  }

  def versionIDExists(versionID: VersionID): Boolean = {
    //TODO traverse all files, not just open files
    if (journalRollback.exists(u => u.versionID == versionID || u.prevVersionID == versionID)) true
    else if (shards.values().asScala.flatten.exists(u => u.versionID == versionID || u.prevVersionID == versionID)) true
    else false
  }

  def update(
              versionID: VersionID,
              toRemove: Iterable[K],
              toUpdate: Iterable[(K, V)]
            ): Unit = {

    //produce sorted and merged data set
    val data = new mutable.TreeMap[K, V]()
    for (key <- toRemove) {
      val old = data.put(key, Store.tombstone)
      if (old.isDefined)
        throw new IllegalArgumentException("duplicate key in `toRemove`")
    }
    for ((key, value) <- toUpdate) {
      val old = data.put(key, value)
      if (old.isDefined)
        throw new IllegalArgumentException("duplicate key in `toUpdate`")
    }


    lock.writeLock().lock()
    try {
      if (versionIDExists(versionID))
        throw new IllegalArgumentException("versionID is already used")

      initJournal()

      //last version from journal
      val prevVersionID = journalLastVersionID.getOrElse(new ByteArrayWrapper(0))

      var journalFileNum = journalCurrentFileNum()
      //if journal file is too big, start new file
      if (numToFile(journalFileNum).length() > maxFileSize) {
        //start a new file
        //        fileOuts.remove(journalFileNum).get.close()
        journalFileNum = journalStartNewFile()
      }

      val updateEntry = updateAppend(
        fileNum = journalFileNum,
        data = data,
        versionID = versionID,
        prevVersionID = prevVersionID,
        merged = false)

      journalNotDistributed = updateEntry :: journalNotDistributed
      journalRollback = updateEntry :: journalRollback

      //update journal cache
      for (key <- toRemove) {
        journalCache.put(key, Store.tombstone)
      }
      for ((key, value) <- toUpdate) {
        journalCache.put(key, value)
      }

      //TODO if write fails, how to recover from this? close journal.out?

      if (journalCache.size() > maxJournalEntryCount) {
        //run sharding task
        taskRun {
          taskDistribute()
        }
      }
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[persistence] def updateAppend(
                                    fileNum: Long,
                                    data: Iterable[(K, V)],
                                    versionID: VersionID,
                                    prevVersionID: V,
                                    merged: Boolean
                                  ): LogFileUpdate = {

    //insert new Update into journal
    val updateData = serializeUpdate(
      versionID = versionID,
      prevVersionID = prevVersionID,
      data = data,
      isMerged = merged)

    var updateOffset = 0L
    val out = new FileOutputStream(numToFile(fileNum), true)
    try {
      updateOffset = out.getChannel.position()
      //TODO assert fileOffset is equal to end of linked list of updates
      out.write(updateData)
      out.getFD.sync()
    } finally {
      out.close()
    }

    //update file size
    val oldHandle = fileHandles.getOrElse(fileNum, null)
    val newHandle: Any =
      if (oldHandle == null) fileAccess.open(numToFile(fileNum).getPath)
      else fileAccess.expandFileSize(oldHandle)

    if (oldHandle != newHandle) {
      fileHandles.put(fileNum, newHandle)
    }
    //append new entry to journal
    LogFileUpdate(
      offset = updateOffset.toInt,
      keyCount = data.size,
      merged = merged,
      fileNum = fileNum,
      versionID = versionID,
      prevVersionID = prevVersionID
    )
  }

  def taskRun(f: => Unit) {
    if (taskSchedulerDisabled)
      return // task should be triggered by user
    if (executor == null) {
      // execute in foreground
      f
      return
    }
    //send task to executor for background execution
    val runnable = new Runnable() {
      def run() = {
        try {
          f
        } catch {
          case e: Throwable => {
            Utils.LOG.log(Level.SEVERE, "Background task failed", e)
          }
        }
      }
    }
    executor.execute(runnable)
  }


  protected[persistence] def serializeShardSpec(
                                          versionID: VersionID,
                                          shards: Seq[(K, Long, VersionID)]
                                        ): Array[Byte] = {

    assert(shards.nonEmpty)
    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for `checksum` and `update size`
    out2.writeLong(0)
    out2.writeInt(0)

    out2.writeInt(shards.size)
    out2.writeInt(keySize)
    out2.writeInt(versionID.size)
    out2.write(versionID.data)

    for (s <- shards) {
      out2.write(s._1.data)
      out2.writeLong(s._2)
      //versionID
      out2.writeInt(s._3.size)
      out2.write(s._3.data)
    }

    //now write file size and checksum
    val ret = out.toByteArray
    val wrap = ByteBuffer.wrap(ret)
    wrap.putInt(8, ret.size)
    val checksum = Utils.checksum(ret) - 1000 // 1000 is to distinguish different type of data
    wrap.putLong(0, checksum)
    ret
  }


  protected[persistence] def deserializeShardSpec(in: DataInputStream): ShardSpec = {
    val checksum = in.readLong()
    val updateSize = in.readInt()
    val b = new Array[Byte](updateSize)
    in.read(b, 12, updateSize - 12)
    //restore size
    Utils.putInt(b, 8, updateSize)
    //validate checksum
    if (Utils.checksum(b) - 1000 != checksum) {
      throw new DataCorruptionException("Wrong data checksum")
    }

    val in2 = new DataInputStream(new ByteArrayInputStream(b))
    in2.readLong() //skip checksum
    in2.readInt()
    // skip updateSize
    val keyCount = in2.readInt()
    assert(keySize == in2.readInt())

    val versionID = new VersionID(in2.readInt())
    in2.read(versionID.data)
    //read table
    val keyFileNums = (0 until keyCount).map { index =>
      val key = new K(keySize)
      in2.read(key.data)
      val fileNum = in2.readLong()
      val versionID = new ByteArrayWrapper(in2.readInt())
      in2.read(versionID.data)

      (key, fileNum, versionID)
    }

    val specs = (0 until keyCount).map { i =>
      ShardSpecEntry(
        startKey = keyFileNums(i)._1,
        endKey = if (i + 1 == keyCount) null else keyFileNums(i + 1)._1,
        fileNum = keyFileNums(i)._2,
        versionID = keyFileNums(i)._3
      )
    }

    ShardSpec(versionID = versionID, shards = specs)
  }

  protected[persistence] def serializeUpdate(
                                       versionID: VersionID,
                                       prevVersionID: VersionID,
                                       data: Iterable[(K, V)],
                                       isMerged: Boolean
                                     ): Array[Byte] = {

    //TODO check total size is <2GB

    //check for key size
    assert((versionID == tombstone && prevVersionID == tombstone) ||
      data.forall(_._1.size == keySize), "wrong key size")

    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for `checksum` and `update size`
    out2.writeLong(0)
    out2.writeInt(0)

    //basic data
    out2.writeInt(data.size) // number of keys
    out2.writeInt(keySize) // key size

    //versions
    out2.writeInt(versionID.size)
    out2.writeInt(prevVersionID.size)

    out2.writeBoolean(isMerged) //is merged

    //write keys
    data.map(_._1.data).foreach(out2.write(_))

    var valueOffset = out.size() + data.size * 8 + versionID.size + prevVersionID.size

    //write value sizes and their offsets
    data.foreach { t =>
      val value = t._2
      if (value == Store.tombstone) {
        //tombstone
        out2.writeInt(-1)
        out2.writeInt(0)
      } else {
        //actual data
        out2.writeInt(value.size)
        out2.writeInt(valueOffset)
        valueOffset += value.size
      }
    }

    out2.write(versionID.data)
    out2.write(prevVersionID.data)

    //write values
    data.foreach { t =>
      if (t._2 != null) //filter out tombstones
        out2.write(t._2.data)
    }

    //write checksum and size at beginning of byte[]
    val ret = out.toByteArray
    val wrap = ByteBuffer.wrap(ret)
    wrap.putInt(8, ret.size)
    wrap.putLong(0, Utils.checksum(ret))
    ret
  }

  override def lastVersionID: Option[VersionID] = {
    lock.readLock().lock()
    try {
      journalLastVersionID
    } finally {
      lock.readLock().unlock()
    }
  }


  def getUpdates(key: K, logFile: List[LogFileUpdate]): Option[V] = {
    var updates = logFile
    while (updates != null && updates.nonEmpty) {
      //run binary search on current item
      val value = fileAccess.getValue(
        fileHandle = fileHandles(updates.head.fileNum),
        key = key,
        keySize = keySize,
        updateOffset = updates.head.offset)
      if (value != null)
        return value

      if (updates.head.merged)
        return null // this update is merged, no need to continue traversal

      updates = updates.tail
    }

    return null
  }

  def get(key: K): Option[V] = {
    lock.readLock().lock()
    try {
      val ret2 = journalCache.get(key)
      if (ret2 eq tombstone)
        return None
      if (ret2 != null)
        return Some(ret2)

      //      var ret = getUpdates(key, journal)
      //      if(ret!=null)
      //        return ret

      //not found in journal, look at shards
      val shardEntry = shards.floorEntry(key)
      if (shardEntry == null)
        return None // shards not initialized yet

      val ret = getUpdates(key, shardEntry.getValue)
      if (ret != null)
        return ret

      // not found
      None
    } finally {
      lock.readLock().unlock()
    }
  }

  /**
    * Task ran periodically in background thread.
    * It distributes content of Journal between Shards.
    */
  def taskDistribute(): Unit = {
    lock.writeLock().lock()
    try {
      val versionID = lastVersionID.getOrElse(null)
      if (versionID == null)
        return // empty store
      if (journalCache.isEmpty)
        return

      Utils.LOG.log(Level.FINE, "Run Sharding Task for " + journalCache.size() + " keys.")

      if (shards.isEmpty && !journalCache.isEmpty) {
        //add lowest possible key for given size
        shards.put(new ByteArrayWrapper(keySize), Nil)
      }

      (shards.keySet().asScala ++ List(null)).reduce { (shardKey, hiKey) =>

        val entries =
          if (hiKey == null) journalCache.tailMap(shardKey, true)
          else journalCache.subMap(shardKey, true, hiKey, false)

        if (!entries.isEmpty) {
          var logFile = shards.get(shardKey)
          val fileNum: Long =
            if (logFile.isEmpty) createEmptyShard()
            else logFile.head.fileNum

          //insert new update into Shard
          val updateEntry = updateAppend(fileNum,
            data = entries.asScala,
            merged = logFile.isEmpty,
            versionID = versionID,
            prevVersionID = logFile.headOption.map(_.versionID).getOrElse(tombstone)
          )
          logFile = updateEntry :: logFile
          shards.put(shardKey, logFile)

          //schedule merge on this Shard, if it has enough unmerged changes
          val unmergedCount = logFile.takeWhile(!_.merged).size
          if (unmergedCount > maxShardUnmergedCount)
            taskRun {
              taskShardMerge(shardKey)
            }
        }
        hiKey
      }

      journalCache.clear()
      journalNotDistributed = Nil

      //backup current shard layout
      shardRollback.put(versionID, shards.clone().asInstanceOf[ShardLayout])

      val shardLayoutFile = new File(dir, LSMStore.shardLayoutLog)
      val shardLayoutLog = new FileOutputStream(shardLayoutFile, true)

      try {
        //update shard layout log
        //write first entry into shard layout log
        val data = serializeShardSpec(versionID = versionID,
          shards = shards.asScala.map(u => (u._1, u._2.head.fileNum, u._2.head.versionID)).toSeq)
        shardLayoutLog.write(data)
        shardLayoutLog.getFD.sync()
      } finally {
        shardLayoutLog.close()
      }
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }


  // takes first N items from iterator,
  // Iterator.take() can not be used, read scaladoc
  private def take(count: Int, iter: Iterator[(K, V)]): Iterable[(K, V)] = {
    val ret = new ArrayBuffer[(K, V)]()
    var counter = 0
    while (counter < count && iter.hasNext) {
      counter += 1
      ret += iter.next()
    }
    ret
  }


  def taskShardMerge(shardKey: K): Unit = {
    lock.writeLock().lock()
    try {
      val shard = shards.get(shardKey)
      if (shard == null)
        return // shard not present

      if (shard.head.merged)
        return // shard is empty or already merged

      Utils.LOG.log(Level.FINE, "Starting sharding for shard: " + shardKey)

      //iterator over merged data set
      val b = keyValues(shard, dropTombstones = true).toBuffer
      val merged = b.iterator

      // decide if this will be put into single shard, or will be split between multiple shards
      // Load on-heap enough elements to fill single shard
      var mergedFirstShard = take(splitSize, merged)

      // Decide if all data go into single shard.
      if (!merged.hasNext) {
        // Data are small enough, keep everything in single shard
        val updateEntry = updateAppend(
          fileNum = createEmptyShard(),
          versionID = shard.head.versionID,
          prevVersionID = Store.tombstone,
          data = mergedFirstShard,
          merged = true)
        shards.put(shardKey, List(updateEntry))
        Utils.LOG.log(Level.FINE, "Sharding finished into single shard")
      } else {
        // split data into multiple shards
        val mergedBuf: BufferedIterator[(K, V)] = (mergedFirstShard.iterator ++ merged).buffered
        mergedFirstShard = null
        //release for GC
        val splitSize2: Int = Math.max(1, (splitSize.toDouble * 0.33).toInt)
        val parentShardEndKey = shards.higherKey(shardKey)

        var startKey: K = shardKey
        var shardCounter = 0
        while (mergedBuf.hasNext) {
          val keyVals = take(splitSize2, mergedBuf)

          val endKey = if (mergedBuf.hasNext) mergedBuf.head._1 else parentShardEndKey
          assert(keyVals.map(_._1).forall(k => k >= startKey && (endKey == null || k < endKey)))

          assert(endKey == null || startKey < endKey)

          val updateEntry = updateAppend(
            fileNum = createEmptyShard(),
            versionID = shard.head.versionID,
            prevVersionID = Store.tombstone,
            data = keyVals,
            merged = true
          )

          shards.put(startKey, List(updateEntry))

          startKey = endKey
          shardCounter += 1
        }
        Utils.LOG.log(Level.FINE, "Sharding finished into " + shardCounter + " shards")
      }
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def getAll(consumer: (K, V) => Unit) = {
    lock.readLock().lock()
    try {
      //iterate over all shards, and add content not present in journal
      val shards2 = shards.values().asScala.toBuffer
      for (shard: List[LogFileUpdate] <- shards2;
           (k, v) <- keyValues(shard, dropTombstones = true)) {
        if (!journalCache.containsKey(k)) {
          consumer(k, v)
        }
      }
      //include journal cache
      for ((k, v) <- journalCache.asScala.filterNot(a => a._2 eq tombstone)) {
        consumer(k, v)
      }
    } finally {
      lock.readLock().unlock()
    }
  }

  override def clean(count: Int): Unit = {
    taskCleanup()
  }

  override def cleanStop(): Unit = {

  }

  override def rollback(versionID: VersionID): Unit = {
    def notFound() = throw new NoSuchElementException("versionID not found, can not rollback")

    lock.writeLock().lock()
    try {
      if (journalRollback.isEmpty)
        notFound()

      journalRollback.find(_.versionID == versionID).getOrElse(notFound())

      if (journalNotDistributed != Nil && journalNotDistributed.head.versionID == versionID)
        return

      //cut journal
      var notFound2 = true;
      var lastShard: ShardLayout = null
      journalRollback = journalRollback
        //find starting version
        .dropWhile { u =>
        notFound2 = u.versionID != versionID
        notFound2
      }
      //find end version, where journal was sharded, also restore Shard Layout in side effect
      val j = journalRollback.takeWhile { u =>
        lastShard = shardRollback.get(u.versionID).map(_.clone().asInstanceOf[ShardLayout]).orNull
        lastShard == null
      }

      if (notFound2)
        notFound()

      if (lastShard == null) {
        //Reached end of journalRollback, without finding shard layout
        //Check if journal log was not truncated
        if (journalRollback.last.prevVersionID != tombstone)
          notFound()
        shards.clear()
      } else {
        shards = lastShard
      }
      //rebuild journal cache
      journalCache.clear()

      keyValues(j, dropTombstones = false).foreach { e =>
        journalCache.put(e._1, e._2)
      }
      journalNotDistributed = j

      //insert new marker update to journal, so when reopened we start with this version
      val updateEntry = updateAppend(
        fileNum = journalCurrentFileNum(),
        data = Nil,
        versionID = versionID,
        prevVersionID = versionID,
        merged = false)

      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def close(): Unit = {
    lock.writeLock().lock()
    try {
      fileHandles.values.foreach(fileAccess.close(_))
      fileHandles.clear()
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }


  protected[persistence] def keyValues(fileLog: List[LogFileUpdate], dropTombstones: Boolean): Iterator[(K, V)] = {
    //assert that all updates except last are merged

    val iters = fileLog.zipWithIndex.map { a =>
      val u = a._1
      val index = a._2
      fileAccess.readKeyValues(fileHandle = fileHandles(u.fileNum),
        offset = u.offset, keySize = keySize)
        .map(p => (p._1, index, p._2)).asJava
    }.asJava.asInstanceOf[java.lang.Iterable[util.Iterator[(K, Int, V)]]]

    //merge multiple iterators, result iterator is sorted union of all iters
    var prevKey: K = null
    val iter = Iterators.mergeSorted[(K, Int, V)](iters, KeyOffsetValueComparator)
      .asScala
      .filter { it =>
        val include =
          (prevKey == null || //first key
            !prevKey.equals(it._1)) && //is first version of this key
            (!dropTombstones || it._3 != null) // only include if is not tombstone
        prevKey = it._1
        include
      }
      //drop the tombstones
      .filter(it => !dropTombstones || it._3 != Store.tombstone)
      //remove update
      .map(it => (it._1, it._3))

    iter
  }

  def verify(): Unit = {
    //verify shard boundaries
    (List(shards) ++ shardRollback.values).foreach { s =>
      s.asScala.foldRight(null.asInstanceOf[K]) { (e: Tuple2[K, List[LogFileUpdate]], endKey: K) =>
        val fileNum = e._2.head.fileNum
        assert(fileHandles.contains(fileNum))
        val updates = loadUpdates(file = numToFile(fileNum), fileNum = fileNum)

        e._1
      }
    }

    val existFiles =
      (listFiles().filter(isJournalFile).map(journalFileToNum) ++
        listFiles().filter(isShardFile).map(shardFileToNum)).toSet

    assert(existFiles == fileHandles.keySet)
  }


  def takeWhileExtra[A](iter: Iterable[A], f: A => Boolean): Iterable[A] = {
    val ret = new ArrayBuffer[A]
    val it = iter.iterator
    while (it.hasNext) {
      val x = it.next()
      ret += x
      if (!f(x))
        return ret.result()
    }
    ret.result()
  }

  /** closes and deletes files which are no longer needed */
  def taskCleanup(deleteFiles: Boolean = true): Unit = {
    lock.writeLock().lock()
    try {
      if (fileHandles.isEmpty)
        return
      //this updates will be preserved
      var index = Math.max(keepVersions, journalNotDistributed.size)
      //expand index until we find shard or reach end
      while (index < journalRollback.size && !shardRollback.contains(journalRollback(index).versionID)) {
        index += 1
      }
      index += 1

      journalRollback = journalRollback.take(index)

      //build set of files to preserve
      val journalPreserveNums: Set[Long] = (journalRollback.map(_.fileNum) ++ List(journalCurrentFileNum())).toSet
      assert(journalPreserveNums.contains(journalCurrentFileNum()))


      //shards to preserve
      val shardNumsToPreserve: Set[Long] =
        (List(shards) ++ journalRollback.flatMap(u => shardRollback.get(u.versionID)))
          .flatMap(_.values().asScala)
          .flatMap(_.map(_.fileNum))
          .toSet

      val numsToPreserve = (journalPreserveNums ++ shardNumsToPreserve)
      assert(numsToPreserve.forall(fileHandles.contains(_)))

      val deleteNums = (fileHandles.keySet diff numsToPreserve)

      //delete unused journal files
      for (num <- deleteNums) {
        fileAccess.close(fileHandles.remove(num).get)
        if (deleteFiles) {
          val f = numToFile(num)
          assert(f.exists())
          val deleted = f.delete()
          assert(deleted)
        }
      }

      //cut unused shards
      val versionsInJournalRollback = journalRollback.map(_.versionID).toSet
      (shardRollback.keySet diff versionsInJournalRollback).foreach(shardRollback.remove(_))

      //      if(!shardRollback.isEmpty)
      //        journalRollback.lastOption.foreach(u=>assert(shardRollback.contains(u.versionID)))

    } finally {
      lock.writeLock().unlock()
    }
  }

  def rollbackVersions(): Iterable[VersionID] = journalRollback.map(_.versionID)

}

/** Compares key-value pairs. Key is used for comparation, value is ignored */
protected[persistence] object KeyOffsetValueComparator extends Comparator[(K, Int, V)] {
  def compare(o1: (K, Int, V),
              o2: (K, Int, V)): Int = {
    //compare by key
    var c = o1._1.compareTo(o2._1)
    //compare by index, higher index means newer entry
    if (c == 0)
      c = o1._2.compareTo(o2._2)
    c
  }
}


//TODO this consumes too much memory, should be represented using single primitive long[]
case class LogFileUpdate(
                          offset: Int,
                          keyCount: Int,
                          merged: Boolean,
                          fileNum: Long,
                          versionID: VersionID,
                          prevVersionID: VersionID
                        ) {
}

case class ShardSpec(versionID: VersionID, shards: Seq[ShardSpecEntry])

case class ShardSpecEntry(startKey: K, endKey: K, fileNum: Long, versionID: VersionID) {
  assert((endKey == null && startKey != null) || startKey < endKey)
}