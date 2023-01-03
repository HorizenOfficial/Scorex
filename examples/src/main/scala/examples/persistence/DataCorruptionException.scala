package examples.persistence

/**
  * Exception if data in files are corrupted
  */
class DataCorruptionException(msg: String)
  extends RuntimeException(msg) {

}
