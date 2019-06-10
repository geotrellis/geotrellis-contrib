package geotrellis.contrib.vlm

import geotrellis.spark.io.hadoop.HdfsRangeReader

import org.scalatest._

import java.net.ConnectException

class VlmByteReaderSpec extends FlatSpec with Matchers {
  it should "summon an hdfs rangereader" in {
    // If it gets as far as attempting (and failing) a connection, it created a bytereader
    assertThrows[ConnectException] {
      getByteReader("hdfs://localhost:7777/file123").asInstanceOf[HdfsRangeReader]
    }
  }
}
