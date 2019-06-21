/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
