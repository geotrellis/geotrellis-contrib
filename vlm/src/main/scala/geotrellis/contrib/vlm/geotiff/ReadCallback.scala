/*
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.geotiff
import com.typesafe.scalalogging.LazyLogging
import geotrellis.util.RangeReader

/** Trait for registering a callback for logging or monitoring range reads.
  * NB: The callback may be invoked from within a Spark task, and therefore
  * is serialized along with its closure to executors. */
trait ReadCallback extends Serializable {
  def readRange(start: Long, length: Int): Unit
}
object ReadCallback extends LazyLogging {
  case class ReadLogger(label: String) extends ReadCallback {
    override def readRange(start: Long, length: Int): Unit =
      logger.info(s"Read $length bytes ($start to ${start + length}) from $label")
  }

  case class CallbackEnabledRangeReader(delegate: RangeReader, callback: ReadCallback) extends RangeReader {
    override def totalLength: Long = delegate.totalLength
    override protected def readClippedRange(start: Long, length: Int): Array[Byte] = {
      callback.readRange(start, length)
      delegate.readRange(start, length)
    }
  }

  def loggingRangeReader(label: String, delegate: RangeReader): RangeReader =
    CallbackEnabledRangeReader(delegate, ReadLogger(label))
}