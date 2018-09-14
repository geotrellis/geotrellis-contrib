/*
 * Copyright 2018 Azavea
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

import geotrellis.raster.RasterExtent

import scala.collection.mutable.ArrayBuilder

private[vlm] object RasterExtentPartitioner {
  def partitionRasterExtents(
    rasterExtents: Traversable[RasterExtent],
    maxPartitionSize: Long
  ): Array[Array[RasterExtent]] =
    if (rasterExtents.isEmpty)
      Array[Array[RasterExtent]]()
    else {
      val partition = ArrayBuilder.make[RasterExtent]
      partition.sizeHintBounded(128, rasterExtents)
      var partitionSize: Long = 0l
      var partitionCount: Long = 0l
      val partitions = ArrayBuilder.make[Array[RasterExtent]]

      def finalizePartition() {
        val res = partition.result
        if (res.nonEmpty) partitions += res
        partition.clear()
        partitionSize = 0l
        partitionCount = 0l
      }

      def addToPartition(rasterExtent: RasterExtent) {
        partition += rasterExtent
        partitionSize += rasterExtent.size
        partitionCount += 1
      }

      for (rasterExtent <- rasterExtents) {
        if ((partitionCount == 0) || (partitionSize + rasterExtent.size) < maxPartitionSize)
          addToPartition(rasterExtent)
        else {
          finalizePartition()
          addToPartition(rasterExtent)
        }
      }

      finalizePartition()
      partitions.result
    }
}
