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
