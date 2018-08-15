package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.partition._
import geotrellis.spark.tiling._
import geotrellis.proj4._

import org.apache.spark._
import org.apache.spark.rdd._


object RasterSourceRDD {
  final val PARTITION_BYTES: Long = 64l * 1024 * 1024

  def apply(
    sources: Seq[RasterSource],
    layout: LayoutDefinition,
    partitionBytes: Long = PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }

    val cellType = {
      val cellTypes = sources.map { source => Set(source.cellType) }.reduce { _ ++ _ }

      if (cellTypes.size > 1)
        throw new Exception(s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")
      else
        cellTypes.head
    }

    val crs = {
      val projections = sources.map { source => Set(source.crs) }.reduce { _ ++ _ }

      if (projections.size > 1)
        throw new Exception(s"All RasterSources must be in the same projection, but multiple ones were found: $projections")
      else
        projections.head
    }

    val layerKeyBounds = {
      val bounds =
        sources.map { source =>
          val projectedExtent = ProjectedExtent(source.extent, source.crs)
          val boundsKey = projectedExtent.translate(SpatialKey(0, 0))
          KeyBounds(boundsKey, boundsKey)
        }.reduce { _ combine _ }

      bounds.setSpatialBounds(KeyBounds(mapTransform(combinedExtents)))
    }

    val layerMetadata =
      TileLayerMetadata[SpatialKey](cellType, layout, combinedExtents, crs, layerKeyBounds)

    val sourcesRDD: RDD[(RasterSource, Array[RasterExtent])] =
      sc.parallelize(sources).flatMap { source =>
        val rasterExtents: Traversable[RasterExtent] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              val keys = mapTransform.keysForGeometry(intersection.toPolygon)

              keys.map { key => RasterExtent(mapTransform(key), layout.tileCols, layout.tileRows) }
            case None => Seq[RasterExtent]()
          }

        if (rasterExtents.isEmpty)
          Seq((source, rasterExtents.toArray))
        else {
          val totalRasterExtents = rasterExtents.size * source.bandCount

          val maxPartitionBytes =
            partitionBytes / math.max(source.cellType.bytes * totalRasterExtents, totalRasterExtents)

          RasterExtentPartitioner
            .partitionRasterExtents(rasterExtents, maxPartitionBytes)
            .map { res => (source, res) }
        }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size)
        sourcesRDD.repartition(count)
      else
        sourcesRDD
    }

    val result: RDD[(SpatialKey, MultibandTile)] =
      repartitioned.flatMap { case (source, rasterExtents) =>
        source.read(rasterExtents).map { raster =>
          val center = raster.extent.center
          val key = mapTransform.pointToKey(center)

          (key, raster.tile)
        }
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def apply(source: RasterSource, layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    apply(Seq(source), layout)
}
