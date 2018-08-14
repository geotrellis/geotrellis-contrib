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
  def apply(
    sources: Seq[RasterSource],
    layout: LayoutDefinition
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }
    val cellType = sources.map { source => Set(source.cellType) }.reduce { _ ++ _ }.head
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

    val partitioner = SpacePartitioner[SpatialKey](layerKeyBounds)

    val sourcesRDD: RDD[(SpatialKey, RasterSource)] =
      sc.parallelize(sources).flatMap { source =>
        val keys: Set[SpatialKey] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              mapTransform.keysForGeometry(intersection.toPolygon)
            case None => Set[SpatialKey]()
          }
        keys.map { key => (key, source) }
      }

    val groupedRDD: RDD[(SpatialKey, Iterable[RasterSource])] =
      sourcesRDD.groupByKey(partitioner)

    val keyedRDD: RDD[(SpatialKey, MultibandTile)] =
      groupedRDD.flatMap { case (key, sources) =>
        val keyExtent = mapTransform(key)
        val rasterExtent = RasterExtent(keyExtent, layout.tileCols, layout.tileRows)

        sources.flatMap { source =>
          source.read(Seq(rasterExtent)).map { raster =>
            val center = raster.extent.center
            val k = mapTransform.pointToKey(center)

            (k, raster.tile)
          }
        }
      }

    ContextRDD(keyedRDD, layerMetadata)
  }


  def apply(source: RasterSource, layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    apply(Seq(source), layout)
}
