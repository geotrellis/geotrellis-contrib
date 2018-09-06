package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.gdal._

import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.merge._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.scalatest._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.io.File


class RasterSourceRDDSpec extends FunSpec with TestEnvironment {
  val uri = s"file://${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val rasterSource = new GeoTiffRasterSource(uri)

  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  val layout = scheme.levelForZoom(13).layout

  /*
  describe("reading in GeoTiffs as RDDs using GeoTiffRasterSource") {
    val uri = s"file://${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val rasterSource = new GeoTiffRasterSource(uri)

    it("should have the right number of tiles") {
      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(rasterSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }

      val rdd = RasterSourceRDD(rasterSource, layout)

      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }

      for ((actual, expected) <- actualKeys.zip(expectedKeys)) {
        actual should be (expected)
      }
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = rasterSource.withCRS(targetCRS)
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)

      val values = rdd.values.collect()

      values.map { value => (value.cols, value.rows) should be ((256, 256)) }
    }
  }

  describe("reading in GeoTiffs as RDDs using GDALRasterSource") {
    val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val rasterSource = GDALRasterSource(uri)

    it("should have the right number of tiles") {
      val warpRasterSource = WarpGDALRasterSource(uri, targetCRS)
      val rdd = RasterSourceRDD(warpRasterSource, layout)

      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(warpRasterSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }.toSet

      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }.toSet

      expectedKeys should be (actualKeys)
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = WarpGDALRasterSource(uri, targetCRS)
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)

      val values = rdd.values.collect()

      values.map { value => (value.cols, value.rows) should be ((256, 256)) }
    }
  }
  */

  describe("Match reprojection from HadoopGeoTiffRDD") {
    val floatingLayout = FloatingLayoutScheme(256)
    val geoTiffRDD = HadoopGeoTiffRDD.spatialMultiband(uri)
    val md = geoTiffRDD.collectMetadata[SpatialKey](floatingLayout)._2

    val reprojectedExpectedRDD: MultibandTileLayerRDD[SpatialKey] = {
      geoTiffRDD
        .tileToLayout(md)
        .reproject(
          targetCRS,
          layout,
          Reproject.Options(targetCellSize = Some(layout.cellSize))
        )._2.persist()
    }

    def assertRDDLayersEqual(
      expected: MultibandTileLayerRDD[SpatialKey],
      actual: MultibandTileLayerRDD[SpatialKey]
    ): Unit = {
      val joinedRDD = expected.leftOuterJoin(actual)

      joinedRDD.collect().map { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) => assertEqual(expected, actual)
          case None => throw new Exception(s"$key does not exist in the rasterSourceRDD")
        }
      }
    }

    describe("GeoTiffRasterSource") {
      val rasterSource = GeoTiffRasterSource(uri)

      /*
      it("should reproduce tileToLayout") {
        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource, md.layout)

        // Complete the reprojection
        val reprojectedSource =
          rasterSourceRDD.reproject(targetCRS, layout)._2

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
      }
      */

      it("should reproduce tileToLayout followed by reproject") {
        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource.withCRS(targetCRS), layout)

        val keys = {
          val inter = layout.mapTransform.extent.intersection(rasterSource.withCRS(targetCRS).extent).get
          layout.mapTransform.keysForGeometry(inter.toPolygon)
        }

        val results = reprojectedExpectedRDD.filter { case (k, v) =>
          v.bands.map { _.isNoDataTile }.reduce { _ && _ }
        }

        val aKeys = reprojectedSourceRDD.keys.collect().toSet
        val eKeys = reprojectedExpectedRDD.keys.collect().toSet

        println(s"\nThis is the number of keys that intersect with the extent: ${keys.size}")
        println(s"This is the number of elements in the reprojectedSourceRDD: ${reprojectedSourceRDD.count}")
        println(s"This is the number of elements in the reprojectedExpectedRDD: ${reprojectedExpectedRDD.count}")
        println(s"This is the number of noDataTiles in the the reprojectedExpectedRDD: ${results.count}")
        println(s"These are keys with the noDataTiles: ${results.keys.collect().toSet}\n")
        println(s"These are the keys that are not found in reprojectedSourceRDD: ${eKeys.diff(aKeys)}\n")

       val mapTransform = layout.mapTransform

       val intersects: (SpatialKey) => Boolean =
         (key: SpatialKey) => mapTransform(key).intersects(layout.extent)

       assertRDDLayersEqual(
         ContextRDD(reprojectedExpectedRDD.filter { case (k, _) => intersects(k) }, reprojectedExpectedRDD.metadata),
         ContextRDD(reprojectedSourceRDD.filter { case (k, _) => intersects(k) }, reprojectedSourceRDD.metadata)
       )
      }
    }

    /*
    describe("GDALRasterSource") {
      val gdalURI = "/tmp/aspect-tiled.tif"
      val rasterSource = GDALRasterSource(gdalURI)

      it("should reproduce tileToLayout") {
        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource, md.layout)

        // Complete the reprojection
        val reprojectedSource =
          rasterSourceRDD.reproject(targetCRS, layout)._2

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
      }

      it("should reproduce tileToLayout followed by reproject") {
        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource.withCRS(targetCRS), layout)

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSourceRDD)
      }
    }
    */
  }
}
