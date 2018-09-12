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
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val uri = s"file://$filePath"

  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  val layout = scheme.levelForZoom(13).layout

  /*
  describe("reading in GeoTiffs as RDDs using GeoTiffRasterSource") {
    val rasterSource = GeoTiffRasterSource(uri)

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
    val rasterSource = GDALRasterSource(filePath)
    val reprojectedRasterSource = rasterSource.withCRS(targetCRS)

    it("should have the right number of tiles") {
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)

      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(reprojectedRasterSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }.toSet

      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }.toSet

      expectedKeys should be (actualKeys)
    }

    it("should read in the tiles as squares") {
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

    @transient val mapTrans = layout.mapTransform

    val reprojectedExpectedRDD: MultibandTileLayerRDD[SpatialKey] =
      geoTiffRDD
        .tileToLayout(md)
        .reproject(
          targetCRS,
          layout,
          Reproject.Options(targetCellSize = Some(layout.cellSize))
        )._2.persist()

    val expectedExtent = reprojectedExpectedRDD.metadata.extent

    def assertRDDLayersEqual(
      expected: MultibandTileLayerRDD[SpatialKey],
      actual: MultibandTileLayerRDD[SpatialKey]
    ): Unit = {
      val filteredExpected = expected.filter { case (k, _) =>
        mapTrans.keyToExtent(k).intersects(expectedExtent)
      }

      val filteredActual =
        actual.filter { case (k, _) =>
          mapTrans.keyToExtent(k).intersects(expectedExtent)
        }

      //println(s"\nCount of expected: ${filteredExpected.count}")
      //println(s"\nCount of actual  : ${filteredActual.count}\n")

      val joinedRDD = filteredExpected.leftOuterJoin(filteredActual)

      joinedRDD.collect().map { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) =>
            println(s"\nThis is the key that is being compared: $key")
            assertEqual(expected, actual)
          case None => throw new Exception(s"$key does not exist in the rasterSourceRDD")
        }
      }
    }

    /*
    describe("GeoTiffRasterSource") {
      val rasterSource = GeoTiffRasterSource(uri)

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

    describe("GDALRasterSource") {
      val rasterSource = GDALRasterSource(filePath)

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

       assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSourceRDD)
      }
    }
  }
}
