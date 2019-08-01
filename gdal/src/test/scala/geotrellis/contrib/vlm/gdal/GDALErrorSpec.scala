package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.Resource

import com.azavea.gdal.GDALWarp

import org.scalatest._


class GDALErrorSpec extends FunSpec {
  val uri = Resource.path("img/c41078a1.tif")
  val token = GDALWarp.get_token(uri, Array())
  val dataset: GDALDataset.DatasetType = GDALWarpOptions.EMPTY.datasetType

  val sourceWindow: Array[Int] = Array(1000000, 1000000, 5000000, 5000000)
  val destWindow: Array[Int] = Array(500, 500)
  val buffer = Array.ofDim[Byte](500 * 500)

  describe("GDALErrors") {
    it("should return the ObjectNull error code") {
      val result = GDALWarp.get_data(token, dataset.value, 1, sourceWindow, destWindow, 42, 1, buffer)

      assert(math.abs(result) == 10)
    }

    it("should return the IllegalArg error code") {
      val result = GDALWarp.get_data(token, dataset.value, 1, sourceWindow, destWindow, 1, 1, buffer)

      assert(math.abs(result) == 5)
    }
  }
}
