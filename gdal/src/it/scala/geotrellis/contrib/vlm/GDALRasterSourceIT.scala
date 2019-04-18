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

package geotrellis.contrib.vlm
import geotrellis.contrib.testkit.TestRasterData
import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.testkit.RasterMatchers
import org.scalatest.{FunSpec, Inspectors}

class GDALRasterSourceIT extends FunSpec with RasterMatchers with BetterRasterMatchers with Inspectors with TestRasterData {
  describe("GDAL vs JVM results") {
    it("should report the same extent") {
      val cases = Seq(pdsCogSampleHttp, pdsL8SampleHttp, pdsModisSampleHttp)

      forEvery(cases) { path =>
        assertSimilarExtents(GDALRasterSource(path).extent, GeoTiffRasterSource(path).extent)
      }
    }
  }
}
