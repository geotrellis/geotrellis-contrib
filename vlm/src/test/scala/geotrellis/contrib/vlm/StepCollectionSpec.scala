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

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff.reader._

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.raster.testkit.RasterMatchers

import org.scalatest._

class StepCollectionSpec extends FunSpec with RasterMatchers with BetterRasterMatchers {
  val reprojectStep = ReprojectStep(LatLng, WebMercator, Reproject.Options.DEFAULT)
  val convertStep = ConvertStep(IntCellType, FloatCellType)

  // TODO: Added more tests

  it("should update an empty stepCollectin") {
    val expected = StepCollection(List(reprojectStep))

    val collection = StepCollection()
    val actual = collection.update(reprojectStep)

    actual should be (expected)
  }

  it("should update a stepCollectin") {
    val expected = StepCollection(List(reprojectStep, convertStep))

    val collection = StepCollection(List(reprojectStep))
    val actual = collection.update(convertStep)

    actual should be (expected)
  }

  it("should update a stepCollectin with the newest step") {
    val newStep = ReprojectStep(LatLng, CRS.fromEpsgCode(2031), Reproject.Options.DEFAULT)
    val expected = StepCollection(List(convertStep, newStep))

    val collection = StepCollection(List(convertStep, newStep))
    val actual = collection.update(reprojectStep).update(newStep)

    actual should be (expected)
  }
}
