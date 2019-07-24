/*
 * Copyright 2019 Azavea
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

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey, SpatialKey}
import geotrellis.raster.ResampleMethod
import geotrellis.raster.resample.NearestNeighbor

trait TileToLayoutRasterSource[K] extends Serializable {
  def tileToLayout(
    rs: RasterSource,
    layout: LayoutDefinition,
    keyTransformation: (RasterSource, SpatialKey) => K,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): LayoutTileSource[K]
}

object TileToLayoutRasterSource {
  def apply[K: TileToLayoutRasterSource] = implicitly[TileToLayoutRasterSource[K]]

  implicit val spatialTileToLayoutRasterSource = new TileToLayoutRasterSource[SpatialKey] {
    def tileToLayout(
      rs: RasterSource,
      layout: LayoutDefinition,
      keyTransformation: (RasterSource, SpatialKey) => SpatialKey = (_, sk) => sk,
      resampleMethod: ResampleMethod = NearestNeighbor
     ): LayoutTileSource[SpatialKey] =
      LayoutTileSource(rs.resampleToGrid(layout, resampleMethod), layout, keyTransformation)
  }

  implicit val spaceTimeTileToLayoutRasterSource = new TileToLayoutRasterSource[SpaceTimeKey] {
    def tileToLayout(
      rs: RasterSource,
      layout: LayoutDefinition,
      keyTransformation: (RasterSource, SpatialKey) => SpaceTimeKey,
      resampleMethod: ResampleMethod = NearestNeighbor
    ): LayoutTileSource[SpaceTimeKey] =
      LayoutTileSource.temporal(rs.resampleToGrid(layout, resampleMethod), layout, keyTransformation)
  }
}
