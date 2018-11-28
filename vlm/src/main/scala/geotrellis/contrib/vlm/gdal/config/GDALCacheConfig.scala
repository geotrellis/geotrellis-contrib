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

package geotrellis.contrib.vlm.gdal.config

import geotrellis.contrib.vlm.gdal.GDAL
import geotrellis.contrib.vlm.cache.LazyCache

import com.github.blemale.scaffeine.Scaffeine
import org.gdal.gdal.Dataset
import com.typesafe.scalalogging.LazyLogging

case class GDALCacheConfig(
  maximumSize: Option[Long] = None,
  enableDefaultRemovalListener: Boolean = true,
  valuesType: ValueType = Weak,
  enabled: Boolean = true,
  withShutdownHook: Boolean = true
) extends LazyLogging {
  def getCache: LazyCache[String, Dataset] = {
    def get = () => {
      val cache = Scaffeine()
      maximumSize.foreach(cache.maximumSize)
      valuesType match {
        case Weak => cache.weakValues()
        case Soft => cache.softValues()
        case _ => // do nothing
      }
      if (enableDefaultRemovalListener)
        cache.removalListener[String, Dataset] { case (key, dataset, event) =>
          logger.warn(s"removalListener: $key - ${dataset} event: $event")
          if (dataset != null) dataset.delete
        }

      cache.build[String, Dataset]
    }

    LazyCache(get, enabled)
  }

  def addShutdownHook: Unit =
    if(withShutdownHook) Runtime.getRuntime.addShutdownHook(new Thread() { override def run(): Unit = GDAL.cacheCleanUp })
}

object GDALCacheConfig extends PureConfigSettings{
  lazy val conf: GDALCacheConfig = pureconfig.loadConfigOrThrow[GDALCacheConfig]("gdal.cache")
  implicit def gdalCacheConfig(obj: GDALCacheConfig.type): GDALCacheConfig = conf
}
