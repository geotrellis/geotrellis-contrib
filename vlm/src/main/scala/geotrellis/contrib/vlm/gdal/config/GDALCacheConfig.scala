package geotrellis.contrib.vlm.gdal.config

import geotrellis.contrib.vlm.gdal.GDAL
import geotrellis.util.LazyLogging
import geotrellis.contrib.vlm.cache.LazyCache

import com.github.blemale.scaffeine.Scaffeine
import org.gdal.gdal.Dataset

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
