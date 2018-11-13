package geotrellis.contrib.vlm.gdal.config

import pureconfig.{CamelCase, ConfigFieldMapping, ProductHint}

trait PureConfigSettings {
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
}
