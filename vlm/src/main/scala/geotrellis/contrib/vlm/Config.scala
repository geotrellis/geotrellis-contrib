package geotrellis.contrib.vlm

import pureconfig._

object Config {
  case class S3Options(allowGlobalRead: Boolean)
  val s3 = pureconfig.loadConfigOrThrow[S3Options]("vlm.geotiff.s3")
}
