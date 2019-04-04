package geotrellis.contrib.vlm.config

case class S3Config(allowGlobalRead: Boolean, region: Option[String] = None)

object S3Config {
  lazy val conf: S3Config = pureconfig.loadConfigOrThrow[S3Config]("vlm.geotiff.s3")
  implicit def s3Config(obj: S3Config.type): S3Config = conf
}
