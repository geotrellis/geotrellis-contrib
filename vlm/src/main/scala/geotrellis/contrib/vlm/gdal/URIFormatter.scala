package geotrellis.contrib.vlm.gdal


import java.net.URI


object Schemes {
  final val FTP = "ftp"
  final val HTTP = "http"
  final val HTTPS = "https"

  final val TAR = "tar"
  final val ZIP = "zip"
  final val GZIP = "gzip"

  final val FILE = "file"

  final val S3 = "s3"

  final val GS = "gs"

  final val WASB = "wasb"
  final val WASBS = "wasbs"

  final val HDFS = "hdfs"

  final val FILE_TYPE_TO_SCHEME =
    Map(
      TAR -> TAR,
      "tgz" -> TAR,
      ZIP -> ZIP,
      "kmz" -> ZIP,
      "ods" -> ZIP,
      "xlsx" -> ZIP,
      GZIP -> GZIP
    )
}


object URIFormatter {
  import Schemes._

  def apply(path: String): String =
    apply(new URI(path))

  def apply(uri: URI): String = {
    // TODO: Support reading a single file from a zip file
    // ie. reading 1.tiff from a zip: s3://bucket/path/to/my/zipped/data/data.zip/1.tiff

    val path = uri.toString

    // We must first determine if we need to chain file handlers
    // ie. Reading from a zip file on s3
    val leadingScheme: String =
      if (path.contains("."))
        FILE_TYPE_TO_SCHEME.get(path.substring(path.lastIndexOf(".") + 1)) match {
          case Some(scheme) => s"/vsi$scheme/"
          case None => ""
        }
      else
        ""

    val formattedPath: String =
      uri.getScheme match {
        case (FTP | HTTP | HTTPS) =>
          s"/vsicurl/$path"
        case S3 =>
          s"/vsis3/${uri.getAuthority}${uri.getPath}"
        case GS =>
          s"/vsigs/${uri.getAuthority}${uri.getPath}"
        case (WASB | WASBS) =>
          val azurePath = {
            val p = uri.getPath

            if (p.startsWith("/")) p.drop(1) else p
          }

          s"/vsiaz/${uri.getUserInfo}/$azurePath"
        case HDFS =>
          s"/vsihdfs/${uri.toString}"
        case FILE =>
          path.split("file://").tail.head
        case _ =>
          // If no scheme is found, assume the uri is
          // a relative path to a file
          path
      }

    leadingScheme + formattedPath
  }
}
