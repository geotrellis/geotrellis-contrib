package geotrellis.contrib.vlm.gdal


/*
 * This object conatins the different schemes and filetypes one can pass
 * into GDAL.
 */
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

  final val TGZ = "tgz"

  final val KMZ = "kmz"

  final val ODS = "ods"

  final val XLSX = "xlsx"

  final val COMPRESSED_FILE_TYPES =
    Array(
      s".$TAR",
      s".$TGZ",
      s".$ZIP",
      s".$KMZ",
      s".$ODS",
      s".$XLSX",
      s".$GZIP"
    )

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
