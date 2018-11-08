package geotrellis.contrib.vlm.gdal

import scala.util.matching.Regex

import java.io.File


object Schemes2 {
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

object Patterns2 {
  final val schemePattern: Regex = """.*?(?=\:)""".r
  final val firstSchemePattern: Regex = """[^\+]*""".r
  final val secondSchemePattern: Regex = """(?<=\+).*?(?=\:)""".r

  final val userInfoPattern: Regex = """(?<=\/\/).*?(?=@)""".r

  final val authorityPattern: Regex = """(?<=\/\/).*?(?=\/)""".r

  final val defaultPathPattern: Regex = """(?<=(?:(\/){2})).+""".r
  final val windowsLocalPathPattern: Regex = """(?<=(?:(\/))).+""".r
}

case class VSIPath2(path: String) {
  import Schemes2._
  import Patterns2._

  private val isWindows: Boolean =
    System.getProperty("os.name").toLowerCase == "win"

  lazy val scheme: Option[String] =
    schemePattern.findFirstIn(path)

  private lazy val schemeString: String =
    scheme match {
      case Some(targetScheme) => targetScheme.toLowerCase
      case None => ""
    }

  private lazy val isLocal: Boolean =
    schemeString.contains(FILE) || schemeString == ""

  private lazy val onLocalWindows: Boolean =
    isWindows && isLocal

  lazy val targetPath: Option[String] =
    scheme match {
      case Some(_) =>
        if (onLocalWindows)
          windowsLocalPathPattern.findFirstIn(path)
        else
          defaultPathPattern.findFirstIn(path)
      case None =>
        Some(path)
    }

  private lazy val targetPathString: String =
    targetPath match {
      case Some(pathString) => pathString
      case None => throw new Exception(s"Could not determine the paths for: $path")
    }

  private lazy val formattedPathString: String =
    if (!targetsCompressedFile)
      targetPathString
    else {
      val formattedFileName =
        if (onLocalWindows)
          targetFileName.replace("!", """\\""")
        else
          targetFileName.replace("!", "/")

      val pathWithoutFileName =
        targetPathString.substring(0, targetPathString.size - formattedFileName.size)

      pathWithoutFileName + formattedFileName
    }

  lazy val targetFileName: String = {
    val filePath =
      if (onLocalWindows)
        // File isn't able to find the name of the
        // target file if '\' are in the path, so we
        // need to replace them with '/'.
        targetPathString.replaceAll("""\\""", "/")
      else
        targetPathString

    new File(filePath).getName
  }

  private lazy val targetsCompressedData: Boolean =
    COMPRESSED_FILE_TYPES
      .map { extension => targetFileName.contains(extension) }
      .reduce { _ || _ }

  private lazy val targetsCompressedFile: Boolean =
    targetFileName.contains("!")

  lazy val isChained: Boolean =
    schemeString.contains("+") || (targetsCompressedData && !isLocal)

  lazy val authority: Option[String] =
    authorityPattern.findFirstIn(path)

  lazy val userInfo: Option[String] =
    userInfoPattern.findFirstIn(path)

  lazy val (firstScheme, secondScheme): (Option[String], Option[String]) =

    // Schemes that contain "+" point to a file that's in a compressed container
    if (schemeString.contains("+")) {
      val (firstScheme, unformattedSecondScheme): (String, String) =
        schemeString.splitAt(schemeString.lastIndexOf("+"))

      // We need to take the tail because "+" is still apart of the
      // scheme.
      val secondScheme = unformattedSecondScheme.substring(1)

      (Some(firstScheme), Some(secondScheme))

    // The scheme does not contain "+", but it still points some kind
    // of compressed data. In the case where we're reading data from
    // a local file system, we can drop the scheme for that backend
    // as it's not neeeded for file.
    } else if (targetsCompressedData) {

      // The data just points to a compressed file and not data within
      // it.
      if (!targetsCompressedFile) {
        val extensionIndex = targetFileName.lastIndexOf(".") + 1
        val extension = targetFileName.substring(extensionIndex)

        (
          FILE_TYPE_TO_SCHEME.get(extension),
          if (isLocal) None else Some(schemeString)
        )

      // The path points to data within a compressed file that needs
      // to be read.
      } else {
        val compressionName = targetFileName.split("!").head

        val extensionIndex = compressionName.lastIndexOf(".") + 1
        val extension = compressionName.substring(extensionIndex)

        (
          FILE_TYPE_TO_SCHEME.get(extension),
          if (isLocal) None else Some(schemeString)
        )
      }

    // We're not reading any compressed data
    } else
      (None, if (isLocal) None else Some(schemeString))

  private lazy val firstVSIScheme: String =
    firstScheme.flatMap { FILE_TYPE_TO_SCHEME.get } match {
      case Some(firstHalf) => s"/vsi$firstHalf/"
      case None => ""
    }

  private lazy val secondSchemeString: String =
    secondScheme match {
      case Some(scheme) => scheme
      case None => ""
    }
}
