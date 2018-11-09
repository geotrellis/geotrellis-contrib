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

case class VSIPath2(
  path: String,
  compressedFileDelimiter: String = "!"
) {
  import Schemes2._
  import Patterns2._

  private val isWindows: Boolean =
    System.getProperty("os.name").toLowerCase == "win"

  private lazy val isLocal: Boolean =
    schemeString.contains(FILE) || schemeString == ""

  private lazy val onLocalWindows: Boolean =
    isWindows && isLocal

  // Determing the scheme

  lazy val scheme: Option[String] =
    schemePattern.findFirstIn(path)

  private lazy val schemeString: String =
    scheme match {
      case Some(targetScheme) => targetScheme.toLowerCase
      case None => ""
    }

  // Determining the path

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
          targetFileName.replace(compressedFileDelimiter, """\\""")
        else
          targetFileName.replace(compressedFileDelimiter, "/")

      val pathWithoutFileName =
        targetPathString.substring(0, targetPathString.size - formattedFileName.size)

      //println(s"This was the formattedFileName: $formattedFileName")
      //println(s"This was the pathWithoutFileName: $pathWithoutFileName")

      pathWithoutFileName + formattedFileName
    }

  // Determinging Authority

  lazy val authority: Option[String] =
    authorityPattern.findFirstIn(path)

  lazy private val authorityString: String =
    authority match {
      case Some(authority) => authority
      case None => ""
    }

  // Determining UserInfo

  lazy val userInfo: Option[String] =
    userInfoPattern.findFirstIn(path)

  lazy private val userInfoString: String =
    userInfo match {
      case Some(info) => info
      case None => ""
    }

  // Determining the target file to read from

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

  private lazy val targetsCompressedDirectory: Boolean =
    COMPRESSED_FILE_TYPES
      .map { extension => targetFileName.contains(extension) }
      .reduce { _ || _ }

  private lazy val targetsCompressedFile: Boolean =
    targetFileName.contains(compressedFileDelimiter)

  lazy val isChained: Boolean =
    schemeString.contains("+") || (targetsCompressedDirectory && !isLocal)

  lazy val (firstScheme, secondScheme): (Option[String], Option[String]) =
    // Schemes that contain "+" point to a file that's either compressed or
    // within a compressed file itself.
    if (schemeString.contains("+")) {
      val (firstScheme, unformattedSecondScheme): (String, String) =
        schemeString.splitAt(schemeString.lastIndexOf("+"))

      // "+" is the first character of the unformattedSecondScheme string,
      // so we take everything after it.
      val secondScheme = unformattedSecondScheme.substring(1)

      (Some(firstScheme), Some(secondScheme))

    // The scheme does not contain "+", but it still points some kind
    // of compressed data. In the case where we're reading data from
    // a local file system, we can drop the scheme for that backend
    // as it's not neeeded for file.
    } else if (targetsCompressedDirectory) {
      //println(s"It does target a compressed directory")

      // The data just points to a compressed file and not data within
      // it.
      if (!targetsCompressedFile) {
        //println(s"It DOES NOT target a compressed file")
        val extensionIndex = targetFileName.lastIndexOf(".") + 1
        val extension = targetFileName.substring(extensionIndex)

        (
          FILE_TYPE_TO_SCHEME.get(extension),
          if (isLocal) None else Some(schemeString)
        )

      // The path points to data within a compressed file that needs
      // to be read.
      } else {
        //println(s"It DOES target a compressed file")
        val compressionName = targetFileName.split(compressedFileDelimiter).head

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

  private lazy val secondVSIScheme: String = {
    secondSchemeString match {
      case (FTP | HTTP | HTTPS) =>
        if (targetsCompressedFile)
          s"/vsicurl/$secondSchemeString://$formattedPathString"
        else
          s"/vsicurl/$path"
      case S3 =>
        s"/vsis3/$formattedPathString"
      case GS =>
        s"/vsigs/$formattedPathString"
      case (WASB | WASBS) =>
        val azurePath = {
          val formattedPathSize = formattedPathString.size
          val authoritySize = authorityString.size
          val substring = formattedPathString.substring(authorityString.size)

          if (substring.startsWith("/")) substring.drop(1) else substring
        }

        s"/vsiaz/$userInfoString/$azurePath"
      case HDFS =>
        if (targetsCompressedFile)
          s"/vsihdfs/$formattedPathString"
        else
          s"/vsihdfs/$path"
      case FILE =>
        formattedPathString
      case _ =>
        // If no scheme is found, assume the uri is
        // a relative path to a file
        formattedPathString
    }
  }

  lazy val vsiPath:String = firstVSIScheme + secondVSIScheme
}
