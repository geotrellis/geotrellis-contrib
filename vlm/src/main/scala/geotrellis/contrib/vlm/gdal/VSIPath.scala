package geotrellis.contrib.vlm.gdal

import scala.util.matching.Regex

import java.io.File


case class VSIPath(
  path: String,
  compressedFileDelimiter: String = "!"
) {
  import Schemes._
  import Patterns._

  private lazy val isLocal: Boolean =
    schemeString.contains(FILE) || schemeString == ""

  // Trying to read something locally on Windows matters
  // because of how file paths on Windows are formatted.
  // Therefore, we need to handle them differently.
  private lazy val onLocalWindows: Boolean =
    System.getProperty("os.name").toLowerCase == "win" && isLocal

  lazy val scheme: Option[String] =
    SCHEME_PATTERN.findFirstIn(path)

  private lazy val schemeString: String =
    scheme match {
      case Some(targetScheme) => targetScheme.toLowerCase
      case None => ""
    }

  // The given path can contain 0, 1, or 2 different schemes. Any one of these
  // can be chained together with other handlers. Thus, we must find out what
  // our first and second scheme is in order to properly format the path.
  //
  // The firstScheme is only used when there is a chained operation, and it
  // always points to the handler needed to read the target file
  // (ie. vsizip, vsitar, etc).
  //
  // The secondScheme points the to the handler needed to access the target
  // backend (ie. vsis3, vsihdfs, etc). The only time there isn't a
  // secondScheme is when the user is reading from the local file system.
  lazy val (firstScheme, secondScheme): (Option[String], Option[String]) =

    // Schemes that contain "+" point to a file that's either compressed or
    // within a compressed file itself.
    if (schemeString.contains("+")) {
      val firstScheme = FIRST_SCHEME_PATTERN.findFirstIn(schemeString)
      val secondScheme = SECOND_SCHEME_PATTERN.findFirstIn(path)

      (firstScheme, secondScheme)

    // The scheme does not contain "+", but it still points some kind
    // of compressed data. In the case where we're reading data from
    // a local file system, we can drop the scheme for that backend
    // as it's not neeeded to read the file.
    } else if (targetsCompressedDirectory) {

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

  private lazy val secondVSIScheme: String = {
    val secondSchemeString: String =
      secondScheme match {
        case Some(scheme) => scheme
        case None => ""
      }

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

  lazy val authority: Option[String] =
    AUTHORITY_PATTERN.findFirstIn(path)

  lazy private val authorityString: String =
    authority match {
      case Some(authority) => authority
      case None => ""
    }

  lazy val userInfo: Option[String] =
    USER_INFO_PATTERN.findFirstIn(path)

  lazy private val userInfoString: String =
    userInfo match {
      case Some(info) => info
      case None => ""
    }

  lazy val targetPath: Option[String] =
    scheme match {
      case Some(_) =>
        if (onLocalWindows)
          WINDOWS_LOCAL_PATH_PATTERN.findFirstIn(path)
        else
          DEFAULT_PATH_PATTERN.findFirstIn(path)
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

  private lazy val targetsCompressedDirectory: Boolean =
    COMPRESSED_FILE_TYPES
      .map { extension => targetFileName.contains(extension) }
      .reduce { _ || _ }

  private lazy val targetsCompressedFile: Boolean =
    targetFileName.contains(compressedFileDelimiter)
}
