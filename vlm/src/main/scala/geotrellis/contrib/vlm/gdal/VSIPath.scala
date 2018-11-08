package geotrellis.contrib.vlm.gdal

import scala.util.matching.Regex

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


/*
trait VSIPath {
  val path: String

  protected val isWindows: Boolean =
    System.getProperty("os.name").toLowerCase == "win"

  lazy val scheme: Option[String] =
    VSIPath.schemePattern.findFirstIn(path)

  protected lazy val pathPattern: Regex =
    scheme match {
      case Some(targetScheme) =>
        if (!isWindows || targetScheme.toLowerCase.contains(Schemes.FILE))
          VSIPath.defaultPathPattern
        else
          VSIPath.windowsLocalPathPattern
      case None =>
    }


case class VSIPath(path: String) {
  import Schemes._

  private final val schemePattern: Regex = """.*?(?=\:)""".r
  private final val firstSchemePattern: Regex = """[^\+]*""".r
  private final val secondSchemePattern: Regex = """(?<=\+).*?(?=\:)""".r

  private final val userInfoPattern: Regex = """(?<=\/\/).*?(?=@)""".r

  private final val authorityPattern: Regex = """(?<=\/\/).*?(?=\/)""".r

  private final val defaultPathPattern: Regex = """(?<=(?:(\/){2})).+""".r
  private final val windowsLocalPathPattern: Regex = """(?<=(?:(\/))).+""".r

  private val isWindows: Boolean =
    System.getProperty("os.name").toLowerCase == "win"

  lazy val scheme: Option[String] =
    schemePattern.findFirstIn(path)

  lazy val targetFileExtension: String =
    path.substring(path.lastIndexOf(".") + 1)

    FILE_TYPE_TO_SCHEME
      .get(targetPath.substring(targetPath.lastIndexOf(".") + 1)) match {
      case Some(scheme) => s"/vsi$scheme/"
      case None => ""
    }

  lazy val isChained: Boolean =
    scheme match {
      case Some(targetScheme) =>
        targetScheme.contains("+") || FILE_TYPE_TO_SCHEME.contains(targetFileExtension)
      case _ => false
    }

  lazy val firstScheme: Option[String] =
    firstSchemePattern.findFirstIn(path)

  lazy val secondScheme: Option[String] =
    secondSchemePattern.findFirstIn(path)

  private lazy val firstVSIScheme: String =
    if (isChained)
*/



object VSIPath {
  import Schemes._

  final val schemePattern: Regex = """.*?(?=\:)""".r
  final val firstSchemePattern: Regex = """[^\+]*""".r
  final val secondSchemePattern: Regex = """(?<=\+).*?(?=\:)""".r

  final val userInfoPattern: Regex = """(?<=\/\/).*?(?=@)""".r

  final val authorityPattern: Regex = """(?<=\/\/).*?(?=\/)""".r

  final val defaultPathPattern: Regex = """(?<=(?:(\/){2})).+""".r
  final val windowsLocalPathPattern: Regex = """(?<=(?:(\/))).+""".r

  def apply(uri: URI): String = ???

  def apply(path: String): String = {
    val pathScheme: Option[String] = schemePattern.findFirstIn(path)

    lazy val defaultPath = defaultPathPattern.findFirstIn(path)
    lazy val windowsPath = windowsLocalPathPattern.findFirstIn(path)

    val isWindows: Boolean =
      System.getProperty("os.name").toLowerCase == "win"

    pathScheme match {
      case Some(scheme) if (scheme.contains("+")) =>
        for {
          firstHalf <- firstSchemePattern.findFirstIn(scheme)
          secondHalf <- secondSchemePattern.findFirstIn(scheme)
          //targetPath <- defaultPath if (!isWindows || secondHalf != FILE) else windowsPath
        } yield {
          ???
          // Get the scheme for the compressed file type
          val fileScheme: String = schemeForFileType(firstHalf)
          ???
        }

        ???
    }

    def schemeForFileType(targetPath: String): String =
      FILE_TYPE_TO_SCHEME
        .get(targetPath.substring(targetPath.lastIndexOf(".") + 1)) match {
        case Some(scheme) => s"/vsi$scheme/"
        case None => ""
      }

    ???

    /*
    def formatPath(targetURI: URI): String = {
      lazy val uriPath = targetURI.getPath

      targetURI.getScheme match {
        case (FTP | HTTP | HTTPS) =>
          s"/vsicurl/${targetURI.toString}"
        case S3 =>
          s"/vsis3/${targetURI.getAuthority}$uriPath"
        case GS =>
          s"/vsigs/${targetURI.getAuthority}$uriPath"
        case (WASB | WASBS) =>
          val azurePath =
            if (uriPath.startsWith("/")) uriPath.drop(1) else uriPath

          s"/vsiaz/${targetURI.getUserInfo}/$azurePath"
        case HDFS =>
          s"/vsihdfs/${targetURI.toString}"
        case FILE =>
          if (isWindows)
            targetURI.toString.split("file:/").tail.head
          else
            targetURI.toString.split("file://").tail.head
        case _ =>
          // If no scheme is found, assume the uri is
          // a relative path to a file
          uriPath
      }
    }

    val (leadingScheme, secondScheme) =
      if (scheme != null && scheme.contains("+")) {
        // If the Scheme contains a "+" then we'll be reading a
        // single file from a compressed source on some backend.

        // The first scheme is the type of compression the source has
        // zip, gzip, tar, etc.
        // The second scheme is the backend where the source is located
        val (firstScheme, unformattedSecondScheme): (String, String) =
          scheme.splitAt(scheme.indexOf("+"))

        // We need to take the tail because "+" is still apart of the
        // scheme
        val secondScheme = unformattedSecondScheme.tail

        // On Windows, the local file system Scheme is followed by
        // ":/" (+2) as opposed to "://" (+3) which is used on other
        // types of machines.
        val pathWithoutScheme: String =
          if (isWindows && scheme == FILE)
            path.substring(scheme.size + 2)
          else
            path.substring(scheme.size + 3)

        // The target file is seperated from its source in the URI with
        // an "!". Therefore, we must find and replace it with the
        // appropriate symbol.
        val exclamationIndex: Int = pathWithoutScheme.lastIndexOf("!")

        val firstHalf: String = pathWithoutScheme.substring(0, exclamationIndex)
        val secondHalf: String = pathWithoutScheme.substring(exclamationIndex + 1)

        // If we're not trying to read a local file on a Windows machine,
        // then we use this path.
        lazy val defaultFormattedPath: String = secondScheme + "://" + firstHalf + "/" + secondHalf

        val formattedPath: String =
          if (isWindows && secondScheme == FILE)
            secondScheme + ":/" + firstHalf + "\\" + secondHalf
          else
            defaultFormattedPath

        // Get the scheme for the compressed file type
        val fileScheme: String = schemeForFileType(firstHalf)

        // The scheme the scheme that points to the file on the given backend
        val followingScheme: String = formatPath(new URI(formattedPath))

        (fileScheme, followingScheme)

      } else if (path.contains("."))
        // This URI could potentially contain chained Schemes, so we should check
        // to make sure.
        (schemeForFileType(path), formatPath(uri))

      else
        // The URI is not pointing to a file, so throw an error.
        throw new Exception(s"The given URI: $uri must point to a file, but none was found")

    leadingScheme + secondScheme
    */
  }
}
