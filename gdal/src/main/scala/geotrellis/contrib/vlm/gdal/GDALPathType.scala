package geotrellis.contrib.vlm.gdal

import io.lemonlabs.uri._


sealed trait GDALPathType {
  def scheme: String
  def firstScheme: Option[String]
  def secondScheme: String
  def targetsNestedFile: Boolean
  def targetsCompressedFile: Boolean

  def path: String
  def uri: Url

  def pathParts: Vector[String] =
    uri.path.parts

  def targetFile: String =
    pathParts(pathParts.size - 1)

  def targetedFileCompressed: Boolean =
    Schemes.COMPRESSED_FILE_TYPES
      .map { targetFile.endsWith(_) }
      .reduce { _ || _ }
}

case class VSIPath(vsiPath: String) extends GDALPathType {
  val uri: Url = Url.parse(vsiPath)

  private val schemes: Vector[String] =
    if (vsiPath.contains("//"))
      pathParts.take(3).filterNot { _.isEmpty }.reverse
    else
      pathParts.take(1)

  val secondScheme: String = schemes.head
  val firstScheme: Option[String] = schemes.tail.headOption

  val scheme: String =
    firstScheme match {
      case Some(sch) => s"/$sch//$secondScheme/"
      case None => s"/$secondScheme/"
    }

  private val firstSchemeCompressed: Boolean =
    firstScheme match {
      case Some(sch) =>
        Schemes
          .COMPRESSED_FILE_TYPES
          .map { sch.contains(_) }
          .reduce { _ || _ }
      case None => false
    }

  val targetsNestedFile: Boolean =
    firstSchemeCompressed && !targetedFileCompressed

  val targetsCompressedFile: Boolean =
    targetsNestedFile || targetedFileCompressed

  val Array(_, path) = vsiPath.split(scheme)
}


case class URIPath(uri: Url) extends GDALPathType {
  import Schemes._

  def scheme: String =
    uri.schemeOption match {
      case Some(str) => str
      case None => ""
    }

  val (firstScheme, secondScheme): (Option[String], String) =
    if (scheme.contains("+")) {
      val Array(first, second) = scheme.split('+')
      (Some(first), second)
    } else if (targetsCompressedFile) {
      val mappedParts: Array[String] =
        targetFile
          .split('.')
          .map { _.toLowerCase }

      val extension: String = mappedParts(mappedParts.size - 1)

      val extensionScheme =
        Schemes.COMPRESSED_FILE_TYPES
          .filter { fileType => fileType == extension }
          .headOption
          .flatMap { FILE_TYPE_TO_SCHEME.get }

      (extensionScheme, scheme)
    } else
      (None, scheme)

  private val firstSchemeCompressed: Boolean =
    firstScheme match {
      case Some(sch) => "gdal" != firstScheme
      case None => false
    }

  def targetsNestedFile: Boolean =
    firstSchemeCompressed && !targetedFileCompressed

  def targetsCompressedFile: Boolean =
    targetsNestedFile || targetedFileCompressed

  private val rawPath: String =
    uri.path.toStringRaw

  private val authority: String =
    uri match {
      case UrlWithAuthority(auth, _, _, _) => auth.toString
      case AbsoluteUrl(_, auth, _, _, _) => auth.toString
      case ProtocolRelativeUrl( auth, _, _, _) => auth.toString
      case _ => ""
    }

  val path: String =
    secondScheme match {
      case (FTP | HTTP | HTTPS | HDFS) =>
        s"$secondScheme://${authority}${rawPath}"
      case (WASB | WASBS) =>
        uri.user match {
          case Some(info) => s"${info}${rawPath}"
          case None => s"${authority}${rawPath}"
        }
      case _ =>
        s"${authority}${rawPath}"
    }
}
