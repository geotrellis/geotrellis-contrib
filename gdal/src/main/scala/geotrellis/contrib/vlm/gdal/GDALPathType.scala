package geotrellis.contrib.vlm.gdal

import io.lemonlabs.uri.{Path, UrlWithAuthority}


sealed trait GDALPathType {
  def path: String
  def targetFile: String
  def targetsCompressedFile: Boolean
  def targetsNestedFile: Boolean
  def scheme: String

  def firstScheme: Option[String]
  def secondScheme: String
}

case class RelativePath(localPath: Path) extends GDALPathType {
  import Schemes._

  def path: String = localPath.toString

  private val pathParts: Vector[String] = localPath.parts
  def targetFile: String = pathParts(pathParts.size - 1)

  def targetsCompressedFile: Boolean =
    Schemes.COMPRESSED_FILE_TYPES
      .map { targetFile.endsWith(_) }
      .reduce { _ || _ }

  def targetsNestedFile: Boolean = false

  def scheme: String = "file"

  val (firstScheme, secondScheme): (Option[String], String) =
    if (targetsCompressedFile) {
      val Array(_, extension) =
        targetFile
          .split('.')
          .map { _.toLowerCase }

      val extensionScheme =
        COMPRESSED_FILE_TYPES
          .filter { fileType => fileType == extension }
          .headOption
          .flatMap { FILE_TYPE_TO_SCHEME.get }

      (extensionScheme, scheme)
    } else
      (None, scheme)
}

case class URIPath(uri: UrlWithAuthority) extends GDALPathType {
  import Schemes._

  private val pathParts: Vector[String] =
    uri.path.parts

  def targetFile: String =
    pathParts(pathParts.size - 1)

  private val targetedFileCompressed: Boolean =
    COMPRESSED_FILE_TYPES
      .map { targetFile.endsWith(_) }
      .reduce { _ || _ }

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
      val Array(_, extension) =
        targetFile
          .split('.')
          .map { _.toLowerCase }

      val extensionScheme =
        COMPRESSED_FILE_TYPES
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

  def path: String =
    secondScheme match {
      case (FTP | HTTP | HTTPS | HDFS) =>
        s"$secondScheme://${uri.authority}${uri.path}"
      case (WASB | WASBS) =>
        uri.user match {
          case Some(info) => s"${info}${uri.path}"
          case None => s"${uri.authority}${uri.path}"
        }
      case _ =>
        s"${uri.authority}${uri.path}"
    }
}
