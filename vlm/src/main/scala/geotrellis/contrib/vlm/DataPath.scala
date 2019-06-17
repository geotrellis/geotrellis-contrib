package geotrellis.contrib.vlm

import java.net.{URI, URL}


trait DataPath {
  def path: String
  def targetPath: String

  protected def servicePrefix: String

  override def toString: String = targetPath
  def toURI: URI = new URI(targetPath)
  def toURL: URL = new URL(targetPath)
}
