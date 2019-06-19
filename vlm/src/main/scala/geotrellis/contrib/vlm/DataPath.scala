package geotrellis.contrib.vlm

import java.net.URI


trait DataPath {
  def path: String
  def formattedPath: String

  def servicePrefix: String

  override def toString: String = path

  def toURI: URI = new URI(path)
}
