package geotrellis.contrib.vlm

import java.net.URI


trait DataPath {
  def path: String

  def servicePrefixes: List[String]

  override def toString: String = path

  def toURI: URI = new URI(path)
}
