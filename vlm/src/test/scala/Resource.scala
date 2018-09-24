package geotrellis.contrib.vlm

import java.io._
import java.net.URI

object Resource {
  def apply(name: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(s"/$name")
    try { scala.io.Source.fromInputStream( stream ).getLines.mkString(" ") } finally { stream.close() }
  }

  def uri(name: String): URI = {
    getClass.getResource(s"/$name").toURI
  }

  def path(name: String): String = {
    getClass.getResource(s"/$name").getFile
  }
}