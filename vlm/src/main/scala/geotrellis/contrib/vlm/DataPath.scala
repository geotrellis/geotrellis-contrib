package geotrellis.contrib.vlm

import java.net.URI


/**
 * Represents the path to data that is to be read.
 */
trait DataPath {
  /**
   * The given path to the data. This can be formatted in a number of different
   * ways depending on which [[RasterSource]] is to be used. For more information
   * on the different ways of formatting this string, see the docs on the
   * DataPath for that given soure.
   */
  def path: String

  override def toString: String = path

  def toURI: URI = new URI(path)
}
