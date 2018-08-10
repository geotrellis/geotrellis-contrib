package geotrellis.contrib.vlm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.net.URI


trait GTInfoReader extends InfoReader {
  def tiffExtensions: Seq[String]
  def streaming: Boolean

  def geoTiffInfoRDD(uri: URI)(implicit sc: SparkContext): RDD[String]
}
