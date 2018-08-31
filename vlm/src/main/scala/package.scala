package geotrellis.contrib

import java.nio.file.Paths
import java.net.URI

import geotrellis.util.{FileRangeReader, RangeReader, StreamingByteReader}

package object vlm extends Implicits {
  private[vlm] def getByteReader(uri: String): StreamingByteReader = {
    val javaURI = new URI(uri)
    val rr =  javaURI.getScheme match {
      case "file" | null =>
        FileRangeReader(Paths.get(javaURI).toFile)

      case "s3" => ???
      //     val s3Uri = new AmazonS3URI(java.net.URLDecoder.decode(uri, "UTF-8"))
      //     val s3Client = new AmazonS3Client(new AWSAmazonS3Client(new DefaultAWSCredentialsProviderChain))
      //     S3RangeReader(s3Uri.getBucket, s3Uri.getKey, s3Client)

      case scheme =>
        throw new IllegalArgumentException(s"Unable to read scheme $scheme at $uri")
    }
    new StreamingByteReader(rr)
  }


}
