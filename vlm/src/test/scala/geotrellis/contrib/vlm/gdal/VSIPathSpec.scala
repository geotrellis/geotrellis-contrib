package geotrellis.contrib.vlm.gdal

import java.net.URI

import org.scalatest._


class VSIPathSpec extends FunSpec with Matchers {
  describe("Formatting the given uris") {
    it("should format - http url") {
      val filePath = "www.radomdata.com/test-files/file-1.tiff"
      val url = s"http://$filePath"
      val expectedPath = s"/vsicurl/$url"
      val vsi = VSIPath2(url)

      println(s"This is the first Scheme: ${vsi.firstScheme}")
      println(s"This is the second Scheme: ${vsi.secondScheme}")

      //VSIPath2(url) should be (expectedPath)
    }
    /*
    it("should format - http url") {
      val filePath = "www.radomdata.com/test-files/file-1.tiff"
      val url = s"http://$filePath"
      val expectedPath = s"/vsicurl/$url"

      VSIPath(url) should be (expectedPath)
    }

    it("should format - ftp url") {
      val filePath = "/tmp/test-files/file-1.tiff"
      val url = s"ftp://$filePath"
      val expectedPath = s"/vsicurl/$url"

      VSIPath(url) should be (expectedPath)
    }

    it("should format - https url") {
      val filePath = "www.radomdata.com/test-files/file-1.tiff"
      val url = s"https://$filePath"
      val expectedPath = s"/vsicurl/$url"

      VSIPath(url) should be (expectedPath)
    }

    it("should format - chained https url") {
      val filePath = "www.radomdata.com/test-files/files.gzip"
      val url = s"https://$filePath"
      val expectedPath = s"/vsigzip//vsicurl/$url"

      VSIPath(url) should be (expectedPath)
    }

    it("should format - file uri") {
      val filePath = "/home/jake/Documents/test-files/file-1.tiff"
      val uri = s"file://$filePath"
      val expectedPath = filePath

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - chained file uri") {
      val filePath = "/home/jake/Documents/test-files/files.zip"
      val uri = s"file://$filePath"
      val expectedPath = s"/vsizip/$filePath"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - s3 uri") {
      val filePath = "test-files/nlcd/data/tiff-0.tiff"
      val uri = s"s3://$filePath"
      val expectedPath = s"/vsis3/$filePath"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - chained s3 uri") {
      val filePath = "test-files/nlcd/data/data.gzip"
      val uri = s"s3://$filePath"
      val expectedPath = s"/vsigzip//vsis3/$filePath"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - hdfs uri") {
      val filePath = "test-files/nlcd/data/tiff-0.tiff"
      val uri = s"hdfs://$filePath"
      val expectedPath = s"/vsihdfs/$uri"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - chained hdfs uri") {
      val filePath = "test-files/nlcd/data/my_data.tgz"
      val uri = s"hdfs://$filePath"
      val expectedPath = s"/vsitar//vsihdfs/$uri"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - Google Cloud Storage uri") {
      val filePath = "test-files/nlcd/data/tiff-0.tiff"
      val uri = s"gs://$filePath"
      val expectedPath = s"/vsigs/$filePath"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - chained Google Cloud Storage uri") {
      val filePath = "test-files/nlcd/data/data.tar"
      val uri = s"gs://$filePath"
      val expectedPath = s"/vsitar//vsigs/$filePath"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - Azure uri") {
      val uri = "wasb://test-files@myaccount.blah.core.net/nlcd/data/tiff-0.tiff"
      val expectedPath = "/vsiaz/test-files/nlcd/data/tiff-0.tiff"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - chained Azure uri") {
      val uri = "wasb://test-files@myaccount.blah.core.net/nlcd/data/info.kmz"
      val expectedPath = "/vsizip//vsiaz/test-files/nlcd/data/info.kmz"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - zip+file uri") {
      val path = "/tmp/some/data/data.zip!file_1.tif"
      val uri = s"zip+file://$path"
      val expectedPath = "/vsizip//tmp/some/data/data.zip/file_1.tif"

      VSIPath(uri) should be (expectedPath)
    }

    it("should format - gzip+s3 uri") {
      val path = "some/bucket/data/data.gzip!file_1.tif"
      val uri = s"zip+s3://$path"
      val expectedPath = "/vsigzip//vsis3/some/bucket/data/data.gzip/file_1.tif"

      VSIPath(uri) should be (expectedPath)
    }
  */
  }
}
