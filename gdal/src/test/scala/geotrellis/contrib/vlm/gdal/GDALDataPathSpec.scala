/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.gdal

import org.scalatest._


class GDALDataPathSpec extends FunSpec with Matchers {
  val fileName = "file-1.tiff"

  describe("Formatting the given uris") {
    describe("http") {
      it("http url") {
        val filePath = "www.radomdata.com/test-files/file-1.tiff"
        val url = s"http://$filePath"
        val expectedPath = s"/vsicurl/$url"

        GDALDataPath(url).vsiPath should be (expectedPath)
      }

      it("http that points to gzip url") {
        val filePath = "www.radomdata.com/test-files/data.gzip"
        val url = s"http://$filePath"
        val expectedPath = s"/vsigzip//vsicurl/$url"

        GDALDataPath(url).vsiPath should be (expectedPath)
      }

      it("http that points to gzip with ! url") {
        val filePath = "www.radomdata.com/test-files/data.gzip"
        val url = s"gzip+http://$filePath!$fileName"
        val expectedPath = s"/vsigzip//vsicurl/http://$filePath/$fileName"

        GDALDataPath(url).vsiPath should be (expectedPath)
      }

      it("http that points to gz url") {
        val filePath = "www.radomdata.com/test-files/data.gz"
        val url = s"http://$filePath"
        val expectedPath = s"/vsigzip//vsicurl/$url"

        GDALDataPath(url).vsiPath should be (expectedPath)
      }

      it("zip+http url") {
        val filePath = "www.radomdata.com/test-files/data.zip"
        val url = s"zip+http://$filePath"
        val expectedPath = s"/vsizip//vsicurl/http://$filePath"

        GDALDataPath(url).vsiPath should be (expectedPath)
      }

      it("zip+http with ! url") {
        val filePath = "www.radomdata.com/test-files/data.zip"
        val url = s"zip+http://$filePath!$fileName"
        val expectedPath = s"/vsizip//vsicurl/http://$filePath/$fileName"

        GDALDataPath(url).vsiPath should be (expectedPath)
      }
    }

    describe("file") {
      it("file uri") {
        val filePath = "/home/jake/Documents/test-files/file-1.tiff"
        val uri = s"file://$filePath"
        val expectedPath = filePath

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("file that points to zip uri") {
        val filePath = "/home/jake/Documents/test-files/files.zip"
        val uri = s"file://$filePath"
        val expectedPath = s"/vsizip/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("zip+file uri") {
        val path = "/tmp/some/data/data.zip"
        val uri = s"zip+file://$path"
        val expectedPath = "/vsizip//tmp/some/data/data.zip"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("zip+file with ! uri") {
        val path = "/tmp/some/data/data.zip"
        val uri = s"zip+file://$path!$fileName"
        val expectedPath = s"/vsizip/$path/$fileName"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }
    }

    describe("s3") {
      it("s3 uri") {
        val filePath = "test-files/nlcd/data/tiff-0.tiff"
        val uri = s"s3://$filePath"
        val expectedPath = s"/vsis3/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("s3 that points to gzip uri") {
        val filePath = "test-files/nlcd/data/data.gzip"
        val uri = s"s3://$filePath"
        val expectedPath = s"/vsigzip//vsis3/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("gzip+s3 uri") {
        val path = "some/bucket/data/data.gzip"
        val uri = s"gzip+s3://$path"
        val expectedPath = s"/vsigzip//vsis3/$path"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("gzip+s3 uri with !") {
        val path = "some/bucket/data/data.gzip"
        val uri = s"gzip+s3://$path!$fileName"
        val expectedPath = s"/vsigzip//vsis3/$path/$fileName"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("s3 that points to gz uri") {
        val filePath = "test-files/nlcd/data/data.gz"
        val uri = s"s3://$filePath"
        val expectedPath = s"/vsigzip//vsis3/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("gzip+s3 uri for a .gz ext") {
        val path = "some/bucket/data/data.gz"
        val uri = s"gzip+s3://$path"
        val expectedPath = s"/vsigzip//vsis3/$path"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("gzip+s3 uri with ! for a .gz ext") {
        val path = "some/bucket/data/data.gz"
        val uri = s"gzip+s3://$path!$fileName"
        val expectedPath = s"/vsigzip//vsis3/$path/$fileName"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }
    }

    describe("hdfs") {
      it("hdfs uri") {
        val filePath = "test-files/nlcd/data/tiff-0.tiff"
        val uri = s"hdfs://$filePath"
        val expectedPath = s"/vsihdfs/$uri"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("hdfs that points to tgz uri") {
        val filePath = "test-files/nlcd/data/my_data.tgz"
        val uri = s"hdfs://$filePath"
        val expectedPath = s"/vsitar//vsihdfs/$uri"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("zip+hdfs uri") {
        val filePath = "hdfs://test-files/nlcd/data/data.zip"
        val uri = s"zip+$filePath"
        val expectedPath = s"/vsizip//vsihdfs/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("zip+hdfs with ! uri") {
        val filePath = "hdfs://test-files/nlcd/data/data.zip"
        val uri = s"zip+$filePath!$fileName"
        val expectedPath = s"/vsizip//vsihdfs/$filePath/$fileName"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }
    }

    describe("Google Cloud Storage") {
      it("Google Cloud Storage uri") {
        val filePath = "test-files/nlcd/data/tiff-0.tiff"
        val uri = s"gs://$filePath"
        val expectedPath = s"/vsigs/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("Google Cloud Storage that points to tar uri") {
        val filePath = "test-files/nlcd/data/data.tar"
        val uri = s"gs://$filePath"
        val expectedPath = s"/vsitar//vsigs/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("tar+gs uri") {
        val filePath = "test-files/nlcd/data/data.tar"
        val uri = s"tar+gs://$filePath"
        val expectedPath = s"/vsitar//vsigs/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("tar+gs with ! uri") {
        val filePath = "test-files/nlcd/data/data.tar"
        val uri = s"tar+gs://$filePath!$fileName"
        val expectedPath = s"/vsitar//vsigs/$filePath/$fileName"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }
    }

    describe("Azure") {
      it("Azure uri") {
        val uri = "wasb://test-files@myaccount.blah.core.net/nlcd/data/tiff-0.tiff"
        val expectedPath = "/vsiaz/test-files/nlcd/data/tiff-0.tiff"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("Azure that points to kmz uri") {
        val uri = "wasb://test-files@myaccount.blah.core.net/nlcd/data/info.kmz"
        val expectedPath = "/vsizip//vsiaz/test-files/nlcd/data/info.kmz"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("wasb+zip uri") {
        val uri = "zip+wasb://test-files@myaccount.blah.core.net/nlcd/data/info.zip"
        val expectedPath = "/vsizip//vsiaz/test-files/nlcd/data/info.zip"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("wasb+zip with ! uri") {
        val path = "zip+wasb://test-files@myaccount.blah.core.net/nlcd/data/info.zip"
        val uri = s"$path!$fileName"
        val expectedPath = s"/vsizip//vsiaz/test-files/nlcd/data/info.zip/$fileName"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }
    }

    describe("relative path") {
      it("relative path uri") {
        val filePath = "../../test-files/file-1.tiff"
        val uri = filePath
        val expectedPath = filePath

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("relative path that points to zip uri") {
        val filePath = "../../test-files/data.zip"
        val uri = filePath
        val expectedPath = s"/vsizip/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }
    }

    describe("Formatting the given uris - edge cases") {
      ignore("should parse a path with uncommon characters") {
        val filePath = """data/jake__user--data!@#$%^&*()`~{}[]\|=+,?';<>;/files/my-data.tif"""
        val uri = s"s3://$filePath"
        val expectedPath = s"/vsis3/$filePath"

        GDALDataPath(uri).vsiPath should be (expectedPath)
      }

      it("should parse a targeted compressed file with a differenct delimiter") {
        val filePath = "data/my-data/data!.zip"
        val uri = s"zip+s3://$filePath/$fileName"
        val expectedPath = s"/vsizip//vsis3/$filePath/$fileName"

        GDALDataPath(uri, "/").vsiPath should be (expectedPath)
      }
    }
  }
}
