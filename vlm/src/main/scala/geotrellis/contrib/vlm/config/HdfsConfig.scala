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

package geotrellis.contrib.vlm.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import pureconfig.generic.auto._
import pureconfig._

case class HdfsConfig(resources: List[Path]) {
  def load() = {
    val conf = new Configuration()
    resources.foreach(conf.addResource)
    conf
  }
}

object HdfsConfig {
  implicit val hadoopPathReader = ConfigReader.fromString[Path] {
    ConvertHelpers.catchReadError(pathString => new Path(pathString))
  }

  lazy val conf: HdfsConfig = pureconfig.loadConfigOrThrow[HdfsConfig]("vlm.geotiff.hdfs")
  implicit def hdfsConfig(obj: HdfsConfig.type): HdfsConfig = conf
}
