/*
 * Copyright 2018 Azavea
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

package geotrellis.contrib.vlm

import TestCatalog._
import geotrellis.spark.testkit.TestEnvironment
import org.scalatest.Suite
import java.io.File

trait CatalogTestEnvironment extends TestEnvironment { self: Suite =>

  override def beforeAll() = {
    val file = new File(TestCatalog.outputPath)
    if (!(file).exists) {
      TestCatalog.create
    } else {
      println(s"Test catalog exists at: $file")
    }
  }

}
