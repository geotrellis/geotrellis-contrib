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

package geotrellis.contrib.vlm

import cats.instances.string._
import cats.instances.map._
import cats.data.NonEmptyList

case class MosaicMetadata(list: NonEmptyList[SourceMetadata]) extends SourceMetadata {
  def base: Map[String, String] = list.map(_.base).reduce
  def band(b: Int): Map[String, String] = if(b == 0) base else list.map(_.band(b)).reduce
}
