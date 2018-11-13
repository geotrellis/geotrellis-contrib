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

package geotrellis.contrib.vlm.gdal.config

import cats.syntax.either._
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigCursor, ConfigReader}

sealed trait ValueType {
  lazy val name = s"${this.getClass.getName.split("\\$").last.split("\\.").last}"
  override def toString = name
}

case object Hard extends ValueType
case object Soft extends ValueType
case object Weak extends ValueType

object ValueType extends PureConfigSettings {
  def fromName(name: String): ValueType = {
    name match {
      case Soft.name => Soft
      case Hard.name => Hard
      case _         => Weak
    }
  }

  implicit object ValueTypeReader extends ConfigReader[ValueType] {
    def from(cur: ConfigCursor): Either[ConfigReaderFailures, ValueType] = cur.asString.map(ValueType.fromName)
  }
}
