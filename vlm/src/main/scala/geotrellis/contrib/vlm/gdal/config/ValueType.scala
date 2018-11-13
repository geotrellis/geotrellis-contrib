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
