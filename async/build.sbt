import Dependencies._

name := "geotrellis-contrib-async"

libraryDependencies ++= Seq(
  geotrellisSpark,
  pureconfig,
  scalatest % Test
)

initialCommands in console :="""
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.vector._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
"""
