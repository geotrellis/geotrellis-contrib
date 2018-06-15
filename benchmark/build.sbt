name := "benchmark"

enablePlugins(JmhPlugin)

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.2.1"
)
