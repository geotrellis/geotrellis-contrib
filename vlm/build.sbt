import Dependencies._

name := "vlm"

libraryDependencies ++= Seq(
  geotrellisSpark,
  geotrellisSparkTestKit % Test,
  geotrellisS3,
  geotrellisRaster,
  geotrellisVector,
  geotrellisUtil,
  sparkCore,
  hadoopClient,
  catsCore,
  catsEffect,
  fs2Core,
  fs2Io,
  scalatest % Test,
  gdal
)

Test / fork := true
fork in run := true

javaOptions ++= Seq("-Xms1024m", "-Xmx6144m", "-Djava.library.path=/usr/local/lib")
