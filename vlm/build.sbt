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
  scalatest % Test
)

Test / fork := true

javaOptions ++= Seq("-Xms1024m", "-Xmx6144m")
