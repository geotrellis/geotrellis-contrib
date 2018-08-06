import Dependencies._

name := "vlm"

libraryDependencies ++= Seq(
  geotrellisSpark,
  geotrellisS3,
  geotrellisRaster,
  geotrellisVector,
  geotrellisUtil,
  sparkCore,
  hadoopClient,
  catsCore,
  catsEffect,
  fs2Core,
  fs2Io
)
