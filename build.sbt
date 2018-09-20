import Dependencies._

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

lazy val commonSettings = Seq(
  scalaVersion := Version.scala,
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url(Info.url)),
  scmInfo := Some(ScmInfo(
    url("https://github.com/geotrellis/geotrellis-contrib"), "scm:git:git@github.com:geotrellis/geotrellis-contrib.git"
  )),
  scalacOptions ++= Seq(
    "-deprecation", "-unchecked", "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-language:experimental.macros",
    "-Ypartial-unification" // Required by Cats
  ),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary),
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full),
  dependencyUpdatesFilter := moduleFilter(organization = "org.scala-lang"),
  resolvers ++= Seq(
    "geosolutions" at "http://maven.geo-solutions.it/",
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "osgeo" at "http://download.osgeo.org/webdav/geotools/"
  ),
  headerLicense := Some(HeaderLicense.ALv2("2018", "Azavea")),
  bintrayOrganization := Some("azavea"),
  bintrayRepository := "geotrellis",
  bintrayPackageLabels := Seq("gis", "raster", "vector")
)

lazy val root = Project("geotrellis-contrib", file(".")).
  aggregate(
    vlm
  ).
  settings(commonSettings: _*).
  settings(skip in publish := true).
  settings(
    initialCommands in console :=
      """
      """
  )

lazy val vlm = project
  .settings(commonSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-vlm",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      geotrellisSpark, geotrellisS3, geotrellisUtil,
      catsCore, catsEffect,
      fs2Core, fs2Io,
      sparkCore % Provided,      
      geotrellisSparkTestKit % Test,
      scalatest % Test),
    Test / fork := true,
    javaOptions ++= Seq("-Xms1024m", "-Xmx6144m")
  )

lazy val benchmark = (project in file("benchmark"))
  .settings(commonSettings: _*)
