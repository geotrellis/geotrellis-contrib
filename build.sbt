import Dependencies._

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

addCommandAlias("bintrayPublish", ";publish;bintrayRelease")

lazy val commonSettings = Seq(
  scalaVersion := Version.scala,
  crossScalaVersions := Version.crossScala,
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
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full),
  dependencyUpdatesFilter := moduleFilter(organization = "org.scala-lang"),
  resolvers ++= Seq(
    Resolver.bintrayRepo("azavea", "geotrellis"),
    "geosolutions" at "http://maven.geo-solutions.it/",
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "osgeo" at "http://download.osgeo.org/webdav/geotools/",
    "geotrellis-staging" at "https://oss.sonatype.org/service/local/repositories/orglocationtechgeotrellis-1009/content"
  ),
  headerLicense := Some(HeaderLicense.ALv2("2018", "Azavea")),
  // bintrayOrganization := Some("azavea"),
  // bintrayRepository := "geotrellis",
  // bintrayPackageLabels := Seq("gis", "raster", "vector"),
  // bintrayReleaseOnPublish := false,
  publishTo := {
    val bintrayPublishTo = publishTo.value
    val nexus = "http://nexus.internal.azavea.com"

    if (isSnapshot.value) {
      Some("snapshots" at nexus + "/repository/azavea-snapshots")
    } else {
      bintrayPublishTo
    }
  }
)

lazy val root = Project("geotrellis-contrib", file(".")).
  aggregate(
    vlm
  ).
  settings(commonSettings: _*).
  settings(publish / skip := true).
  settings(
    initialCommands in console :=
      """
      """
  )

lazy val IntegrationTest = config("it") extend Test

lazy val vlm = project
  .dependsOn(testkit % Test)
  .settings(commonSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-vlm",
    libraryDependencies ++= Seq(
      geotrellisSpark,
      geotrellisS3,
      geotrellisUtil,
      scalactic,
      squants,
      sparkCore % Provided,
      sparkSQL % Test,
      geotrellisSparkTestKit % Test,
      scalatest % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
  )
  .settings(
    initialCommands in console :=
      """
        |import geotrellis.contrib.vlm._
        |import geotrellis.contrib.vlm.geotiff._
        |import geotrellis.contrib.vlm.avro._
      """.stripMargin
  )


lazy val gdal = project
  .dependsOn(testkit % Test)
  .dependsOn(vlm)
  .settings(commonSettings)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-gdal",
    libraryDependencies ++= Seq(
      gdalWarp,
      sparkCore % Test,
      sparkSQL % Test,
      geotrellisSparkTestKit % Test,
      scalatest % Test,
      gdalBindings % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
    javaOptions ++= Seq("-Djava.library.path=/usr/local/lib")
  )
  .settings(
    initialCommands in console :=
      """
        |import geotrellis.contrib.vlm._
        |import geotrellis.contrib.vlm.geotiff._
        |import geotrellis.contrib.vlm.gdal._
      """.stripMargin
  )

lazy val testkit = project
  .settings(commonSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-testkit",
    libraryDependencies ++= Seq(
      geotrellisSpark, // SpatialKey
      geotrellisRasterTestkit,
      geotrellisRaster,
      geotrellisMacros,
      scalatest % Provided
    )
  )

lazy val summary = project
  .settings(commonSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-summary",
    libraryDependencies ++= Seq(
      geotrellisRaster,
      geotrellisVector,
      simulacrum,
      scalatest % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oD")
  )

lazy val benchmark = (project in file("benchmark"))
  .settings(commonSettings: _*)
  .settings( publish / skip := true)
