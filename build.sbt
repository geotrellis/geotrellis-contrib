import xerial.sbt.Sonatype._
import Dependencies._

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

lazy val commonSettings = Seq(
  scalaVersion := Version.scala,
  crossScalaVersions := Version.crossScala,
  // We are overriding the default behavior of sbt-git which, by default,
  // only appends the `-SNAPSHOT` suffix if there are uncommitted
  // changes in the workspace.
  version := {
    // Avoid Cyclic reference involving error
    if (git.gitCurrentTags.value.isEmpty || git.gitUncommittedChanges.value)
      git.gitDescribedVersion.value.get + "-SNAPSHOT"
    else
      git.gitDescribedVersion.value.get
  },
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
  pomIncludeRepository := { _ => false },
  addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary),
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full),
  resolvers ++= Seq(
    Resolver.bintrayRepo("azavea", "geotrellis"),
    "geosolutions" at "http://maven.geo-solutions.it/",
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "osgeo" at "http://download.osgeo.org/webdav/geotools/",
    "geotrellis-staging" at "https://oss.sonatype.org/service/local/repositories/orglocationtechgeotrellis-1009/content"
  )
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val publishSettings = Seq(
  organization := "com.azavea.geotrellis",
  organizationName := "GeoTrellis",
  organizationHomepage := Some(new URL("https://geotrellis.io/")),
  description := "GeoTrellis Contrib is a place to put GeoTrellis projects that are not included in GeoTrellis Core for one reason or another.",
  headerLicense := Some(HeaderLicense.ALv2("2019", "Azavea")),
  publishArtifact in Test := false
) ++ sonatypeSettings ++ credentialSettings

lazy val sonatypeSettings = Seq(
  publishMavenStyle := true,

  sonatypeProfileName := "com.azavea",
  sonatypeProjectHosting := Some(GitHubHosting(user="geotrellis", repository="geotrellis-contrib", email="systems@azavea.com")),
  developers := List(
    Developer(id = "echeipesh", name = "Eugene Cheipesh", email = "echeipesh@azavea.com", url = url("https://github.com/echeipesh")),
    Developer(id = "pomadchin", name = "Grigory Pomadchin", email = "gpomadchin@azavea.com", url = url("https://github.com/pomadchin"))
  ),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  publishTo := sonatypePublishTo.value
)

lazy val credentialSettings = Seq(
  credentials += Credentials(
    "GnuPG Key ID",
    "gpg",
    System.getenv().get("GPG_KEY_ID"),
    "ignored"
  ),
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    System.getenv().get("SONATYPE_USERNAME"),
    System.getenv().get("SONATYPE_PASSWORD")
  )
)

lazy val root = Project("geotrellis-contrib", file("."))
  .aggregate(vlm, gdal, slick)
  .settings(commonSettings: _*)
  .settings(noPublishSettings)

lazy val IntegrationTest = config("it") extend Test

lazy val vlm = project
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-vlm",
    libraryDependencies ++= Seq(
      geotrellisSpark,
      geotrellisS3,
      geotrellisUtil,
      scalactic,
      scalaURI,
      squants,
      catsPar,
      sparkCore % Provided,
      sparkSQL % Test,
      geotrellisSparkTestkit % Test,
      geotrellisRasterTestkit % Test,
      scalatest % Test
    ),
    // caused by the AWS SDK v2
    dependencyOverrides ++= {
      val deps = Seq(
        jacksonCore,
        jacksonDatabind,
        jacksonAnnotations
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ jacksonModuleScala
      }
    },
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF")
  )
  .settings(
    initialCommands in console :=
      """
        |import geotrellis.contrib.vlm.effect._
      """.stripMargin
  )


lazy val gdal = project
  .dependsOn(vlm)
  .settings(commonSettings)
  .settings(publishSettings)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-gdal",
    libraryDependencies ++= Seq(
      geotrellisGDAL,
      sparkCore % Provided,
      sparkSQL % Test,
      geotrellisSparkTestkit % Test,
      geotrellisRasterTestkit % Test,
      scalatest % Test,
      gdalBindings % Test
    ),
    // caused by the AWS SDK v2
    dependencyOverrides ++= {
      val deps = Seq(
        jacksonCore,
        jacksonDatabind,
        jacksonAnnotations
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ jacksonModuleScala
      }
    },
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
    javaOptions ++= Seq("-Djava.library.path=/usr/local/lib")
  )
  .settings(
    initialCommands in console :=
      """
        |import geotrellis.contrib.vlm.gdal.effect._
      """.stripMargin
  )

lazy val slick = project
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    organization := "com.azavea.geotrellis",
    name := "geotrellis-contrib-slick",
    libraryDependencies ++= Seq(
      "org.locationtech.geotrellis" %% "geotrellis-vector" % "2.3.1",
      slickPG,
      scalatest % Test
    )
  )
  .settings(crossScalaVersions := Seq(scalaVersion.value))

lazy val benchmark = (project in file("benchmark"))
  .dependsOn(vlm)
  .settings(commonSettings: _*)
  .settings(noPublishSettings)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "benchmark",
    fork := true,
    libraryDependencies ++= Seq(
      sparkCore
    )
  )
