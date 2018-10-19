/*
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

object Version {
  val geotrellis  = "2.1.0"
  val scala       = "2.11.12"
  val circe       = "0.9.3"
  val hadoop      = "2.8.0"
  val spark       = "2.3.0"
  val gdal        = "2.3.0"
}

object Dependencies {
  val geotrellisSpark         = "org.locationtech.geotrellis" %% "geotrellis-spark"          % Version.geotrellis
  val geotrellisSparkTestKit  = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit"  % Version.geotrellis
  val geotrellisS3            = "org.locationtech.geotrellis" %% "geotrellis-s3"             % Version.geotrellis
  val geotrellisRaster        = "org.locationtech.geotrellis" %% "geotrellis-raster"         % Version.geotrellis
  val geotrellisMacros        = "org.locationtech.geotrellis" %% "geotrellis-macros"         % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisVector        = "org.locationtech.geotrellis" %% "geotrellis-vector"         % Version.geotrellis
  val geotrellisUtil          = "org.locationtech.geotrellis" %% "geotrellis-util"           % Version.geotrellis
  val geotrellisShapefile     = "org.locationtech.geotrellis" %% "geotrellis-shapefile"      % Version.geotrellis

  val pureconfig          = "com.github.pureconfig"      %% "pureconfig"               % "0.9.1"
  val logging             = "com.typesafe.scala-logging" %% "scala-logging"            % "3.9.0"
  val scalatest           = "org.scalatest"              %% "scalatest"                % "3.0.5"
  val scalactic           = "org.scalactic"              %% "scalactic"                % "3.0.5"

  val sparkCore           = "org.apache.spark"           %% "spark-core"               % Version.spark
  val sparkSQL            = "org.apache.spark"           %% "spark-sql"                % Version.spark
  val hadoopClient        = "org.apache.hadoop"           % "hadoop-client"            % Version.hadoop

  val circeCore           = "io.circe"                   %% "circe-core"               % Version.circe
  val circeGeneric        = "io.circe"                   %% "circe-generic"            % Version.circe
  val circeGenericExtras  = "io.circe"                   %% "circe-generic-extras"     % Version.circe
  val circeParser         = "io.circe"                   %% "circe-parser"             % Version.circe

  val gdal                = "org.gdal"                    % "gdal"                     % Version.gdal
}
