# GeoTrellis Contributions repo

[![Build Status](https://travis-ci.org/geotrellis/geotrellis-contrib.svg?branch=master)](https://travis-ci.org/geotrellis/geotrellis-contrib)

This is a repository is a place to put extra [GeoTrellis](https://github.com/locationtech/geotrellis) projects, that were not included into the main repo for various reasons.

 - Project names should be of the form `geotrellis-contrib-{feature}`
 - Project versions should start with `0.0.1` and not be tied to GeoTrellis version.
 - Project publishing will happen on BinTray


## Usage

via `sbt`:

```scala
resolvers += "Azavea Public Builds" at "https://dl.bintray.com/azavea/geotrellis"
...
libraryDependencies ++= Seq(
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "X.Y.Z",
  "com.azavea.geotrellis" %% "geotrellis-contrib-gdal" % "X.Y.Z"
)
```

Go to [releases](https://github.com/geotrellis/geotrellis-contrib/releases) to see available versions.

