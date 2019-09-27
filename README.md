# GeoTrellis Contributions repo

[![CircleCI](https://circleci.com/gh/geotrellis/geotrellis-contrib/tree/develop.svg?style=svg)](https://circleci.com/gh/geotrellis/geotrellis-contrib/tree/develop)

This is a repository is a place to put extra [GeoTrellis](https://github.com/locationtech/geotrellis) projects, that were not included into the main repo for various reasons.

- Project names should be of the form `geotrellis-contrib-{feature}`
- Project versions should start with `0.0.1` and not be tied to GeoTrellis version.
- Project publishing will happen on SonaType

## Usage

via `sbt`:

```scala
libraryDependencies ++= Seq(
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "X.Y.Z",
  "com.azavea.geotrellis" %% "geotrellis-contrib-gdal" % "X.Y.Z"
)
```

Go to [releases](https://github.com/geotrellis/geotrellis-contrib/releases) to see available versions.
