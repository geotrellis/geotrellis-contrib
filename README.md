# GeoTrellis Contributions repo

[![Build Status](https://travis-ci.org/geotrellis/geotrellis-contrib.svg?branch=master)](https://travis-ci.org/geotrellis/geotrellis-contrib)

This is a repository is a place to put extra [GeoTrellis](https://github.com/locationtech/geotrellis) projects, that were not included into the main repo for various reasons.

 - Project names should be of the form `geotrellis-contrib-{feature}`
 - Project versions should start with `0.0.1` and not be tied to GeoTrellis version.
 - Project publishing will happen on BinTray


## GDAL Support

The `vlm` subproject requires GDAL JNI bindings. 

### Linux 

See the steps list in `Dockerfile.benchmark`.

### MacOS

Here's one approach, using Homebrew:

1. `brew install osgeo/osgeo4mac/gdal2 --with-java --with-swig-java --with-libkml --with-opencl`
2. Add `/usr/local/opt/gdal2/lib` to JVM system property `java.library.path`, or add the path to the `DYLD_LIBRARY_PATH` environment variable. 



