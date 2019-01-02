# GeoTrellis Contributions repo

[![Build Status](https://travis-ci.org/geotrellis/geotrellis-contrib.svg?branch=master)](https://travis-ci.org/geotrellis/geotrellis-contrib)

This is a repository is a place to put extra [GeoTrellis](https://github.com/locationtech/geotrellis) projects, that were not included into the main repo for various reasons.

 - Project names should be of the form `geotrellis-contrib-{feature}`
 - Project versions should start with `0.0.1` and not be tied to GeoTrellis version.
 - Project publishing will happen on BinTray


## GDAL Support

The `vlm` subproject requires GDAL JNI bindings. 

### Linux 

See the steps listed in `https://github.com/geotrellis/geotrellis-gdal/blob/master/Dockerfile`.

### MacOS

Here's one approach, using Homebrew:

1. `brew install osgeo/osgeo4mac/gdal2 --with-swig-java`
2. The `gdal2` recipe is "keg only", meaning it doesn't add sybolic links in the various `/usr/local/*` directories. 
   you can either add `/usr/local/opt/gdal2/lib` to your `DYLD_LIBRARY_PATH`, or to `java.library.path`, or manually
   create symbolic links: 

   ```console
   (cd /usr/local/lib; for f in /usr/local/opt/gdal2/lib/*.dylib; do ln -s $f; done)
   
   ```


