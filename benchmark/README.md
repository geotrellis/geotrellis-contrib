# With Docker #

## Build ##

In the root of the repository, type `make -f Makefile.benchmark`.

## Run ##

With the Docker image built, type `docker run -it --rm benchmark`.
Inside of the docker container, type
```bash
cd /geotrellis-contrib
./sbt "project benchmark" "jmh:run"
```

# Without Docker #

## Get Dependencies ##

### Dependency Jars ###

Pre-built ImageIO jars can be found [here](https://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.24/).
The jars are in [this](https://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.24/imageio-ext-1.1.24-jars.zip) archive.

### Native Dependencies ###

Pre-built native dependencies can be found [here](https://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.24/native/gdal/).
For Ubuntu 12.04 and compatible systems, [this tarball](https://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.24/native/gdal/linux/gdal192-Ubuntu12-gcc4.6.3-x86_64.tar.gz) is known to work.
GDAL also needs certain data to operate, those files can be found [here](https://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.24/native/gdal/gdal-data.zip).

### GDAL Java Bindings ###

Java bindings are also needed.
For Ubuntu 12.04 and compatible systems, [this package](https://packages.ubuntu.com/trusty/amd64/libgdal-java/download) contains the needed files.

## Get Data ##

The test data can be found [here](https://landsatonaws.com/L8/001/003/LC08_L1GT_001003_20170921_20170921_01_RT).

## Run ##

```bash
GDAL_DATA=$(pwd)/benchmark/gdal/gdal-data LD_LIBRARY_PATH=$(pwd)/benchmark/gdal/native:$LD_LIBRARY_PATH ./sbt "project benchmark" "jmh:run"
```
