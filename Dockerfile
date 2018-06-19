FROM ubuntu:trusty

RUN apt-get update
RUN apt-get install -y software-properties-common unzip
RUN add-apt-repository -y ppa:openjdk-r/ppa
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y wget
RUN apt-get clean

ADD archives/geotrellis-contrib.tar /
ADD archives/imageio-ext-1.1.24-jars.zip /tmp
RUN (cd /geotrellis-contrib/lib ; unzip /tmp/imageio-ext-1.1.24-jars.zip)
ADD archives/gdal192-Ubuntu12-gcc4.6.3-x86_64.tar.gz /geotrellis-contrib/benchmark/gdal/native/
ADD archives/gdal-data.zip /tmp
RUN (cd /geotrellis-contrib/benchmark/gdal ; unzip /tmp/gdal-data.zip)
ADD archives/libgdal-java_1.10.1+dfsg-5ubuntu1_amd64.deb /tmp/
RUN (dpkg -x /tmp/libgdal-java_1.10.1+dfsg-5ubuntu1_amd64.deb /tmp/moop ; \
     cp -f /tmp/moop/usr/lib/jni/*.so /geotrellis-contrib/benchmark/gdal/native/ ; \
     cp -f /tmp/moop/usr/share/java/gdal.jar /geotrellis-contrib/lib/)

RUN update-ca-certificates -f
RUN (cd /geotrellis-contrib ; ./sbt "project benchmark" compile)

ADD benchmark/src/main/resources/LC08_L1GT_001003_20170921_20170921_01_RT_B1.TIF /geotrellis-contrib/benchmark/src/main/resources/

ENV GDAL_DATA=/geotrellis-contrib/benchmark/gdal/gdal-data
ENV LD_LIBRARY_PATH=/geotrellis-contrib/benchmark/gdal/native:${LD_LIBRARY_PATH}
