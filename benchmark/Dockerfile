FROM ubuntu:trusty

RUN set -ex \
     && apt-get update \
     && apt-get install -y software-properties-common \
     && add-apt-repository -y ppa:openjdk-r/ppa \
     && apt-get update \
     && deps=" \
          unzip \
          openjdk-8-jdk \
          wget \
     " \
     && apt-get install -y $deps --no-install-recommends \
     && update-ca-certificates -f \
     && rm -rf /var/lib/apt/lists/*

ADD archives/imageio-ext-1.1.24-jars.zip /tmp
ADD archives/gdal-data.zip /tmp
ADD archives/libgdal-java_1.10.1+dfsg-5ubuntu1_amd64.deb /tmp/

COPY . /opt/geotrellis-contrib

RUN set -ex \
     && unzip /tmp/imageio-ext-1.1.24-jars.zip -d /opt/geotrellis-contrib/lib \
     && unzip /tmp/gdal-data.zip -d /opt/geotrellis-contrib/benchmark/gdal

WORKDIR /opt/geotrellis-contrib

ADD archives/gdal192-Ubuntu12-gcc4.6.3-x86_64.tar.gz /opt/geotrellis-contrib/benchmark/gdal/native/
ADD benchmark/src/main/resources/LC08_L1GT_001003_20170921_20170921_01_RT_B1.TIF /opt/geotrellis-contrib/benchmark/src/main/resources/

RUN set -ex \
     && dpkg -x /tmp/libgdal-java_1.10.1+dfsg-5ubuntu1_amd64.deb /tmp/moop \
     && cp -f /tmp/moop/usr/lib/jni/*.so /opt/geotrellis-contrib/benchmark/gdal/native/ \
     && cp -f /tmp/moop/usr/share/java/gdal.jar /opt/geotrellis-contrib/lib/
RUN ./sbt "project benchmark" compile

ENV GDAL_DATA=/opt/geotrellis-contrib/benchmark/gdal/gdal-data
ENV LD_LIBRARY_PATH=/opt/geotrellis-contrib/benchmark/gdal/native:${LD_LIBRARY_PATH}
